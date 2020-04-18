package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.db.Hit;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A connection to a client. A client will only have one connection open.
 * <p>
 * When a client changes dimension or server, it will drop the connection and make a new one.
 */
public abstract class Connection {
    private static final long MIN_READ_INTERVAL_MS = 5_000;

    public Connection(World world) {
        this.world = world;
    }

    private final World world;
    private final Map<Integer, Task> tasks = new HashMap<>();
    private final Map<BlockPos, BlockCheckManager.BlockCheck> checks = new HashMap<>();
    private int taskIDSeq = 0;
    private final Set<OnlinePlayer> onlinePlayerSet = new HashSet<>();
    private final Map<OnlinePlayer, Long> removalTimestamps = new HashMap<>();
    private long mostRecentRead = System.currentTimeMillis();

    public void readLoop() {
        ScheduledFuture<?> future = TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(() -> {
            long time = System.currentTimeMillis() - mostRecentRead;
            if (time > MIN_READ_INTERVAL_MS) {
                System.out.println("NO DATA!");
                closeUnderlying();
            }
        }), 0, 1, TimeUnit.SECONDS);
        while (true) {
            try {
                read();
                mostRecentRead = System.currentTimeMillis();
            } catch (Throwable th) {
                th.printStackTrace();
                closeUnderlying();
                world.connectionClosed(this); // redistribute tasks to the other connections
                future.cancel(false);
                break;
            }
        }
    }

    public void requestServerDisconnect() {
        NoComment.executor.execute(this::dispatchDisconnectRequest);
    }

    public synchronized void acceptBlockCheck(BlockCheckManager.BlockCheck check) {
        BlockCheckManager.BlockCheck curr = checks.get(check.pos);
        if (curr != null && check.priority >= curr.priority) {
            // if this check is higher (worse) priority than what we currently have, don't spam them with another copy
            return;
        }
        checks.put(check.pos, check);
        NoComment.executor.execute(() -> dispatchBlockCheck(check));
    }

    public synchronized void acceptTask(Task task) {
        // even at 500 per second, it would take over 3 months of 24/7 2b2t uptime for this to overflow, so don't worry
        int id = taskIDSeq++;
        tasks.put(id, task);
        NoComment.executor.execute(() -> dispatchTask(task, id));
    }

    protected void hitReceived(int taskID, ChunkPos pos) {
        Hit hit = new Hit(world, pos);
        hit.saveToDBAsync();
        Task task;
        synchronized (this) {
            task = tasks.get(taskID);
        }
        // this cannot be on another thread / executor because then a hitReceived could possibly be reordered after taskCompleted
        task.hitReceived(hit);
    }

    protected void taskCompleted(int taskID) {
        Task task;
        synchronized (this) {
            task = tasks.remove(taskID);
        }
        // this could theoretically be on another thread / executor, because it's guaranteed to be the last thing in this task, so no worries if it gets delayed for any amount of time
        task.completed();
        world.worldUpdate();
    }

    protected synchronized void playerJoinLeave(boolean join, OnlinePlayer player) {
        if (join) {
            onlinePlayerSet.remove(player);
            onlinePlayerSet.add(player);
        } else {
            removalTimestamps.put(player, System.currentTimeMillis());
            onlinePlayerSet.remove(player);
        }
        world.serverUpdate(); // prevent two-way deadlock with the subsequent two functions
    }

    protected void checkCompleted(BlockPos pos, OptionalInt blockState) {
        BlockCheckManager.BlockCheck check;
        synchronized (this) {
            check = checks.remove(pos);
        }
        if (check != null) {
            // check can be null if we are asked for the same pos twice in a row with increasing priority, and we get two responses with a time delay
            // in that case, the second response would have check is null
            NoComment.executor.execute(() -> check.onCompleted(blockState));
        }
        world.worldUpdate();
    }

    public synchronized void addOnlinePlayers(Collection<OnlinePlayer> collection) {
        collection.removeAll(onlinePlayerSet);
        collection.addAll(onlinePlayerSet);
    }

    public synchronized void addQuelledFromRemoval(Map<OnlinePlayer, Long> map) {
        // this just overwrites
        // this is Probably Fine
        map.putAll(removalTimestamps);
        removalTimestamps.clear();
    }

    /**
     * Sum the sizes of pending tasks with higher or equal priority to this one
     *
     * @param priority the priority of a tentative task
     * @return total counts of all tasks
     */
    public synchronized int sumHigherPriority(int priority) {
        int sum = 0;
        for (Task task : tasks.values()) {
            if (task.priority <= priority) {
                sum += task.count;
            }
        }
        for (BlockCheckManager.BlockCheck check : checks.values()) {
            if (check.priority <= priority) {
                sum++;
            }
        }
        return sum;
    }

    public synchronized void forEachDispatch(Consumer<PriorityDispatchable> consumer) {
        tasks.values().forEach(consumer);
        checks.values().forEach(consumer);
    }

    public int getIdentity() {
        return Database.idForPlayer(new OnlinePlayer(getUUID()));
    }

    public abstract String getUUID();

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchTask(Task task, int taskID);

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchBlockCheck(BlockCheckManager.BlockCheck check);

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchDisconnectRequest();

    /**
     * Read (looped). Throw exception on any failure.
     */
    protected abstract void read() throws IOException;

    /**
     * Never throw exception
     */
    protected abstract void closeUnderlying();

    @Override
    public String toString() {
        return getUUID() + " " + tasks.size() + " tasks, " + sumHigherPriority(Integer.MAX_VALUE) + " checks, " + taskIDSeq + " task ID, " + onlinePlayerSet.size() + " online players reported, " + (System.currentTimeMillis() - mostRecentRead) + "ms since most recent read";
    }
}
