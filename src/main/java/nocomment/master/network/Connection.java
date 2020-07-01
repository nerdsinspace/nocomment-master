package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.db.Hit;
import nocomment.master.slurp.BlockCheck;
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

    private static final long MIN_READ_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);

    public Connection(World world) {
        this.world = world;
    }

    private final World world;
    private final Map<Integer, Task> tasks = new HashMap<>();
    private final Map<BlockPos, BlockCheck> checks = new HashMap<>();
    private final Map<BlockPos, Long> recentCheckTimestamps = new HashMap<>();
    private int taskIDSeq = 0;
    private final Set<OnlinePlayer> onlinePlayerSet = new HashSet<>();
    private final Map<OnlinePlayer, Long> removalTimestamps = new HashMap<>();
    private final Set<BlockPos> pendingSignChecks = new HashSet<>();
    private long mostRecentRead = System.currentTimeMillis();
    private Integer identityCache;

    public void readLoop() {
        int playerID = getIdentity();
        short serverID = world.server.serverID;
        Database.updateStatus(playerID, serverID, "ONLINE", Optional.empty());
        Database.setDimension(playerID, serverID, world.dimension);
        ScheduledFuture<?> future = TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(() -> {
            long time = System.currentTimeMillis() - mostRecentRead;
            if (time > MIN_READ_INTERVAL_MS) {
                System.out.println("NO DATA!");
                closeUnderlying();
            }
            clearRecentChecks();
            Database.updateStatus(playerID, serverID, "ONLINE", Optional.empty());
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

    public abstract void writeLoop();

    private synchronized void clearRecentChecks() {
        long now = System.currentTimeMillis();
        recentCheckTimestamps.values().removeIf(ts -> ts < now - 1000);
    }

    public synchronized boolean blockAffinity(BlockPos pos) {
        return checks.containsKey(pos) || recentCheckTimestamps.containsKey(pos);
    }

    public synchronized void acceptBlockCheck(BlockCheck check) {
        BlockCheck curr = checks.get(check.pos);
        if (curr != null && check.priority >= curr.priority) {
            // if this check is higher (worse) priority than what we currently have, don't spam them with another copy
            return;
        }
        checks.put(check.pos, check);
        dispatchBlockCheck(check);
    }

    public synchronized void acceptSignCheck(BlockPos pos) {
        if (pendingSignChecks.add(pos)) {
            dispatchSignCheck(pos);
        }
    }

    public synchronized void acceptTask(Task task) {
        // even at 500 per second, it would take over 3 months of 24/7 2b2t uptime for this to overflow, so don't worry
        int id = taskIDSeq++;
        tasks.put(id, task);
        dispatchTask(task, id);
        world.stats.taskDispatched(task.priority, task.count);
    }

    protected void hitReceived(int taskID, ChunkPos pos) {
        Hit hit = new Hit(world, pos);
        hit.saveToDBSoon();
        Task task;
        synchronized (this) {
            task = tasks.get(taskID);
        }
        // this cannot be on another thread / executor because then a hitReceived could possibly be reordered after taskCompleted
        task.hitReceived(hit);
        world.notifyHit(pos, task.priority);
    }

    protected void taskCompleted(int taskID) {
        Task task;
        synchronized (this) {
            task = tasks.remove(taskID);
        }
        // this could theoretically be on another thread / executor, because it's guaranteed to be the last thing in this task, so no worries if it gets delayed for any amount of time
        task.completed();
        world.worldUpdate();
        world.stats.taskCompleted(task);
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
        BlockCheck check;
        synchronized (this) {
            check = checks.remove(pos);
            recentCheckTimestamps.put(pos, System.currentTimeMillis());
        }
        if (check != null) {
            // check can be null if we are asked for the same pos twice in a row with increasing priority, and we get two responses with a time delay
            // in that case, the second response would have check is null
            NoComment.executor.execute(() -> check.onCompleted(blockState));
        }
        world.worldUpdate();
        if (check != null) {
            if (blockState.isPresent()) {
                world.stats.blockReceived(check.priority);
            } else {
                world.stats.blockUnloaded(check.priority);
            }
        }
    }

    protected synchronized void signCompleted(BlockPos pos, Optional<byte[]> nbt) {
        pendingSignChecks.remove(pos);
        NoComment.executor.execute(() -> world.signManager.response(pos, nbt));
        if (nbt.isPresent()) {
            world.stats.signHit();
        } else {
            world.stats.signMiss();
        }
    }

    protected void chatMessage(String msg, byte chatType) {
        long now = System.currentTimeMillis();
        NoComment.executor.execute(() -> Database.saveChat(msg, chatType, getIdentity(), world.server.serverID, now));
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
     * <p>
     * Exclude the single highest priority task. This lets us send a task even if there's
     * one big one already there. Essentially, if we're dealing with Big Tasks, such as the spiral
     * scanner, we should keep one task "on standby" while the other is running. So, if there is
     * just one BIG task with higher priority, we send anyway, give it a friend.
     *
     * @param priority the priority of a tentative task
     * @return total counts of all tasks
     */
    public synchronized int sumHigherPriority(int priority) {
        int sum = 0;
        int highest = 0;
        for (Task task : tasks.values()) {
            if (task.priority <= priority) {
                sum += task.count;
                if (task.count > highest) {
                    highest = task.count;
                }
            }
        }
        sum -= highest;
        for (BlockCheck check : checks.values()) {
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

    public synchronized void forEachPendingSign(Consumer<BlockPos> consumer) {
        pendingSignChecks.forEach(consumer);
    }

    public int getIdentity() {
        if (identityCache == null) {
            identityCache = Database.idForPlayer(new OnlinePlayer(getUUID()));
        }
        return identityCache;
    }

    public abstract String getUUID();

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchTask(Task task, int taskID);

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchBlockCheck(BlockCheck check);

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    public abstract void dispatchDisconnectRequest();

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchSignCheck(BlockPos pos);

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    public abstract void dispatchChatMessage(String msg);

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
        String ret = getUUID() + " " + tasks.size() + " tasks, " + sumHigherPriority(Integer.MAX_VALUE) + " checks, " + taskIDSeq + " task ID, " + onlinePlayerSet.size() + " online players reported, " + (System.currentTimeMillis() - mostRecentRead) + "ms since most recent read";
        OptionalLong join = Staggerer.currentSessionJoinedAt(getIdentity(), world.server.serverID);
        if (join.isPresent()) {
            ret += ", " + (System.currentTimeMillis() - join.getAsLong()) / (double) TimeUnit.HOURS.toMillis(1) + "hours since server join";
        }
        return ret;
    }
}
