package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;
import nocomment.master.util.OnlinePlayer;

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
        return sum;
    }

    public synchronized void forEachTask(Consumer<Task> consumer) {
        tasks.values().forEach(consumer);
    }

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchTask(Task task, int taskID);

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
        return tasks.size() + " tasks, " + sumHigherPriority(Integer.MAX_VALUE) + " checks, " + taskIDSeq + " task ID, " + onlinePlayerSet.size() + " online players reported, " + (System.currentTimeMillis() - mostRecentRead) + "ms since most recent read";
    }
}
