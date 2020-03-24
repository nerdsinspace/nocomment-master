package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
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
    private static final long REMOVAL_QUELL_DURATION_MS = 30_000;

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
        ScheduledFuture<?> future = TrackyTrackyManager.scheduler.scheduleAtFixedRate(() -> {
            long time = System.currentTimeMillis() - mostRecentRead;
            if (time > MIN_READ_INTERVAL_MS) {
                closeUnderlying();
            }
        }, 0, 1, TimeUnit.SECONDS);
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
        task.hitReceived(hit);
    }

    protected void taskCompleted(int taskID) {
        Task task;
        synchronized (this) {
            task = tasks.remove(taskID);
        }
        // TODO is it overkill to move task.completed here, and task.hitReceived in the prev function, into a NoComment.executor.execute()?
        task.completed();
        world.worldUpdate();
    }

    protected synchronized void playerJoinLeave(boolean join, OnlinePlayer player) {
        if (join) {
            onlinePlayerSet.add(player);
        } else {
            removalTimestamps.put(player, System.currentTimeMillis());
            onlinePlayerSet.remove(player);
        }
        world.serverUpdate(); // prevent two-way deadlock with the subsequent two functions
    }

    public synchronized Collection<OnlinePlayer> onlinePlayers() {
        return new ArrayList<>(onlinePlayerSet);
    }

    public synchronized Collection<OnlinePlayer> quelledFromRemoval() {
        for (OnlinePlayer player : new ArrayList<>(removalTimestamps.keySet())) { // god i hate concurrentmodificationexception
            if (removalTimestamps.get(player) < System.currentTimeMillis() - REMOVAL_QUELL_DURATION_MS) {
                removalTimestamps.remove(player);
            }
        }
        return new ArrayList<>(removalTimestamps.keySet());
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
}
