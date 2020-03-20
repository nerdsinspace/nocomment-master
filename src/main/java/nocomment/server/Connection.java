package nocomment.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.function.Consumer;

/**
 * A connection to a client. A client will only have one connection open.
 * <p>
 * When a client changes dimension or server, it will drop the connection and make a new one.
 */
public abstract class Connection {
    protected final World world;

    public Connection(World world) {
        this.world = world;
    }

    private final HashMap<Integer, Task> tasks = new HashMap<>();
    private int taskIDSeq;

    public void readLoop() {
        while (true) {
            try {
                read();
            } catch (IOException ex) {
                closeUnderlying();
                world.connectionClosed(this); // redistribute tasks to the other connections
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
        Task task;
        synchronized (this) {
            task = tasks.get(taskID);
        }
        task.hitReceived(pos);
    }

    protected void taskCompleted(int taskID) {
        Task task;
        synchronized (this) {
            task = tasks.remove(taskID);
        }
        task.completed();
        world.update();
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
