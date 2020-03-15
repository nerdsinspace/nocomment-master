package nocomment.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A connection to a client. A client will only have one connection open.
 * <p>
 * When a client changes dimension or server, it will drop the connection and make a new one.
 */
public abstract class Connection {
    protected final World world;

    public Connection(World dimension) {
        this.world = dimension;
    }

    final Map<Long, Task> tasks = new HashMap<>();
    private long taskIDSeq = System.nanoTime();

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

    public void acceptTask(Task task) {
        dispatchTask(task, claimID(task));
    }

    private synchronized long claimID(Task task) {
        long id = taskIDSeq++;
        tasks.put(id, task);
        return id;
    }

    protected synchronized void hitReceived(long taskID, ChunkPos pos) {
        tasks.get(taskID).hitReceived(pos);
    }

    protected synchronized void taskCompleted(long taskID) {
        tasks.remove(taskID).completed();
    }

    /**
     * Never throw exception. Just close the socket and let read fail gracefully.
     */
    protected abstract void dispatchTask(Task task, long taskID);

    /**
     * Read (looped). Throw exception on any failure.
     */
    protected abstract void read() throws IOException;

    /**
     * Never throw exception
     */
    protected abstract void closeUnderlying();
}
