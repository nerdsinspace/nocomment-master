package nocomment.master;

import nocomment.master.network.Connection;
import nocomment.master.task.CombinedTask;
import nocomment.master.task.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

public class World {
    private static final int MAX_BURDEN = 1000; // about 2.3 seconds
    public final Server server;
    private final List<Connection> connections;
    private final PriorityQueue<Task> pendingTasks;
    public final short dimension;

    public World(Server server, short dimension) {
        this.server = server;
        this.connections = new ArrayList<>();
        this.pendingTasks = new PriorityQueue<>();
        this.dimension = dimension;
    }

    public synchronized void incomingConnection(Connection connection) {
        connections.add(connection);
        NoComment.executor.execute(connection::readLoop);
        worldUpdate();
        // dont preemptively update server until data comes in tbh
    }

    public synchronized void connectionClosed(Connection conn) {
        // due to the read loop structure, by the time we get here we know for a fact that this connection will read no further data, since its read loop is done
        // so, shuffling the tasks elsewhere is safe, and doesn't risk "completed" being called twice or anything like that
        connections.remove(conn);
        conn.forEachTask(pendingTasks::add);
        worldUpdate();
        serverUpdate(); // only for connection status change
    }

    public synchronized void submitTask(Task task) {
        pendingTasks.add(task);
        worldUpdate();
        // don't server update per-task!
    }

    public synchronized Task submitTaskUnlessAlreadyPending(Task task) {
        for (Task dup : pendingTasks) {
            if (dup.interchangable(task)) {
                System.out.println("Already queued. Not adding duplicate task. Queue size is " + pendingTasks.size());
                pendingTasks.remove(dup);
                pendingTasks.add(new CombinedTask(task, dup));
                return task;
            }
        }
        submitTask(task);
        return task;
    }

    private synchronized void sendTasksOnConnections() {
        if (connections.isEmpty()) {
            return;
        }
        while (!pendingTasks.isEmpty()) {
            Task task = pendingTasks.peek();
            if (task.isCanceled()) {
                pendingTasks.poll();
                continue;
            }
            Connection min = connections.get(0);
            int burden = min.sumHigherPriority(task.priority);
            for (int i = 1; i < connections.size(); i++) {
                Connection conn = connections.get(i);
                int connBurden = conn.sumHigherPriority(task.priority);
                if (connBurden < burden) {
                    burden = connBurden;
                    min = conn;
                }
            }
            if (burden > MAX_BURDEN) {
                // can't send anything rn
                break;
            }
            pendingTasks.poll(); // actually take it
            min.acceptTask(task);
        }
    }

    public synchronized Collection<Task> getPendingTasks() {
        return new ArrayList<>(pendingTasks);
    }

    public synchronized Collection<Connection> getOpenConnections() {
        return new ArrayList<>(connections);
    }

    public void worldUpdate() { // something has changed (a connection has completed a task). time to get a new one
        NoComment.executor.execute(this::sendTasksOnConnections);
    }

    public void serverUpdate() {
        NoComment.executor.execute(server::update);
    }
}