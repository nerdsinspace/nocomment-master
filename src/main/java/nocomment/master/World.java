package nocomment.master;

import nocomment.master.network.Connection;
import nocomment.master.task.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

public class World {
    private static final int MAX_BURDEN = 400; // about a second
    public final Server server;
    private final List<Connection> connections;
    private final PriorityQueue<Task> pendingTasks;
    public final int dimension;

    public World(Server server, int dimension) {
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
        conn.forEachTask(this::submitTask);
        worldUpdate();
        serverUpdate(); // only for connection status change
    }

    public synchronized void submitTask(Task task) {
        pendingTasks.add(task);
        worldUpdate();
        // don't server update per-task!
    }

    private synchronized void sendTasksOnConnections() {
        if (connections.isEmpty()) {
            return;
        }
        while (!pendingTasks.isEmpty()) {
            Task task = pendingTasks.peek();
            // create a map from a connection to the number of checks that connection still has to run through before it could get to this task in question
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
                //System.out.println("Too many tasks on this connection " + burden + " lower than " + task.priority);
                break;
            }
            pendingTasks.poll();
            int sum = 0;
            for (Object o : pendingTasks.toArray()) {
                sum += ((Task) o).count;
            }
            //System.out.println("Selected connection with burden " + burden + " for task with priority " + task.priority + " and size " + task.count + " from " + task.start + ". Total sum pending is " + sum);
            min.acceptTask(task);
        }
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