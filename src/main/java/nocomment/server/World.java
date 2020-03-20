package nocomment.server;

import java.util.*;
import java.util.stream.Collectors;

public class World {
    private static final int MAX_BURDEN = 400; // about a second
    private final Server server;
    private final List<Connection> connections;
    private final PriorityQueue<Task> pendingTasks;

    public World(Server server) {
        this.server = server;
        this.connections = new ArrayList<>();
        this.pendingTasks = new PriorityQueue<>(Comparator.comparingInt(task -> task.priority));
    }

    public synchronized void incomingConnection(Connection connection) {
        connections.add(connection);
        NoComment.executor.execute(connection::readLoop);
        update();
    }

    public synchronized void submitTask(Task task) {
        pendingTasks.add(task);
        update();
    }

    private synchronized void sendTasksOnConnections() {
        if (connections.isEmpty()) {
            return;
        }
        while (!pendingTasks.isEmpty()) {
            Task task = pendingTasks.poll();
            // create a map from a connection to the number of checks that connection still has to run through before it could get to this task in question
            Map<Connection, Integer> burdens = connections.stream().collect(Collectors.toMap(c -> c, c -> c.sumHigherPriority(task.priority)));
            Connection connection = connections.stream().min(Comparator.comparingInt(burdens::get)).get();
            int burden = burdens.get(connection);

            if (burden > MAX_BURDEN) {
                // can't send anything rn
                break;
            }

            System.out.println("Selected connection with burden " + burden + " for task with priority " + task.priority + " and size " + task.count);
            connection.acceptTask(task);
        }
    }

    public void update() { // something has changed (a connection has completed a task). time to get a new one
        NoComment.executor.execute(this::sendTasksOnConnections);
    }

    public synchronized void connectionClosed(Connection conn) {
        // due to the read loop structure, by the time we get here we know for a fact that this connection will read no further data, since its read loop is done
        // so, shuffling the tasks elsewhere is safe, and doesn't risk "completed" being called twice or anything like that
        connections.remove(conn);
        conn.forEachTask(this::submitTask);
        update();
    }
}