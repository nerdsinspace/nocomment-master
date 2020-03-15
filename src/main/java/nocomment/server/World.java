package nocomment.server;

import java.util.ArrayList;
import java.util.List;

public class World {
    private final Server server;
    private final List<Connection> connections;
    private final List<Task> pendingTasks;

    public World(Server server) {
        this.server = server;
        this.connections = new ArrayList<>();
        this.pendingTasks = new ArrayList<>();
    }

    public synchronized void incomingConnection(Connection connection) {
        connections.add(connection);
        NoComment.executor.execute(connection::readLoop);
        sendTasksOnConnections();
    }

    public synchronized void submitTask(Task task) {
        pendingTasks.add(task);
        sendTasksOnConnections();
    }

    private synchronized void sendTasksOnConnections() {
        // TODO sort by priority
        // TODO give it to the connection with the least amount of burden
        if (connections.isEmpty()) {
            return;
        }
        while (!pendingTasks.isEmpty()) {
            connections.get(0).acceptTask(pendingTasks.remove(0));
        }
    }

    public synchronized void connectionClosed(Connection conn) {
        // due to the read loop structure, by the time we get here we know for a fact that this connection will read no further data, since its read loop is done
        // so, shuffling the tasks elsewhere is safe, and doesn't risk "completed" being called twice or anything like that
        connections.remove(conn);
        synchronized (conn) { // technically not actually a necessary synchronized...? i think...? i'm far from certain lol
            for (Task task : conn.tasks.values()) {
                submitTask(task);
            }
        }
        sendTasksOnConnections();
    }
}