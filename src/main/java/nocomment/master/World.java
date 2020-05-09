package nocomment.master;

import nocomment.master.network.Connection;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.task.Task;
import nocomment.master.util.BlockCheckManager;
import nocomment.master.util.Staggerer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class World {
    private static final int MAX_BURDEN = 1000; // about 2.3 seconds
    public final Server server;
    private final List<Connection> connections;
    private final PriorityQueue<PriorityDispatchable> pending;
    public final short dimension;
    public final BlockCheckManager blockCheckManager;
    private final LinkedBlockingQueue<Boolean> taskSendSignal;

    public World(Server server, short dimension) {
        this.server = server;
        this.connections = new ArrayList<>();
        this.pending = new PriorityQueue<>();
        this.dimension = dimension;
        this.blockCheckManager = new BlockCheckManager(this);
        this.taskSendSignal = new LinkedBlockingQueue<>();
        new Staggerer(this).start();
        NoComment.executor.execute(this::taskSendLoop);
    }

    public synchronized void incomingConnection(Connection connection) {
        System.out.println("Connection identified as UUID " + connection.getUUID() + " which is player database ID " + connection.getIdentity());
        connections.add(connection);
        NoComment.executor.execute(connection::readLoop);
        NoComment.executor.execute(connection::writeLoop);
        worldUpdate();
        // dont preemptively update server until data comes in tbh
    }

    public synchronized void connectionClosed(Connection conn) {
        // due to the read loop structure, by the time we get here we know for a fact that this connection will read no further data, since its read loop is done
        // so, shuffling the tasks elsewhere is safe, and doesn't risk "completed" being called twice or anything like that
        connections.remove(conn);
        conn.forEachDispatch(pending::add);
        worldUpdate();
        serverUpdate(); // only for connection status change
    }

    public synchronized void submit(PriorityDispatchable dispatch) {
        pending.add(dispatch);
        worldUpdate();
        // don't server update per-task!
    }

    public synchronized Task submitTaskUnlessAlreadyPending(Task task) {
        for (PriorityDispatchable dup : pending) {
            if (dup instanceof Task && ((Task) dup).interchangable(task)) {
                System.out.println("Already queued. Not adding duplicate task. Queue size is " + pending.size());
                //pending.remove(dup);
                //pending.add(new CombinedTask(task, (Task) dup));
                //return task;
                return (Task) dup;
            }
        }
        submit(task);
        return task;
    }

    private void taskSendLoop() {
        try {
            while (true) {
                taskSendSignal.take(); // block
                taskSendSignal.clear(); // clear all extras
                sendTasksOnConnections();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void sendTasksOnConnections() {
        if (connections.isEmpty()) {
            return;
        }
        while (!pending.isEmpty()) {
            PriorityDispatchable toDispatch = pending.peek();
            if (toDispatch.isCanceled()) {
                pending.poll();
                continue;
            }
            Connection conn = selectConnectionFor(toDispatch);
            if (conn == null) {
                break; // can't send anything rn, burden too high on all conns. backpressure time!
            }
            pending.poll(); // actually take toDispatch off the heap
            toDispatch.dispatch(conn);
        }
    }

    private Connection selectConnectionFor(PriorityDispatchable toDispatch) {
        Connection bestConn = null;
        int minBurden = Integer.MAX_VALUE;
        for (Connection conn : connections) {
            int burden = conn.sumHigherPriority(toDispatch.priority);
            if (burden > MAX_BURDEN && conn.countHigherPriority(toDispatch.priority) > 1) {
                // even if burden is above max, allow it but ONLY if ALL that burden is from just ONE task
                continue;
            }
            if (toDispatch.hasAffinity(conn)) {
                // affinity = send on this conn if possible, EVEN IF it isn't the lowest burden connection
                return conn;
            }
            if (burden < minBurden) {
                minBurden = burden;
                bestConn = conn;
            }
        }
        return bestConn;
    }

    public synchronized Collection<PriorityDispatchable> getPending() {
        return new ArrayList<>(pending);
    }

    public synchronized Collection<Connection> getOpenConnections() {
        return new ArrayList<>(connections);
    }

    public void worldUpdate() { // something has changed (a connection has completed a task). time to get a new one
        taskSendSignal.add(true);
    }

    public void serverUpdate() {
        NoComment.executor.execute(server::update);
    }
}