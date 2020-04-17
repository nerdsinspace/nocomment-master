package nocomment.master;

import nocomment.master.network.Connection;
import nocomment.master.task.BlockCheckManager;
import nocomment.master.task.CombinedTask;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.task.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

public class World {
    private static final int MAX_BURDEN = 1000; // about 2.3 seconds
    public final Server server;
    private final List<Connection> connections;
    private final PriorityQueue<PriorityDispatchable> pending;
    public final short dimension;
    public final BlockCheckManager blockCheckManager;

    public World(Server server, short dimension) {
        this.server = server;
        this.connections = new ArrayList<>();
        this.pending = new PriorityQueue<>();
        this.dimension = dimension;
        this.blockCheckManager = new BlockCheckManager(this);

        // TODO schedule a task to grab the connections and kick the oldest one when it's time
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
        conn.forEachDispatch(pending::add);
        worldUpdate();
        serverUpdate(); // only for connection status change
    }

    public synchronized void submit(PriorityDispatchable dispatch) {
        if (dispatch instanceof BlockCheckManager.BlockCheck && recheck((BlockCheckManager.BlockCheck) dispatch)) {
            return;
        }
        pending.add(dispatch);
        worldUpdate();
        // don't server update per-task!
    }

    private boolean recheck(BlockCheckManager.BlockCheck chk) {
        for (PriorityDispatchable dup0 : pending) {
            if (dup0 instanceof BlockCheckManager.BlockCheck) {
                BlockCheckManager.BlockCheck dup = (BlockCheckManager.BlockCheck) dup0;
                if (dup.pos.equals(chk.pos)) {
                    pending.remove(dup0);
                    pending.add(chk.priority < dup.priority ? chk : dup);
                    worldUpdate();
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized Task submitTaskUnlessAlreadyPending(Task task) {
        for (PriorityDispatchable dup : pending) {
            if (dup instanceof Task && ((Task) dup).interchangable(task)) {
                System.out.println("Already queued. Not adding duplicate task. Queue size is " + pending.size());
                pending.remove(dup);
                pending.add(new CombinedTask(task, (Task) dup));
                return task;
            }
        }
        submit(task);
        return task;
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
            Connection min = connections.get(0);
            int burden = min.sumHigherPriority(toDispatch.priority);
            for (int i = 1; i < connections.size(); i++) {
                Connection conn = connections.get(i);
                int connBurden = conn.sumHigherPriority(toDispatch.priority);
                if (connBurden < burden) {
                    burden = connBurden;
                    min = conn;
                }
            }
            if (burden > MAX_BURDEN) {
                // can't send anything rn
                break;
            }
            pending.poll(); // actually take it
            toDispatch.dispatch(min);
        }
    }

    public synchronized Collection<PriorityDispatchable> getPending() {
        return new ArrayList<>(pending);
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