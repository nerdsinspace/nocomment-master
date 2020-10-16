package nocomment.master;

import io.prometheus.client.Gauge;
import nocomment.master.network.Connection;
import nocomment.master.slurp.BlockCheckManager;
import nocomment.master.slurp.SignManager;
import nocomment.master.slurp.SlurpManager;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.task.PriorityDispatchableBinaryHeap;
import nocomment.master.task.Task;
import nocomment.master.util.BlockPos;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.Staggerer;
import nocomment.master.util.WorldStatistics;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public final class World {

    private static final Gauge worldQueueLength = Gauge.build()
            .name("world_queue_length")
            .help("Length of the world queue")
            .labelNames("dimension")
            .register();

    private static final int MAX_BURDEN = 400;
    public final Server server;
    private final List<Connection> connections;
    private final LinkedBlockingQueue<PriorityDispatchable> toRemove;
    private final Map<Task.InterchangeabilityKey, List<Task>> taskDedup;
    private final PriorityDispatchableBinaryHeap heap;
    public final short dimension;
    public final BlockCheckManager blockCheckManager;
    private final LinkedBlockingQueue<Boolean> taskSendSignal;
    public final SignManager signManager;
    public final WorldStatistics stats;
    private final SlurpManager slurpManager;
    private final String dim;

    public World(Server server, short dimension) {
        this.server = server;
        this.connections = new ArrayList<>();
        this.heap = new PriorityDispatchableBinaryHeap();
        this.toRemove = new LinkedBlockingQueue<>();
        this.taskDedup = new HashMap<>();
        this.dimension = dimension;
        this.blockCheckManager = new BlockCheckManager(this);
        this.taskSendSignal = new LinkedBlockingQueue<>();
        this.signManager = new SignManager(this);
        this.stats = new WorldStatistics(this);
        if (dimension == 0) {
            this.slurpManager = new SlurpManager(this);
        } else {
            this.slurpManager = null;
        }
        this.dim = dimension + "";
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
        conn.forEachDispatch(this::submit);
        conn.forEachPendingSign(this::submitSign);
        worldUpdate();
        serverUpdate(); // only for connection status change
    }

    public synchronized void submit(PriorityDispatchable dispatch) {
        if (dispatch instanceof Task) {
            taskDedup.computeIfAbsent(((Task) dispatch).key(), $ -> new ArrayList<>()).add((Task) dispatch);
        }
        heap.insert(dispatch);
        worldUpdate();
        // don't server update per-task!
    }

    public void cancelAndRemoveAsync(PriorityDispatchable dispatch) {
        dispatch.cancel();
        toRemove.add(dispatch);
        worldUpdate();
    }

    private synchronized void remove(PriorityDispatchable dispatch) {
        if (heap.remove(dispatch)) {
            removeFromDedup(dispatch);
        }
    }

    public synchronized Task submitTaskUnlessAlreadyPending(Task task) {
        List<Task> options = taskDedup.get(task.key());
        if (options != null) {
            System.out.println("Already queued. Not adding duplicate task. Queue size is " + heap.size());
            return options.get(0);
        }
        submit(task);
        return task;
    }

    private void taskSendLoop() {
        try {
            while (true) {
                taskSendSignal.take(); // block
                taskSendSignal.clear(); // clear all extras
                consumeRemovalQueue();
                sendTasksOnConnections();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void consumeRemovalQueue() {
        if (toRemove.isEmpty()) {
            return;
        }
        List<PriorityDispatchable> queued = new ArrayList<>(toRemove.size());
        toRemove.drainTo(queued);
        queued.forEach(this::remove);
    }

    private synchronized void removeFromDedup(PriorityDispatchable dispatch) {
        if (!(dispatch instanceof Task)) {
            return;
        }
        Task task = (Task) dispatch;
        List<Task> dedup = taskDedup.get(task.key());
        dedup.remove(task);
        if (dedup.isEmpty()) {
            taskDedup.remove(task.key());
        }
    }

    private synchronized void sendTasksOnConnections() {
        worldQueueLength.labels(dim()).set(heap.size());
        if (connections.isEmpty()) {
            return;
        }
        while (!heap.isEmpty()) {
            PriorityDispatchable toDispatch = heap.peekLowest();
            if (toDispatch.isCanceled()) {
                heap.removeLowest();
                removeFromDedup(toDispatch);
                continue;
            }
            Connection conn = selectConnectionFor(toDispatch);
            if (conn == null) {
                break; // can't send anything rn, burden too high on all conns. backpressure time!
            }
            heap.removeLowest(); // actually take toDispatch off the heap
            removeFromDedup(toDispatch);
            toDispatch.dispatch(conn);
        }
    }

    private Connection selectConnectionFor(PriorityDispatchable toDispatch) {
        Connection bestConn = null;
        int minBurden = Integer.MAX_VALUE;
        for (Connection conn : connections) {
            if (toDispatch.hasAffinity(conn)) {
                // will be cached anyway
                return conn;
            }
            int burden = conn.sumHigherPriority(toDispatch.priority);
            if (burden > MAX_BURDEN) {
                continue;
            }
            if (burden < minBurden) {
                minBurden = burden;
                bestConn = conn;
            }
        }
        return bestConn;
    }

    public synchronized void submitSign(BlockPos pos) {
        if (connections.isEmpty()) {
            // can't do signManager.response while holding synchronized(World) because SignManager also
            // synchronizes on a database access, in request, and we don't want World to block on db
            NoComment.executor.execute(() -> signManager.response(pos, Optional.empty()));
            return;
        }
        // must hold synchronized so that we don't race with connectionClosed for sign redistributing
        connections.get(new Random().nextInt(connections.size())).acceptSignCheck(pos);
    }

    public synchronized Collection<PriorityDispatchable> getPending() {
        return heap.copy();
    }

    public synchronized List<Connection> getOpenConnections() {
        return new ArrayList<>(connections);
    }

    public void worldUpdate() { // something has changed (a connection has completed a task). time to get a new one
        taskSendSignal.add(true);
    }

    public void serverUpdate() {
        NoComment.executor.execute(server::update);
    }

    public void notifyHit(ChunkPos pos, int prio) {
        NoComment.executor.execute(() -> {
            stats.hitReceived(prio);
            if (slurpManager != null) {
                slurpManager.arbitraryHitExternal(pos); // notify slurper that a filter has hit this chunk
            }
        });
    }

    public void dbscanUpdate(ChunkPos cpos) {
        if (slurpManager != null) {
            slurpManager.clusterUpdate(cpos);
        }
    }

    public String dim() {
        return dim;
    }
}