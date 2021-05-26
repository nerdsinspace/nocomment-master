package nocomment.master.slurp;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.BlockPos;
import nocomment.master.util.LoggingExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public final class BlockCheckManager {

    private static final Histogram blockPruneLatencies = Histogram.build()
            .name("block_prune_latencies")
            .help("Block prune latencies")
            .labelNames("dimension")
            .register();
    private static final Gauge checkStatusQueueLength = Gauge.build()
            .name("check_status_queue_length")
            .help("Length of the check status queue")
            .register();
    private static final Gauge checkStatusQueueLatency = Gauge.build()
            .name("check_status_queue_latency")
            .help("Age of the head of the check status queue")
            .register();
    private static final Counter checksRan = Counter.build()
            .name("checks_ran_total")
            .help("Number of checks we have run")
            .register();
    private static final Gauge blockCheckStatuses = Gauge.build()
            .name("block_check_statuses_size")
            .help("Size of the block check statuses")
            .labelNames("dimension")
            .register();
    public final World world;
    private final Long2ObjectOpenHashMap<Long2ObjectOpenHashMap<BlockCheckStatus>> statuses = new Long2ObjectOpenHashMap<>();
    private final Long2LongOpenHashMap observedUnloaded = new Long2LongOpenHashMap();
    private final LinkedBlockingQueue<BlockCheckStatus.ResultToInsert> results = new LinkedBlockingQueue<>();
    private final Object pruneLock = new Object();
    private static final long PRUNE_INTERVAL = TimeUnit.MINUTES.toMillis(15);
    private static final long PRUNE_AGE = TimeUnit.MINUTES.toMillis(30);

    // check status are spammed WAY too fast
    // this executor is for database fetches from blocks or signs
    // to avoid overwhelming the main executor with literally thousands of DB queries for blocks and signs from the past
    public static LinkedBlockingQueue<BlockCheckStatus> checkStatusQueue = new LinkedBlockingQueue<>();
    private static LinkedBlockingQueue<Runnable> checkStatusExecutorQueue = new LinkedBlockingQueue<>();
    public static Executor checkStatusExecutor = new LoggingExecutor(new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, checkStatusExecutorQueue), "check_status");

    public static Executor unloadObservationExecutor = new LoggingExecutor(Executors.newFixedThreadPool(4), "unload_observation");

    public BlockCheckManager(World world) {
        this.world = world;
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::update), 0, 250, TimeUnit.MILLISECONDS);
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(() -> blockPruneLatencies.labels(world.dim()).time(this::blockPrune)), PRUNE_INTERVAL, PRUNE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void update() {
        prune();
        if (results.isEmpty()) {
            return;
        }
        List<BlockCheckStatus.ResultToInsert> toInsert = new ArrayList<>(100);
        try (Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO blocks (x, y, z, block_state, created_at, dimension, server_id) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            connection.setAutoCommit(false);
            do {
                results.drainTo(toInsert, 100);
                for (BlockCheckStatus.ResultToInsert result : toInsert) {
                    result.setupStatement(stmt);
                    stmt.execute();
                }
                connection.commit();
                Database.incrementCommitCounter("block_batched");
                toInsert.clear();
            } while (results.size() >= 100);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public enum BlockEventType {
        UNLOADED,
        FIRST_TIME,
        MATCHES_PREV,
        UPDATED,
        CACHED
    }

    @FunctionalInterface
    public interface BlockListener {

        void accept(OptionalInt state, BlockEventType type, long timestamp);
    }

    private synchronized BlockCheckStatus get(long bpos) {
        return statuses.computeIfAbsent(BlockPos.blockToChunk(bpos), cpos -> new Long2ObjectOpenHashMap<>()).computeIfAbsent(bpos, BlockCheckStatus::new);
    }

    private synchronized long isUnloaded(long cpos) {
        if (observedUnloaded.containsKey(cpos)) {
            return observedUnloaded.get(cpos);
        }
        return -1;
    }

    private synchronized void unloadedAt(long cpos, long now) {
        if (!observedUnloaded.containsKey(cpos) || observedUnloaded.get(cpos) < now) {
            observedUnloaded.put(cpos, now);
        }
        // wrap in executor to prevent stupid deadlock again
        List<BlockCheckStatus> s = new ArrayList<>(statuses.get(cpos).values());
        unloadObservationExecutor.execute(() -> s.forEach(status -> status.onResponseInternal(OptionalInt.empty(), now)));
    }

    public synchronized void loaded(long cpos) {
        observedUnloaded.remove(cpos);
    }

    private synchronized void prune() {
        long now = System.currentTimeMillis();
        long fence = now - TimeUnit.SECONDS.toMillis(10);
        ObjectIterator<Long2LongMap.Entry> it = observedUnloaded.long2LongEntrySet().fastIterator();
        while (it.hasNext()) {
            Long2LongMap.Entry entry = it.next();
            if (entry.getLongValue() < fence) {
                it.remove();
            }
        }
    }

    public synchronized boolean hasBeenRemoved(long bpos) {
        long cpos = BlockPos.blockToChunk(bpos);
        Long2ObjectOpenHashMap<BlockCheckStatus> thisChunk = statuses.get(cpos);
        return thisChunk == null || !thisChunk.containsKey(bpos);
    }

    private synchronized int cacheSize() {
        return statuses.values().stream().mapToInt(Map::size).sum();
    }

    private synchronized void blockPrune() {
        synchronized (pruneLock) {
            long now = System.currentTimeMillis();
            int beforeSz = cacheSize();
            int maybeButNotActually = 0;
            int numActuallyRemoved = 0;
            ObjectIterator<Long2ObjectMap.Entry<Long2ObjectOpenHashMap<BlockCheckStatus>>> outerIt = statuses.long2ObjectEntrySet().fastIterator();
            while (outerIt.hasNext()) {
                Long2ObjectMap.Entry<Long2ObjectOpenHashMap<BlockCheckStatus>> outerEntry = outerIt.next();
                ObjectIterator<Long2ObjectMap.Entry<BlockCheckStatus>> innerIt = outerEntry.getValue().long2ObjectEntrySet().fastIterator();
                while (innerIt.hasNext()) {
                    Long2ObjectMap.Entry<BlockCheckStatus> entry = innerIt.next();
                    BlockCheckStatus bcs = entry.getValue();
                    if (bcs.maybePrunable(now)) {
                        synchronized (bcs) {
                            if (bcs.actuallyPrunable(now)) {
                                innerIt.remove(); // must call remove within bcs lock!!
                                numActuallyRemoved++;
                            } else {
                                maybeButNotActually++;
                            }
                        }
                    }
                }
                if (outerEntry.getValue().isEmpty()) {
                    outerIt.remove();
                }
            }
            int afterSz = cacheSize();
            Map<Integer, Long> countByPriority = statuses.values().stream().map(Map::values).flatMap(Collection::stream).collect(Collectors.groupingBy(bcs -> bcs.highestSubmittedPriority, Collectors.counting()));
            System.out.println("Cache size change should be " + numActuallyRemoved + " but was actually " + (beforeSz - afterSz));
            blockCheckStatuses.labels(world.dim()).set(afterSz);
            System.out.println("FASTER? Block prune in block check manager took " + (System.currentTimeMillis() - now) + "ms. Cache size went from " + beforeSz + " to " + afterSz + ". Maybe but not actually: " + maybeButNotActually + ". Count by priority: " + countByPriority);
        }
    }

    public void requestBlockState(long mustBeNewerThan, BlockPos pos, int priority, BlockListener onCompleted) {
        long bpos = pos.toLong();
        NoComment.executor.execute(() -> {
            synchronized (BlockCheckManager.this) { // i hate myself
                synchronized (pruneLock) {
                    get(bpos).requested(mustBeNewerThan, priority, onCompleted); // this is fine since requested holds no lock                                                           s
                }
            }
        });
    }

    private static void checkStatusConsumer() {
        checkStatusQueueLength.set(checkStatusQueue.size());
        BlockCheckStatus head = checkStatusQueue.peek();
        if (head == null) {
            checkStatusQueueLatency.set(0); // no latency if queue is empty
            return;
        }
        checkStatusQueueLatency.set((System.currentTimeMillis() - head.constructedAt) / 1000.0d);
        List<BlockCheckStatus> statuses = new ArrayList<>(200);
        checkStatusQueue.drainTo(statuses, 200);
        if (statuses.isEmpty()) {
            return;
        }
        try (Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT block_state, created_at FROM blocks WHERE x = ? AND y = ? AND z = ? AND dimension = ? AND server_id = ? ORDER BY created_at DESC LIMIT 1")) {
            connection.setAutoCommit(false);
            for (BlockCheckStatus stat : statuses) {
                stat.checkDatabase(stmt);
            }
            checksRan.inc(statuses.size());
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static final CompletableFuture<Boolean> STATIC_DATABASE_CHECKED_COMPLETION = new CompletableFuture<>();

    static {
        STATIC_DATABASE_CHECKED_COMPLETION.complete(true);
    }

    public class BlockCheckStatus {

        public final long bpos;
        private int highestSubmittedPriority = Integer.MAX_VALUE;
        // split up these two lists into "first" and "rest" to optimize RAM for the common case which are only 0 or 1 entries
        private BlockListener firstListener;
        private List<BlockListener> otherListeners;
        private BlockCheck firstInFlight;
        private List<BlockCheck> otherInFlight;
        private int blockStateOptional;
        private boolean blockStateOptionalPresent; // this optional is "destructured" into an int + a boolean for the same reason - there are tens of millions of these on the heap, and every little thing counts
        private long responseAt;
        private CompletableFuture<Boolean> checkedDatabaseYet; // IMPORTANT: this field is overwritten after completion to STATIC_DATABASE_CHECKED_COMPLETION
        private long lastActivity;
        private final long constructedAt;

        private BlockCheckStatus(long bpos) {
            this.firstListener = null;
            this.otherListeners = null;
            this.bpos = bpos;
            this.firstInFlight = null;
            this.otherInFlight = null;
            this.checkedDatabaseYet = new CompletableFuture<>();
            long now = System.currentTimeMillis();
            lastActivity = now;
            constructedAt = now;
            checkStatusQueue.add(this);
            checkStatusQueueLength.set(checkStatusQueue.size());
            if (checkStatusExecutorQueue.size() < 10) {
                checkStatusExecutor.execute(BlockCheckManager::checkStatusConsumer);
            }
            if (!Thread.holdsLock(BlockCheckManager.this)) {
                throw new IllegalStateException();
            }
            blockCheckStatuses.labels(world.dim()).inc();
        }

        private boolean maybePrunable(long now) {
            return checkedDatabaseYet.isDone() && lastActivity < now - PRUNE_AGE;
        }

        private boolean actuallyPrunable(long now) {
            if (!Thread.holdsLock(BlockCheckStatus.this)) {
                throw new IllegalStateException();
            }
            return maybePrunable(now) && firstListener == null && otherListeners == null && firstInFlight == null && otherInFlight == null;
        }

        private synchronized void checkDatabase(PreparedStatement stmt) throws SQLException { // this synchronized is just for peace of mind, it should never actually become necessary
            try {
                final BlockPos pos = pos();
                stmt.setInt(1, pos.x);
                stmt.setShort(2, (short) pos.y);
                stmt.setInt(3, pos.z);
                stmt.setShort(4, world.dimension);
                stmt.setShort(5, world.server.serverID);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        blockStateOptionalPresent = true;
                        blockStateOptional = rs.getInt("block_state");
                        responseAt = rs.getLong("created_at");
                    }
                }
            } finally {
                // first, fire all async waiters blocking on checkedDatabaseYet
                checkedDatabaseYet.complete(true);
                // it's also okay if .thenAcceptAsync is called right here and races between these two lines - the old checkedDatabaseYet will instantly dispatch since it was previously completed
                checkedDatabaseYet = STATIC_DATABASE_CHECKED_COMPLETION;
                // from now on, checkedDatabaseYet just needs to be an insta-dispatching completed Future
                // this means it doesn't need to be a unique object
                // therefore, to save RAM, replace it with the singleton
            }
        }

        public void requested(long mustBeNewerThan, int priority, BlockListener listener) {
            lastActivity = System.currentTimeMillis();
            checkedDatabaseYet.thenAcceptAsync(ignored -> requested0(mustBeNewerThan, priority, listener), NoComment.executor);
        }

        private synchronized void requested0(long mustBeNewerThan, int priority, BlockListener listener) {
            lastActivity = System.currentTimeMillis();
            // first, check if cached loaded (e.g. from db)
            if (blockStateOptionalPresent && responseAt > mustBeNewerThan) {
                OptionalInt state = OptionalInt.of(blockStateOptional);
                NoComment.executor.execute(() -> listener.accept(state, BlockEventType.CACHED, responseAt));
                return;
            }
            // then, check if cached unloaded
            long unloadedAt = isUnloaded(cpos());
            if (unloadedAt != -1 && unloadedAt > mustBeNewerThan && !(blockStateOptionalPresent && responseAt > unloadedAt)) {
                // if this is unloaded, since the newer than, and not older than a real response
                // then that's what we do
                onResponseInternal(OptionalInt.empty(), unloadedAt);
                NoComment.executor.execute(() -> listener.accept(OptionalInt.empty(), BlockEventType.UNLOADED, unloadedAt));
                return;
            }
            if (firstListener == null) {
                firstListener = listener;
            } else {
                if (otherListeners == null) {
                    otherListeners = new ArrayList<>(1);
                }
                otherListeners.add(listener);
            }
            if (priority < highestSubmittedPriority) {
                highestSubmittedPriority = priority;

                BlockCheck check = new BlockCheck(priority, this);
                if (firstInFlight == null) {
                    firstInFlight = check;
                } else {
                    if (otherInFlight == null) {
                        otherInFlight = new ArrayList<>(1);
                    }
                    otherInFlight.add(check);
                }
                NoComment.executor.execute(() -> world.submit(check));
            }
        }

        public void onResponse(OptionalInt state) {
            lastActivity = System.currentTimeMillis();
            long now = System.currentTimeMillis();
            if (state.isPresent()) {
                onResponseInternal(state, now);
                loaded(cpos());
            } else {
                unloadedAt(cpos(), now);
            }
        }

        private synchronized void onResponseInternal(OptionalInt state, long timestamp) {
            if (state.isPresent()) {
                // chunk confirmed unloaded doesn't count as activity
                lastActivity = System.currentTimeMillis();
            }
            if (responseAt >= timestamp) {
                return;
            }
            BlockEventType type;
            if (!state.isPresent()) {
                type = BlockEventType.UNLOADED;
            } else if (!blockStateOptionalPresent) {
                type = BlockEventType.FIRST_TIME;
            } else if (state.getAsInt() == blockStateOptional) {
                type = BlockEventType.MATCHES_PREV;
            } else {
                type = BlockEventType.UPDATED;
            }
            if (state.isPresent()) {
                responseAt = timestamp;
                blockStateOptionalPresent = true;
                blockStateOptional = state.getAsInt();
                results.add(new ResultToInsert(state.getAsInt(), responseAt));
            }
            highestSubmittedPriority = Integer.MAX_VALUE; // reset
            if (firstInFlight != null) {
                world.cancelAndRemoveAsync(firstInFlight);
                if (otherInFlight != null) {
                    otherInFlight.forEach(world::cancelAndRemoveAsync); // unneeded
                }
            }
            if (firstListener != null) {
                BlockListener localCopy = firstListener;
                NoComment.executor.execute(() -> localCopy.accept(state, type, timestamp));
                if (otherListeners != null) {
                    for (BlockListener listener : otherListeners) {
                        NoComment.executor.execute(() -> listener.accept(state, type, timestamp));
                    }
                }
            }
            firstInFlight = null;
            otherInFlight = null;
            firstListener = null;
            otherListeners = null;
        }

        public final BlockPos pos() {
            return BlockPos.fromLong(bpos);
        }

        public final long cpos() {
            return BlockPos.blockToChunk(bpos);
        }

        private class ResultToInsert {

            private final int blockState;
            private final long timestamp;

            private ResultToInsert(int blockState, long timestamp) {
                this.blockState = blockState;
                this.timestamp = timestamp;
            }

            private void setupStatement(PreparedStatement stmt) throws SQLException {
                stmt.setInt(1, pos().x);
                stmt.setShort(2, (short) pos().y);
                stmt.setInt(3, pos().z);
                stmt.setInt(4, blockState);
                stmt.setLong(5, timestamp);
                stmt.setShort(6, world.dimension);
                stmt.setShort(7, world.server.serverID);
            }
        }
    }
}
