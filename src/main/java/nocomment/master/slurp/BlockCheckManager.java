package nocomment.master.slurp;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.BlockPos;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class BlockCheckManager {
    public final World world;
    private final Map<ChunkPos, Map<BlockPos, BlockCheckStatus>> statuses = new HashMap<>();
    private final Map<ChunkPos, Long> observedUnloaded = new HashMap<>();
    private final LinkedBlockingQueue<BlockCheckStatus.ResultToInsert> results = new LinkedBlockingQueue<>();
    private final Object pruneLock = new Object();
    public static final long PRUNE_AGE = TimeUnit.MINUTES.toMillis(60);

    // check status are spammed WAY too fast
    // this executor is for database fetches from blocks or signs
    // to avoid overwhelming the main executor with literally thousands of DB queries for blocks and signs from the past
    public static Executor checkStatusExecutor = new LoggingExecutor(Executors.newFixedThreadPool(4));

    public BlockCheckManager(World world) {
        this.world = world;
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::update), 0, 250, TimeUnit.MILLISECONDS);
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::blockPrune), 30, 60, TimeUnit.MINUTES);
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
                toInsert.clear();
            } while (results.size() >= 100);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    @FunctionalInterface
    public interface BlockListener {
        void accept(OptionalInt state, boolean updated);
    }

    private synchronized BlockCheckStatus get(BlockPos pos) {
        return statuses.computeIfAbsent(new ChunkPos(pos), cpos -> new HashMap<>()).computeIfAbsent(pos, BlockCheckStatus::new);
    }

    private synchronized OptionalLong isUnloaded(ChunkPos pos) {
        if (observedUnloaded.containsKey(pos)) {
            return OptionalLong.of(observedUnloaded.get(pos));
        }
        return OptionalLong.empty();
    }

    private synchronized void unloadedAt(ChunkPos pos, long now) {
        if (!observedUnloaded.containsKey(pos) || observedUnloaded.get(pos) < now) {
            observedUnloaded.put(pos, now);
        }
        // wrap in executor to prevent stupid deadlock again
        List<BlockCheckStatus> s = new ArrayList<>(statuses.get(pos).values());
        NoComment.executor.execute(() -> s.forEach(status -> status.onResponseInternal(OptionalInt.empty(), now)));
    }

    private synchronized void loaded(ChunkPos pos) {
        observedUnloaded.remove(pos);
    }

    private synchronized void prune() {
        long now = System.currentTimeMillis();
        observedUnloaded.values().removeIf(aLong -> aLong < now - TimeUnit.SECONDS.toMillis(10));

    }

    public synchronized boolean hasBeenRemoved(BlockPos pos) {
        ChunkPos cpos = new ChunkPos(pos);
        return !statuses.containsKey(cpos) || !statuses.get(cpos).containsKey(pos);
    }

    private synchronized int cacheSize() {
        return statuses.values().stream().mapToInt(Map::size).sum();
    }

    private synchronized void blockPrune() {
        long now = System.currentTimeMillis();
        int beforeSz = cacheSize();
        synchronized (pruneLock) {
            statuses.values().forEach(m -> {
                Iterator<BlockCheckStatus> it = m.values().iterator();
                while (it.hasNext()) {
                    BlockCheckStatus bcs = it.next();
                    if (bcs.maybePrunable(now)) {
                        synchronized (bcs) {
                            if (bcs.actuallyPrunable(now)) {
                                it.remove(); // must call remove within bcs lock!!
                            }
                        }
                    }
                }
            });
            statuses.values().removeIf(Map::isEmpty); // any chunk with no remaining checks can be removed, just to save some ram lol
        }
        System.out.println("Block prune in block check manager took " + (System.currentTimeMillis() - now) + "ms. Cache size went from " + beforeSz + " to " + cacheSize());
    }

    public void requestBlockState(long mustBeNewerThan, BlockPos pos, int priority, BlockListener onCompleted) {
        NoComment.executor.execute(() -> {
            synchronized (BlockCheckManager.this) { // i hate myself
                synchronized (pruneLock) {
                    get(pos).requested(mustBeNewerThan, priority, onCompleted); // this is fine since requested holds no locks
                }
            }
        });
    }

    public class BlockCheckStatus {
        public final BlockPos pos;
        private int highestSubmittedPriority = Integer.MAX_VALUE;
        private final List<BlockListener> listeners;
        private final List<BlockCheck> inFlight;
        private OptionalInt blockState;
        private long responseAt;
        private CompletableFuture<Boolean> checkedDatabaseYet;
        private long lastActivity;

        private BlockCheckStatus(BlockPos pos) {
            this.listeners = new ArrayList<>();
            this.pos = pos;
            this.inFlight = new ArrayList<>();
            this.blockState = OptionalInt.empty();
            this.checkedDatabaseYet = new CompletableFuture<>();
            lastActivity = System.currentTimeMillis();
            checkStatusExecutor.execute(this::checkDatabase);
            if (!Thread.holdsLock(BlockCheckManager.this)) {
                throw new IllegalStateException();
            }
        }

        private boolean maybePrunable(long now) {
            return checkedDatabaseYet.isDone() && lastActivity < now - PRUNE_AGE;
        }

        private boolean actuallyPrunable(long now) {
            if (!Thread.holdsLock(BlockCheckStatus.this)) {
                throw new IllegalStateException();
            }
            return maybePrunable(now) && inFlight.isEmpty() && listeners.isEmpty();
        }

        private synchronized void checkDatabase() { // this synchronized is just for peace of mind, it should never actually become necessary
            try (Connection connection = Database.getConnection();
                 PreparedStatement stmt = connection.prepareStatement("SELECT block_state, created_at FROM blocks WHERE x = ? AND y = ? AND z = ? AND dimension = ? AND server_id = ? ORDER BY created_at DESC LIMIT 1")) {
                stmt.setInt(1, pos.x);
                stmt.setShort(2, (short) pos.y);
                stmt.setInt(3, pos.z);
                stmt.setShort(4, world.dimension);
                stmt.setShort(5, world.server.serverID);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        blockState = OptionalInt.of(rs.getInt("block_state"));
                        responseAt = rs.getLong("created_at");
                    }
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            } finally {
                checkedDatabaseYet.complete(true);
            }
        }

        public void requested(long mustBeNewerThan, int priority, BlockListener listener) {
            lastActivity = System.currentTimeMillis();
            checkedDatabaseYet.thenAcceptAsync(ignored -> requested0(mustBeNewerThan, priority, listener), NoComment.executor);
        }

        private synchronized void requested0(long mustBeNewerThan, int priority, BlockListener listener) {
            lastActivity = System.currentTimeMillis();
            // first, check if cached loaded (e.g. from db)
            if (blockState.isPresent() && responseAt > mustBeNewerThan) {
                OptionalInt state = blockState;
                NoComment.executor.execute(() -> listener.accept(state, false));
                return;
            }
            // then, check if cached unloaded
            OptionalLong unloadedAt = isUnloaded(new ChunkPos(pos));
            if (unloadedAt.isPresent() && unloadedAt.getAsLong() > mustBeNewerThan && !(blockState.isPresent() && responseAt > unloadedAt.getAsLong())) {
                // if this is unloaded, since the newer than, and not older than a real response
                // then that's what we do
                onResponseInternal(OptionalInt.empty(), unloadedAt.getAsLong());
                NoComment.executor.execute(() -> listener.accept(OptionalInt.empty(), false));
                return;
            }
            listeners.add(listener);
            if (priority < highestSubmittedPriority) {
                highestSubmittedPriority = priority;

                BlockCheck check = new BlockCheck(priority, this);
                inFlight.add(check);
                NoComment.executor.execute(() -> world.submit(check));
            }
        }

        public void onResponse(OptionalInt state) {
            lastActivity = System.currentTimeMillis();
            long now = System.currentTimeMillis();
            if (state.isPresent()) {
                onResponseInternal(state, now);
                loaded(new ChunkPos(pos));
            } else {
                unloadedAt(new ChunkPos(pos), now);
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
            boolean updated = state.isPresent() && blockState.isPresent() && blockState.getAsInt() != state.getAsInt();
            if (state.isPresent()) {
                responseAt = timestamp;
                blockState = state;
                results.add(new ResultToInsert(state.getAsInt(), responseAt));
            }
            highestSubmittedPriority = Integer.MAX_VALUE; // reset
            inFlight.forEach(PriorityDispatchable::cancel); // unneeded
            inFlight.clear();
            for (BlockListener listener : listeners) {
                NoComment.executor.execute(() -> listener.accept(state, updated));
            }
            listeners.clear();
        }

        private class ResultToInsert {
            private final int blockState;
            private final long timestamp;

            private ResultToInsert(int blockState, long timestamp) {
                this.blockState = blockState;
                this.timestamp = timestamp;
            }

            private void setupStatement(PreparedStatement stmt) throws SQLException {
                stmt.setInt(1, pos.x);
                stmt.setShort(2, (short) pos.y);
                stmt.setInt(3, pos.z);
                stmt.setInt(4, blockState);
                stmt.setLong(5, timestamp);
                stmt.setShort(6, world.dimension);
                stmt.setShort(7, world.server.serverID);
            }
        }
    }
}
