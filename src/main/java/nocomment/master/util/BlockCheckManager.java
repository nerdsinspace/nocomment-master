package nocomment.master.util;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.tracking.TrackyTrackyManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BlockCheckManager {
    public final World world;
    private final Map<ChunkPos, Map<BlockPos, BlockCheckStatus>> statuses = new HashMap<>();
    private final Map<ChunkPos, Long> observedUnloaded = new HashMap<>();
    private final LinkedBlockingQueue<BlockCheckStatus.ResultToInsert> results = new LinkedBlockingQueue<>();

    public BlockCheckManager(World world) {
        this.world = world;
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::update), 0, 250, TimeUnit.MILLISECONDS);
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
        observedUnloaded.values().removeIf(aLong -> aLong < System.currentTimeMillis() - 10_000);
    }

    public void requestBlockState(long mustBeNewerThan, BlockPos pos, int priority, Consumer<OptionalInt> onCompleted) {
        NoComment.executor.execute(() -> get(pos).requested(mustBeNewerThan, priority, onCompleted));
    }

    public class BlockCheckStatus {
        public final BlockPos pos;
        private int highestSubmittedPriority = Integer.MAX_VALUE;
        private final List<Consumer<OptionalInt>> listeners;
        private final List<BlockCheck> inFlight;
        private Optional<OptionalInt> reply;
        private long responseAt;
        private CompletableFuture<Boolean> checkedDatabaseYet;

        private BlockCheckStatus(BlockPos pos) {
            this.listeners = new ArrayList<>();
            this.pos = pos;
            this.inFlight = new ArrayList<>();
            this.reply = Optional.empty();
            this.checkedDatabaseYet = new CompletableFuture<>();
            NoComment.executor.execute(this::checkDatabase);
            if (!Thread.holdsLock(BlockCheckManager.this)) {
                throw new IllegalStateException();
            }
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
                        reply = Optional.of(OptionalInt.of(rs.getInt("block_state")));
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

        public void requested(long mustBeNewerThan, int priority, Consumer<OptionalInt> listener) {
            checkedDatabaseYet.thenAcceptAsync(ignored -> requested0(mustBeNewerThan, priority, listener), NoComment.executor);
        }

        public synchronized void requested0(long mustBeNewerThan, int priority, Consumer<OptionalInt> listener) {
            // first, check if cached loaded (e.g. from db)
            if (reply.isPresent() && reply.get().isPresent() && responseAt > mustBeNewerThan) {
                OptionalInt state = reply.get();
                NoComment.executor.execute(() -> listener.accept(state));
                return;
            }
            // then, check if cached unloaded
            OptionalLong unloadedAt = isUnloaded(new ChunkPos(pos));
            if (unloadedAt.isPresent() && unloadedAt.getAsLong() > mustBeNewerThan && !(reply.isPresent() && responseAt > unloadedAt.getAsLong())) {
                // if this is unloaded, since the newer than, and not older than a real response
                // then that's what we do
                onResponseInternal(OptionalInt.empty(), unloadedAt.getAsLong());
                NoComment.executor.execute(() -> listener.accept(OptionalInt.empty()));
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
            long now = System.currentTimeMillis();
            onResponseInternal(state, now);
            if (state.isPresent()) {
                loaded(new ChunkPos(pos));
            } else {
                unloadedAt(new ChunkPos(pos), now);
            }
        }

        private synchronized void onResponseInternal(OptionalInt state, long timestamp) {
            if (responseAt >= timestamp) {
                return;
            }
            responseAt = timestamp;
            reply = Optional.of(state);
            highestSubmittedPriority = Integer.MAX_VALUE; // reset
            inFlight.forEach(PriorityDispatchable::cancel); // unneeded
            inFlight.clear();
            for (Consumer<OptionalInt> listener : listeners) {
                NoComment.executor.execute(() -> listener.accept(state));
            }
            listeners.clear();
            if (state.isPresent()) {
                results.add(new ResultToInsert(state.getAsInt(), responseAt));
            }
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
