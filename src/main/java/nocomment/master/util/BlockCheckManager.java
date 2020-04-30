package nocomment.master.util;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.task.PriorityDispatchable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class BlockCheckManager {
    public final World world;
    private final Map<BlockPos, BlockCheckStatus> statuses = new HashMap<>();

    public BlockCheckManager(World world) {
        this.world = world;
    }

    private synchronized BlockCheckStatus get(BlockPos pos) {
        return statuses.computeIfAbsent(pos, BlockCheckStatus::new);
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
            try {
                checkedDatabaseYet.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            synchronized (this) {
                if (reply.isPresent() && responseAt > mustBeNewerThan) {
                    OptionalInt state = reply.get();
                    NoComment.executor.execute(() -> listener.accept(state));
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
        }

        public synchronized void onResponse(OptionalInt state) {
            responseAt = System.currentTimeMillis();
            reply = Optional.of(state);
            highestSubmittedPriority = Integer.MAX_VALUE; // reset
            inFlight.forEach(PriorityDispatchable::cancel); // unneeded
            inFlight.clear();
            for (Consumer<OptionalInt> listener : listeners) {
                NoComment.executor.execute(() -> listener.accept(state));
            }
            listeners.clear();
            if (state.isPresent()) {
                NoComment.executor.execute(() -> saveToDatabase(state.getAsInt(), responseAt));
            }
        }

        private void saveToDatabase(int blockState, long timestamp) {
            try (Connection connection = Database.getConnection();
                 PreparedStatement stmt = connection.prepareStatement("INSERT INTO blocks (x, y, z, block_state, created_at, dimension, server_id) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                stmt.setInt(1, pos.x);
                stmt.setShort(2, (short) pos.y);
                stmt.setInt(3, pos.z);
                stmt.setInt(4, blockState);
                stmt.setLong(5, timestamp);
                stmt.setShort(6, world.dimension);
                stmt.setShort(7, world.server.serverID);
                stmt.execute();
            } catch (SQLException ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }
    }
}
