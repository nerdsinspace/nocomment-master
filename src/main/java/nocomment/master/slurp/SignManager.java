package nocomment.master.slurp;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.util.BlockPos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

public final class SignManager {
    public final World world;
    private final Map<BlockPos, List<Consumer<Optional<byte[]>>>> signListeners;

    public SignManager(World world) {
        this.world = world;
        this.signListeners = new HashMap<>();
    }

    public void requestAsync(long mustBeNewerThan, BlockPos pos, Consumer<Optional<byte[]>> consumer) {
        BlockCheckManager.checkStatusExecutor.execute(() -> request(mustBeNewerThan, pos, consumer));
    }

    public void request(long mustBeNewerThan, BlockPos pos, Consumer<Optional<byte[]>> consumer) {
        try (Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT nbt FROM signs WHERE x = ? AND y = ? AND z = ? AND dimension = ? AND server_id = ? AND created_at > ? ORDER BY created_at DESC LIMIT 1")) {
            stmt.setInt(1, pos.x);
            stmt.setShort(2, (short) pos.y);
            stmt.setInt(3, pos.z);
            stmt.setShort(4, world.dimension);
            stmt.setShort(5, world.server.serverID);
            stmt.setLong(6, mustBeNewerThan);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    byte[] nbt = rs.getBytes("nbt");
                    // can't let rs escape try-with-resources into this lambda lmao
                    NoComment.executor.execute(() -> consumer.accept(Optional.of(nbt)));
                    return;
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
        synchronized (this) {
            signListeners.computeIfAbsent(pos, p -> new ArrayList<>()).add(consumer);
        }
        world.submitSign(pos);
    }

    public synchronized void response(BlockPos pos, Optional<byte[]> data) {
        List<Consumer<Optional<byte[]>>> listeners = signListeners.remove(pos);
        if (listeners != null) {
            NoComment.executor.execute(() -> listeners.forEach(l -> l.accept(data)));
        }
        // this can use main executor since sign RESPONSES (unlike sign requests) will occur at max once a second per bot
        data.ifPresent(bytes -> NoComment.executor.execute(() -> saveToDB(pos, bytes)));
    }

    private void saveToDB(BlockPos pos, byte[] data) {
        try (Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO signs (x, y, z, nbt, created_at, dimension, server_id) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            stmt.setInt(1, pos.x);
            stmt.setShort(2, (short) pos.y);
            stmt.setInt(3, pos.z);
            stmt.setBytes(4, data);
            stmt.setLong(5, System.currentTimeMillis());
            stmt.setShort(6, world.dimension);
            stmt.setShort(7, world.server.serverID);
            stmt.execute();
            Database.incrementCommitCounter("sign");
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
