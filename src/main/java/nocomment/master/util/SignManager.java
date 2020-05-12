package nocomment.master.util;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.db.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

public class SignManager {
    public final World world;
    private final Map<BlockPos, List<Consumer<Optional<byte[]>>>> signListeners;

    public SignManager(World world) {
        this.world = world;
        this.signListeners = new HashMap<>();
    }

    public synchronized void request(long mustBeNewerThan, BlockPos pos, Consumer<Optional<byte[]>> consumer) {
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
                    consumer.accept(Optional.of(rs.getBytes("nbt")));
                    return;
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
        signListeners.computeIfAbsent(pos, p -> new ArrayList<>()).add(consumer);
        world.submitSign(pos);
    }


    public synchronized void response(BlockPos pos, Optional<byte[]> data) {
        List<Consumer<Optional<byte[]>>> listeners = signListeners.remove(pos);
        if (listeners != null) {
            NoComment.executor.execute(() -> listeners.forEach(l -> l.accept(data)));
        }
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
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
