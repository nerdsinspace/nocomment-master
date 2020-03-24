package nocomment.master.db;

import java.sql.*;
import java.util.concurrent.CompletableFuture;

public class Database {
    public static Connection connection;

    static {
        try {
            System.out.println("Connecting to database...");
            // docker run --rm  --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 -v $HOME/docker/volumes/postgres:/var/lib/postgresql/data postgres
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "docker");
            System.out.println("Connected.");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static int idForHostname(String hostname) {
        try {
            try (PreparedStatement stmt = connection.prepareStatement("SELECT id FROM servers WHERE hostname = ?")) {
                stmt.setString(1, hostname);
                ResultSet existing = stmt.executeQuery();
                if (existing.next()) {
                    return existing.getInt("id");
                }
            }
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO servers (hostname) VALUES (?) RETURNING id")) {
                stmt.setString(1, hostname);
                ResultSet rs = stmt.executeQuery();
                rs.next();
                return rs.getInt("id");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    static void saveHit(Hit hit, CompletableFuture<Long> hitID) {
        try {
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO hits (created_at, x, z, dimension, server_id) VALUES (?, ?, ?, ?, ?) RETURNING id")) {
                stmt.setLong(1, hit.createdAt);
                stmt.setLong(2, hit.pos.x);
                stmt.setLong(3, hit.pos.z);
                stmt.setLong(4, hit.dimension);
                stmt.setLong(5, hit.serverID);
                ResultSet rs = stmt.executeQuery();
                rs.next();
                hitID.complete(rs.getLong("id"));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            hitID.completeExceptionally(ex);
            throw new RuntimeException(ex);
        }
    }
}
