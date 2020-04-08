package nocomment.master.clustering;

import nocomment.master.db.Database;
import nocomment.master.util.ChunkPos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

public enum HitRetry {
    INSTANCE;

    private static class DisjointTraversalCursor {
        private final int id;
        private final int disjointSize;

        private DisjointTraversalCursor(ResultSet rs) throws SQLException {
            this.id = rs.getInt("id");
            this.disjointSize = rs.getInt("disjoint_size");
        }

        private DisjointTraversalCursor fetchNthChild(Connection connection, int offset) throws SQLException {
            try (PreparedStatement stmt = connection.prepareStatement("SELECT id, disjoint_size FROM dbscan WHERE cluster_parent = ?")) {
                stmt.setInt(1, id);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        DisjointTraversalCursor child = new DisjointTraversalCursor(rs);
                        if (offset < child.disjointSize) {
                            return child;
                        }
                        offset -= child.disjointSize;
                    }
                    throw new IllegalStateException("Unable to get " + offset + "th child of " + id + " with size " + disjointSize);
                }
            }
        }
    }

    private DisjointTraversalCursor pickClusterRoot(Connection connection, int serverID, int dimension) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT id, disjoint_size FROM dbscan WHERE cluster_parent IS NULL AND disjoint_rank > 0 AND server_id = ? AND dimension = ? ORDER BY RANDOM() LIMIT 1")) {
            stmt.setInt(1, serverID);
            stmt.setInt(2, dimension);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new DisjointTraversalCursor(rs);
                } else {
                    return null;
                }
            }
        }
    }

    private DisjointTraversalCursor traverseRandomly(Connection connection, DisjointTraversalCursor src, Random rand) throws SQLException {
        int index = rand.nextInt(src.disjointSize);
        if (index == 0) {
            return src;
        }
        index--; // clamp to proper range
        return traverseRandomly(connection, src.fetchNthChild(connection, index), rand);
    }

    public ChunkPos clusterTraverse(int serverID, int dimension) {
        try (Connection connection = Database.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ); // :elmo:
            DisjointTraversalCursor root = pickClusterRoot(connection, serverID, dimension);
            if (root == null) {
                return null;
            }
            DisjointTraversalCursor destination = traverseRandomly(connection, root, new Random());
            try (PreparedStatement stmt = connection.prepareStatement("SELECT x, z FROM dbscan WHERE id = ?")) {
                stmt.setInt(1, destination.id);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    return new ChunkPos(rs.getInt("x"), rs.getInt("z"));
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
