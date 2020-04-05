package nocomment.master.clustering;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DBSCAN {
    private static final int MIN_PTS = 69;

    public static class Datapoint {
        int id;
        int x;
        int z;
        int dimension;
        int serverID;
        boolean isCore;
        OptionalInt clusterParent;
        int disjointRank;

        private Datapoint(ResultSet rs) throws SQLException {
            this.id = rs.getInt("id");
            this.x = rs.getInt("x");
            this.z = rs.getInt("z");
            this.dimension = rs.getInt("dimension");
            this.serverID = rs.getInt("server_id");
            this.isCore = rs.getBoolean("is_core");
            int parent = rs.getInt("cluster_parent");
            if (rs.wasNull()) {
                this.clusterParent = OptionalInt.empty();
            } else {
                this.clusterParent = OptionalInt.of(parent);
            }
            this.disjointRank = rs.getInt("disjoint_rank");
        }

        public boolean assignedEdge() {
            return !isCore && clusterParent.isPresent();
        }

        public Datapoint root(Connection connection) throws SQLException {
            if (!clusterParent.isPresent()) {
                return this;
            }
            Datapoint directParent = getByID(connection, clusterParent.getAsInt());
            Datapoint root = directParent.root(connection);
            if (root.id != directParent.id) { // disjoint-set path compression
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET cluster_parent = ? WHERE id = ?")) {
                    stmt.setInt(1, root.id);
                    stmt.setInt(2, id);
                    stmt.execute();
                }
                clusterParent = OptionalInt.of(root.id);
            }
            return root;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Datapoint && ((Datapoint) o).id == id;
        }

        @Override
        public int hashCode() {
            return id; // yolo
        }
    }

    private static final String DANK_CONDITION = "cnt > 3 AND server_id = ? AND dimension = ? AND CIRCLE(POINT(x, z), 32) @> CIRCLE(POINT(?, ?), 0)";
    private static final String COLS = "id, x, z, dimension, server_id, is_core, cluster_parent, disjoint_rank";

    private static Datapoint getByID(Connection connection, int id) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT " + COLS + " FROM dbscan WHERE id = ?")) {
            stmt.setInt(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return new Datapoint(rs);
            }
        }
    }

    public static void markForUpdateAllWithinRadius(int serverID, int dimension, int x, int z, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET needs_update = TRUE WHERE " + DANK_CONDITION)) {
            stmt.setInt(1, serverID);
            stmt.setInt(2, dimension);
            stmt.setInt(3, x);
            stmt.setInt(4, z);
            stmt.execute();
        }
    }

    private Datapoint getDatapoint(Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("" +
                "UPDATE dbscan SET needs_update = FALSE WHERE id = (" +
                "    SELECT id FROM dbscan WHERE needs_update ORDER BY is_core DESC LIMIT 1 FOR UPDATE SKIP LOCKED" +
                ") RETURNING " + COLS);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return new Datapoint(rs);
            } else {
                return null;
            }
        }
    }

    private List<Datapoint> getWithinRange(Datapoint source, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT " + COLS + " FROM dbscan WHERE " + DANK_CONDITION)) {
            stmt.setInt(1, source.serverID);
            stmt.setInt(2, source.dimension);
            stmt.setInt(3, source.x);
            stmt.setInt(4, source.z);
            try (ResultSet rs = stmt.executeQuery()) {
                List<Datapoint> ret = new ArrayList<>();
                while (rs.next()) {
                    ret.add(new Datapoint(rs));
                }
                return ret;
            }
        }
    }

    private void merge(Datapoint xRoot, Datapoint yRoot, Connection connection) throws SQLException {
        if (xRoot.disjointRank < yRoot.disjointRank) {
            merge(yRoot, xRoot, connection); // intentionally swap
            return;
        }
        // yRoot.disjointRank < xRoot.disjointRank
        // so, merge yRoot into a new child of xRoot
        try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET cluster_parent = ? WHERE id = ?")) {
            stmt.setInt(1, xRoot.id);
            stmt.setInt(2, yRoot.id);
            stmt.execute();
        }
        if (xRoot.disjointRank == yRoot.disjointRank) {
            try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET disjoint_rank = ? WHERE id = ?")) {
                stmt.setInt(1, xRoot.disjointRank + 1);
                stmt.setInt(2, xRoot.id);
                stmt.execute();
            }
        }
    }


    public void dbscan(Connection connection) throws SQLException {
        while (true) {
            Datapoint point = getDatapoint(connection);
            if (point == null) {
                break;
            }
            List<Datapoint> neighbors = getWithinRange(point, connection); // IMPORTANT: THIS INCLUDES THE POINT ITSELF!
            if (neighbors.size() > MIN_PTS && !point.isCore) {
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET is_core = TRUE WHERE id = ?")) {
                    stmt.setInt(1, point.id);
                    stmt.execute();
                }
                point.isCore = true;
                markForUpdateAllWithinRadius(point.serverID, point.dimension, point.x, point.z, connection);
            }

            if (point.isCore) {
                Set<Datapoint> clustersToMerge = new HashSet<>();
                for (Datapoint d : neighbors) {
                    if (!d.assignedEdge()) {
                        clustersToMerge.add(d.root(connection));
                    }
                }
                if (clustersToMerge.size() > 1) {
                    Datapoint arbitrary = clustersToMerge.iterator().next();
                    clustersToMerge.remove(arbitrary);
                    for (Datapoint remain : clustersToMerge) {
                        merge(arbitrary, remain, connection);
                    }
                }
            }
        }
    }
}
