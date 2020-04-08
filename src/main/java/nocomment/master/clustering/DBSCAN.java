package nocomment.master.clustering;

import nocomment.master.db.Database;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.LoggingExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public enum DBSCAN {
    INSTANCE;
    private static final int MIN_PTS = 200;

    public void beginIncrementalDBSCANThread() {
        // schedule with fixed delay is Very Important, so that we get no overlaps
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::incrementalRun), 0, 30, TimeUnit.SECONDS);
    }

    private synchronized void incrementalRun() {
        while (Aggregator.INSTANCE.aggregateHits()) ;
        try (Connection connection = Database.getConnection()) {
            try {
                dbscan(connection);
            } catch (SQLException ex) {
                connection.rollback();
                throw ex;
            } catch (Throwable th) {
                connection.rollback();
                th.printStackTrace();
                throw new RuntimeException(th);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public class Datapoint {
        int id;
        int x;
        int z;
        int dimension;
        int serverID;
        boolean isCore;
        OptionalInt clusterParent;
        int disjointRank;
        int disjointSize;

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
            this.disjointSize = rs.getInt("disjoint_size");
        }

        public boolean assignedEdge() {
            return !isCore && clusterParent.isPresent();
        }

        public Datapoint root(Connection connection, Map<Integer, Datapoint> knownEventualRoots) throws SQLException {
            if (!clusterParent.isPresent()) {
                knownEventualRoots.put(this.id, this);
                return this;
            }
            int directParentID = clusterParent.getAsInt();
            Datapoint root = knownEventualRoots.get(directParentID);
            if (root == null) {
                root = getByID(connection, directParentID).root(connection, knownEventualRoots);
            }
            if (root.id != directParentID) { // disjoint-set path compression runs even if we could cache the eventual root!
                clusterParent = OptionalInt.of(root.id);
                System.out.println("Updating my parent from " + directParentID + " to " + root.id);
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET cluster_parent = ? WHERE id = ?")) {
                    stmt.setInt(1, root.id);
                    stmt.setInt(2, id);
                    stmt.execute();
                }
                // my previous parent is therefore no longer as big as they used to be
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET disjoint_size = disjoint_size - ? WHERE id = ?")) {
                    stmt.setInt(1, disjointSize);
                    stmt.setInt(2, directParentID);
                    stmt.execute();
                }
                commit = true;
            }
            knownEventualRoots.put(this.id, root);
            return root;
        }

        public int directClusterParent() {
            return clusterParent.orElse(id);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Datapoint && ((Datapoint) o).id == id;
        }

        @Override
        public int hashCode() {
            return id; // yolo
        }

        @Override
        public String toString() {
            return "{x=" + x + " z=" + z + " server_id=" + serverID + " dimension=" + dimension + " size=" + disjointSize + " rank=" + disjointRank + " iscore=" + isCore + " isroot=" + !clusterParent.isPresent() + "}";
        }
    }

    private static final String DANK_CONDITION = "cnt > 3 AND server_id = ? AND dimension = ? AND CIRCLE(POINT(x, z), 32) @> CIRCLE(POINT(?, ?), 0)";
    private static final String COLS = "id, x, z, dimension, server_id, is_core, cluster_parent, disjoint_rank, disjoint_size";

    private Datapoint getByID(Connection connection, int id) throws SQLException {
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
                "    SELECT id FROM dbscan WHERE needs_update ORDER BY is_core ASC LIMIT 1 FOR UPDATE SKIP LOCKED" +
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

    private Datapoint merge(Datapoint xRoot, Datapoint yRoot, Connection connection) throws SQLException {
        if (xRoot.disjointRank < yRoot.disjointRank || (xRoot.disjointRank == yRoot.disjointRank && yRoot.isCore && !xRoot.isCore)) {
            return merge(yRoot, xRoot, connection); // intentionally swap
        }
        // merge based on disjoint rank, but maintain disjoint size as well
        System.out.println("Merging cluster " + yRoot + " into " + xRoot);
        // yRoot.disjointRank <= xRoot.disjointRank
        // so, merge yRoot into a new child of xRoot
        yRoot.clusterParent = OptionalInt.of(xRoot.id);
        try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET cluster_parent = ? WHERE id = ?")) {
            stmt.setInt(1, xRoot.id);
            stmt.setInt(2, yRoot.id);
            stmt.execute();
        }
        if (xRoot.disjointRank == yRoot.disjointRank) {
            xRoot.disjointRank++;
        }
        xRoot.disjointSize += yRoot.disjointSize;
        try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET disjoint_rank = ?, disjoint_size = ? WHERE id = ?")) {
            stmt.setInt(1, xRoot.disjointRank);
            stmt.setInt(2, xRoot.disjointSize);
            stmt.setInt(3, xRoot.id);
            stmt.execute();
        }
        return xRoot;
    }

    private boolean commit;

    private void dbscan(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        int i = 0;
        while (true) {
            long c = System.currentTimeMillis();
            Datapoint point = getDatapoint(connection);
            long d = System.currentTimeMillis();
            if (d - c > 5)
                System.out.println("Took " + (d - c) + "ms to get a point");
            if (point == null) {
                break;
            }
            long a = System.currentTimeMillis();
            List<Datapoint> neighbors = getWithinRange(point, connection); // IMPORTANT: THIS INCLUDES THE POINT ITSELF!
            long b = System.currentTimeMillis();
            if (b - a > 30)
                System.out.println("Took " + (b - a) + "ms to get " + neighbors.size() + " neighbors");
            if (!neighbors.contains(point)) {
                throw new IllegalStateException();
            }
            long e = System.currentTimeMillis();
            commit = i++ % 100 == 0;
            if (neighbors.size() > MIN_PTS && !point.isCore) {
                System.out.println("DBSCAN promoting " + point + " to core point");
                point.isCore = true;
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET is_core = TRUE WHERE id = ?")) {
                    stmt.setInt(1, point.id);
                    stmt.execute();
                }
                commit = true;
                //markForUpdateAllWithinRadius(point.serverID, point.dimension, point.x, point.z, connection);
                // TODO I REALLY thought that this ^ would be necessary, but it isn't. WHY?
            }

            if (point.isCore) {
                // initial really fast sanity check
                Set<Integer> chkParents = new HashSet<>();
                for (Datapoint neighbor : neighbors) {
                    if (!neighbor.assignedEdge()) {
                        chkParents.add(neighbor.directClusterParent());
                    }
                }
                // if all direct parents are the same, then we're already all merged and happy
                if (chkParents.size() > 1) { // otherwise we actually need to figure this shit out!
                    Set<Datapoint> clustersToMerge = new HashSet<>(); // dedupe on id
                    Map<Integer, Datapoint> knownEventualRoots = new HashMap<>();
                    for (Datapoint neighbor : neighbors) {
                        if (!neighbor.assignedEdge()) {
                            clustersToMerge.add(neighbor.root(connection, knownEventualRoots));
                        }
                    }
                    Datapoint merging = point.root(connection, knownEventualRoots);
                    System.out.println("Clusters to merge: " + clustersToMerge);
                    if (!clustersToMerge.remove(merging)) {
                        throw new IllegalStateException();
                    }
                    for (Datapoint remain : clustersToMerge) {
                        merging = merge(merging, remain, connection);
                    }
                    commit = true;
                }
            }
            long f = System.currentTimeMillis();
            if (f - e > 5)
                System.out.println("Took " + (f - e) + "ms to do all the other shit");
            if (commit) {
                connection.commit();
            }
        }
        connection.commit();
        System.out.println("DBSCAN merger committing");
    }
}
