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
            dbscan(connection);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public class Datapoint {
        int id;
        int x;
        int z;
        short dimension;
        short serverID;
        boolean isCore;
        OptionalInt clusterParent;
        int disjointRank;
        int disjointSize;

        private Datapoint(ResultSet rs) throws SQLException {
            this.id = rs.getInt("id");
            this.x = rs.getInt("x");
            this.z = rs.getInt("z");
            this.dimension = rs.getShort("dimension");
            this.serverID = rs.getShort("server_id");
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

        public Datapoint root(Connection connection, Map<Integer, Datapoint> cache) throws SQLException {
            if (!clusterParent.isPresent()) {
                return this;
            }
            int directParentID = clusterParent.getAsInt();
            Datapoint directParent = getByID(connection, directParentID, cache);
            Datapoint root = directParent.root(connection, cache);
            if (root.id != directParentID) { // disjoint-set path compression runs even if we could cache the eventual root!
                clusterParent = OptionalInt.of(root.id);
                System.out.println("Updating my parent from " + directParentID + " to " + root.id);
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET cluster_parent = ? WHERE id = ?")) {
                    stmt.setInt(1, root.id);
                    stmt.setInt(2, id);
                    stmt.execute();
                }
                // my previous parent is therefore no longer as big as they used to be
                directParent.disjointSize -= this.disjointSize;
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET disjoint_size = ? WHERE id = ?")) {
                    stmt.setInt(1, directParent.disjointSize);
                    stmt.setInt(2, directParentID);
                    stmt.execute();
                }
                commit = true;
            }
            return root;
        }

        public Datapoint fetchRootReadOnly(Connection connection, Map<Integer, Datapoint> cache) throws SQLException { // fetch root without performing disjoint-set path compression
            if (!clusterParent.isPresent()) {
                return this;
            }
            return getByID(connection, clusterParent.getAsInt(), cache).fetchRootReadOnly(connection, cache);
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

    private Datapoint getByID(Connection connection, int id, Map<Integer, Datapoint> cache) throws SQLException {
        if (cache.containsKey(id)) {
            return cache.get(id);
        }
        try (PreparedStatement stmt = connection.prepareStatement("SELECT " + COLS + " FROM dbscan WHERE id = ?")) {
            stmt.setInt(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                Datapoint ret = new Datapoint(rs);
                cache.put(id, ret);
                return ret;
            }
        }
    }

    public static void markForUpdateAllWithinRadius(short serverID, short dimension, int x, int z, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO dbscan_to_update (dbscan_id) SELECT id FROM dbscan WHERE " + DANK_CONDITION + " ON CONFLICT DO NOTHING")) {
            stmt.setShort(1, serverID);
            stmt.setShort(2, dimension);
            stmt.setInt(3, x);
            stmt.setInt(4, z);
            stmt.execute();
        }
    }

    private Datapoint getDatapoint(Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("WITH removed_id AS (DELETE FROM dbscan_to_update WHERE dbscan_id = (SELECT MAX(dbscan_id) FROM dbscan_to_update) RETURNING dbscan_id) SELECT " + COLS + " FROM dbscan WHERE id = (SELECT dbscan_id FROM removed_id)");
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return new Datapoint(rs);
            } else {
                return null;
            }
        }
    }

    private List<Datapoint> getWithinRange(Datapoint source, Connection connection) throws SQLException {
        return getWithinRange(source.serverID, source.dimension, source.x, source.z, connection);
    }

    private List<Datapoint> getWithinRange(short serverID, short dimension, int x, int z, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT " + COLS + " FROM dbscan WHERE " + DANK_CONDITION)) {
            stmt.setShort(1, serverID);
            stmt.setShort(2, dimension);
            stmt.setInt(3, x);
            stmt.setInt(4, z);
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
        if (xRoot.disjointRank < yRoot.disjointRank || (xRoot.disjointRank == yRoot.disjointRank && ((!xRoot.isCore && yRoot.isCore) || (xRoot.isCore == yRoot.isCore && (xRoot.disjointSize < yRoot.disjointSize))))) {
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
                System.out.println("Took " + (b - a) + "ms to get " + neighbors.size() + " neighbors of " + point);
            if (!neighbors.contains(point)) {
                throw new IllegalStateException();
            }
            long e = System.currentTimeMillis();
            commit = i++ % 20 == 0;
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
                    Map<Integer, Datapoint> cache = new HashMap<>();
                    neighbors.remove(point);
                    neighbors.add(point); // fix duplicate entry (two different Datapoint objects for the same id)
                    // neighbors will all have distinct IDs
                    for (Datapoint neighbor : neighbors) {
                        cache.put(neighbor.id, neighbor);
                    }
                    for (Datapoint neighbor : neighbors) {
                        if (!neighbor.assignedEdge()) {
                            clustersToMerge.add(neighbor.root(connection, cache));
                        }
                    }
                    Datapoint merging = point.root(connection, cache); // point is guaranteed to be in neighbors now
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

    public Optional<Datapoint> fetch(short serverID, short dimension, int x, int z, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT " + COLS + " FROM dbscan WHERE server_id = ? AND dimension = ? AND x = ? AND z = ? AND (is_core OR cluster_parent IS NOT NULL)")) {
            stmt.setShort(1, serverID);
            stmt.setShort(2, dimension);
            stmt.setInt(3, x);
            stmt.setInt(4, z);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new Datapoint(rs));
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    public OptionalInt broadlyFetchAReasonablyCloseClusterIDFor(short serverID, short dimension, int x, int z, Connection connection) throws SQLException {
        // grab it directly. this is pretty plausible since interesting track endings will be disproportionately clustered
        Map<Integer, Datapoint> cache = new HashMap<>();
        Optional<Datapoint> directMember = fetch(serverID, dimension, x, z, connection);
        if (directMember.isPresent()) {
            return OptionalInt.of(directMember.get().fetchRootReadOnly(connection, cache).id);
        }
        List<Datapoint> neighbors = getWithinRange(serverID, dimension, x, z, connection);
        neighbors.removeIf(dp -> !dp.isCore && !dp.clusterParent.isPresent()); // remove non core points with no parent
        if (neighbors.isEmpty()) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(neighbors.stream().min(Comparator.comparingDouble(dp -> Math.sqrt((dp.x - x) * (dp.x - x) + (dp.z - z) * (dp.z - z)) * 1.0d / dp.disjointSize)).get().fetchRootReadOnly(connection, cache).id);
    }
}
