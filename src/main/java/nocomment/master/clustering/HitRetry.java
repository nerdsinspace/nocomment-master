package nocomment.master.clustering;

import nocomment.master.db.Database;
import nocomment.master.util.ChunkPos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

public enum HitRetry {
    INSTANCE;
    private static final int QUEUE_SIZE = 100;
    // queues since the DBSCAN synchronized lock can be held for multiple minutes at a time, and is held for multiple seconds every 10 seconds
    private final Map<ServerAndDimension, LinkedBlockingQueue<ChunkPos>> hitRetryQueues = new HashMap<>();

    static { // intentional: only start the cluster traversal thread when we're asked to!
        new Thread(INSTANCE::clusterTraversalThread).start();
    }

    private void clusterTraversalThread() {
        // we cannot use the scheduler since we need control over the delay between runs
        try {
            while (true) {
                Optional<ServerAndDimension> sdOpt = mostThirsty();
                if (!sdOpt.isPresent()) {
                    Thread.sleep(1000);
                    continue;
                }
                ServerAndDimension sd = sdOpt.get();
                LinkedBlockingQueue<ChunkPos> queue = getQueueFor(sd);
                ChunkPos pos = clusterTraverse(sd);
                if (pos == null) {
                    // they have nothing
                    synchronized (hitRetryQueues) {
                        System.out.println("Removing overambitious early hit retry queue!");
                        hitRetryQueues.remove(sd);
                    }
                    continue;
                }
                queue.add(pos);
            }
        } catch (Throwable th) {
            th.printStackTrace();
        }
    }

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

    private DisjointTraversalCursor pickClusterRoot(Connection connection, ServerAndDimension sd) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT id, disjoint_size FROM dbscan WHERE cluster_parent IS NULL AND disjoint_rank > 0 AND server_id = ? AND dimension = ? ORDER BY RANDOM() LIMIT 1")) {
            stmt.setInt(1, sd.serverID);
            stmt.setInt(2, sd.dimension);
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

    private ChunkPos clusterTraverse(ServerAndDimension sd) {
        synchronized (DBSCAN.DBSCAN_TRAVERSAL_LOCK) {
            try (Connection connection = Database.getConnection()) {
                DisjointTraversalCursor root = pickClusterRoot(connection, sd);
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

    public LinkedBlockingQueue<ChunkPos> getQueueFor(ServerAndDimension sd) {
        synchronized (hitRetryQueues) {
            return hitRetryQueues.computeIfAbsent(sd, x -> new LinkedBlockingQueue<>());
        }
    }

    private Optional<ServerAndDimension> mostThirsty() {
        synchronized (hitRetryQueues) {
            if (hitRetryQueues.isEmpty()) {
                return Optional.empty();
            }
            ServerAndDimension smallest = hitRetryQueues.keySet().iterator().next();
            for (ServerAndDimension sd : hitRetryQueues.keySet()) {
                if (hitRetryQueues.get(sd).size() < hitRetryQueues.get(smallest).size()) {
                    smallest = sd;
                }
            }
            if (hitRetryQueues.get(smallest).size() < QUEUE_SIZE) {
                return Optional.of(smallest);
            }
            return Optional.empty();
        }
    }

    public static class ServerAndDimension {
        private final int serverID;
        private final int dimension;

        public ServerAndDimension(int serverID, int dimension) {
            this.serverID = serverID;
            this.dimension = dimension;
        }

        @Override
        public int hashCode() {
            return serverID * 420 + dimension * 69;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ServerAndDimension && ((ServerAndDimension) o).serverID == serverID && ((ServerAndDimension) o).dimension == dimension;
        }
    }
}
