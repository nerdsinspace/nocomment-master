package nocomment.master.clustering;

import io.prometheus.client.Gauge;
import nocomment.master.db.Database;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class HitRetry {

    private static final Gauge roots = Gauge.build()
            .name("dbscan_roots_by_updated_at")
            .help("Number of dbscan roots")
            .labelNames("server_id", "dimension", "generation")
            .register();

    private static final Random RANDOM = new Random();
    private static final Map<String, HitRetry> instances = new HashMap<>();

    private static synchronized HitRetry getInstance(short serverID, short dimension) {
        return instances.computeIfAbsent(serverID + " " + dimension, $ -> new HitRetry(serverID, dimension));
    }

    private final short serverID;
    private final short dimension;
    private List<CachedDBSCANRoot> clusterRootCache = null;

    private HitRetry(short serverID, short dimension) {
        this.serverID = serverID;
        this.dimension = dimension;
        updateClusterRootCache(); // the first run MUST be done blockingly
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::updateClusterRootCache), 1, 1, TimeUnit.HOURS);
    }

    private void updateClusterRootCache() {
        long start = System.currentTimeMillis();
        try (Connection connection = Database.getConnection(); PreparedStatement stmt = connection.prepareStatement("SELECT id, disjoint_rank, root_updated_at FROM dbscan WHERE cluster_parent IS NULL AND disjoint_rank > 0 AND server_id = ? AND dimension = ? ORDER BY root_updated_at")) {
            stmt.setShort(1, serverID);
            stmt.setShort(2, dimension);
            try (ResultSet rs = stmt.executeQuery()) {
                List<CachedDBSCANRoot> ret = new ArrayList<>();
                while (rs.next()) {
                    ret.add(new CachedDBSCANRoot(rs));
                }
                saveCount(ret, 0, "all");
                saveCount(ret, weekThreshold(), "week");
                saveCount(ret, monthThreshold(), "month");
                clusterRootCache = ret;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
        System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to update cluster root cache");
    }

    private static class CachedDBSCANRoot {

        private final int id;
        private final int disjointRank;
        private final long rootUpdatedAt;

        private CachedDBSCANRoot(ResultSet rs) throws SQLException {
            this.id = rs.getInt("id");
            this.disjointRank = rs.getInt("disjoint_rank");
            this.rootUpdatedAt = rs.getLong("root_updated_at");
        }
    }

    private static Optional<CachedDBSCANRoot> sampleRootNode(List<CachedDBSCANRoot> cache) {
        OptionalInt indexMin = binarySearch(cache, sampleAgeThreshold());
        if (!indexMin.isPresent()) {
            return Optional.empty();
        }
        int sampledIndex = indexMin.getAsInt() + RANDOM.nextInt(cache.size() - indexMin.getAsInt());
        return Optional.of(cache.get(sampledIndex));
    }

    private void saveCount(List<CachedDBSCANRoot> cache, long timestamp, String generation) {
        roots.labels(serverID + "", dimension + "", generation).set(countNewerThan(cache, timestamp));
    }

    private static int countNewerThan(List<CachedDBSCANRoot> cache, long timestamp) {
        return cache.size() - binarySearch(cache, timestamp).orElse(cache.size());
    }

    private static long sampleAgeThreshold() {
        if (RANDOM.nextBoolean()) {
            return 0;
        }
        if (RANDOM.nextBoolean()) {
            return monthThreshold();
        }
        return weekThreshold();
    }

    private static long weekThreshold() {
        return System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
    }

    private static long monthThreshold() {
        return System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
    }

    private static OptionalInt binarySearch(List<CachedDBSCANRoot> cache, long mustBeGreaterThan) {
        int lo = 0;
        int hi = cache.size() - 1;
        if (hi < lo) {
            return OptionalInt.empty();
        }
        while (lo < hi) {
            int mid = (hi + lo) / 2;
            if (mustBeGreaterThan < cache.get(mid).rootUpdatedAt) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if (mustBeGreaterThan < cache.get(hi).rootUpdatedAt) {
            if (hi > 0) {
                long prev = cache.get(hi - 1).rootUpdatedAt;
                if (prev > mustBeGreaterThan) {
                    throw new IllegalStateException();
                }
            }
            return OptionalInt.of(hi);
        } else {
            if (hi != cache.size() - 1) {
                throw new IllegalStateException();
            }
            return OptionalInt.empty();
        }
    }

    public static Optional<ChunkPos> clusterTraverse(short serverID, short dimension) {
        try (Connection connection = Database.getConnection(); PreparedStatement stmt = connection.prepareStatement("" +
                "            WITH RECURSIVE initial AS (                                      " +
                "                SELECT                                                       " +
                "                    ? AS id,                                                 " +
                "                    ? AS disjoint_rank                                       " +
                "            ),                                                               " +
                "            clusters AS (                                                    " +
                "                SELECT                                                       " +
                "                    id,                                                      " +
                "                    disjoint_rank                                            " +
                "                FROM                                                         " +
                "                    initial                                                  " +
                "                UNION                                                        " +
                "                    SELECT                                                   " +
                "                        dbscan.id,                                           " +
                "                        dbscan.disjoint_rank                                 " +
                "                    FROM                                                     " +
                "                        dbscan                                               " +
                "                    INNER JOIN                                               " +
                "                        clusters                                             " +
                "                            ON dbscan.cluster_parent = clusters.id           " +
                "                    WHERE                                                    " +
                "                        clusters.disjoint_rank > 0                           " +
                "            ), choice AS (                                                   " +
                "                SELECT                                                       " +
                "                    id                                                       " +
                "                FROM                                                         " +
                "                    clusters                                                 " +
                "                ORDER BY RANDOM()                                            " +
                "                LIMIT 1                                                      " +
                "            )                                                                " +
                "            SELECT                                                           " +
                "                x,                                                           " +
                "                z                                                            " +
                "            FROM                                                             " +
                "                dbscan                                                       " +
                "            INNER JOIN choice                                                " +
                "                ON choice.id = dbscan.id                                     ")) {
            Optional<CachedDBSCANRoot> root = sampleRootNode(getInstance(serverID, dimension).clusterRootCache);
            if (!root.isPresent()) {
                return Optional.empty();
            }
            stmt.setInt(1, root.get().id);
            stmt.setInt(2, root.get().disjointRank);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ChunkPos(rs.getInt("x"), rs.getInt("z")));
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
