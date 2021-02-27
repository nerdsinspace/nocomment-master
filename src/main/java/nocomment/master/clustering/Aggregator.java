package nocomment.master.clustering;

import io.prometheus.client.Gauge;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import nocomment.master.db.Database;
import nocomment.master.util.ChunkPos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

enum Aggregator {
    INSTANCE;
    private static final Gauge aggregatorLag = Gauge.build()
            .name("aggregator_lag")
            .help("Hits not yet aggregated")
            .register();
    private static final long DBSCAN_TIMESTAMP_INTERVAL = TimeUnit.HOURS.toMillis(1);
    private static final long MAX_GAP = TimeUnit.MINUTES.toMillis(2);
    private static final long MIN_DURATION_FOR_IGNORE = DBSCAN.MIN_OCCUPANCY_DURATION;
    private static final long MIN_DURATION_FOR_NODE = TimeUnit.MINUTES.toMillis(5);
    private static final int LIMIT_SZ = 1000;

    private final Int2LongOpenHashMap parentAgeCache = new Int2LongOpenHashMap();

    private static class PastHit {
        long id;
        long created_at;
        short serverID;
        short dimension;
        int x;
        int z;

        @Override
        public String toString() {
            return "{" + id + " " + created_at + " " + x + "," + x + " " + serverID + " " + dimension + "}";
        }
    }

    public boolean aggregateEligible(long cpos) {
        final int x = ChunkPos.decodeX(cpos);
        final int z = ChunkPos.decodeZ(cpos);
        return Math.abs(x) > 100 && Math.abs(z) > 100 && Math.abs(Math.abs(x) - Math.abs(z)) > 100 && ChunkPos.distSqSerialized(cpos) > 1500L * 1500L;
    }

    private static List<PastHit> query(long startID, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("" +
                " SELECT                                                                           " +
                "     id, server_id, dimension, x, z, created_at                                   " +
                " FROM                                                                             " +
                "     hits                                                                         " +
                " WHERE                                                                            " +
                "     id > ?                                                                       " +
                "     AND                                                                          " +
                "     (                                                                            " +
                "         (                                                                        " +
                "             dimension = 0 AND                                                    " +
                "             (                                                                    " +
                "                 ABS(x) > 100                                                     " +
                "                 AND ABS(z) > 100                                                 " +
                "                 AND ABS(ABS(x) - ABS(z)) > 100                                   " +
                "                 AND x::BIGINT * x::BIGINT + z::BIGINT * z::BIGINT > 1500 * 1500  " +
                "             )                                                                    " +
                "         )                                                                        " +
                "         OR                                                                       " +
                "         (                                                                        " +
                "             dimension = 1 AND                                                    " +
                "             (                                                                    " +
                "                 ABS(x) > 50                                                      " +
                "                 AND ABS(z) > 50                                                  " +
                "                 AND ABS(ABS(x) - ABS(z)) > 50                                    " +
                "                 AND x::BIGINT * x::BIGINT + z::BIGINT * z::BIGINT > 500 * 500    " +
                "             )                                                                    " +
                "         )                                                                        " +
                "     )                                                                            " +
                " ORDER BY id                                                                      " +
                " LIMIT ?                                                                          "
        )) {
            stmt.setLong(1, startID);
            stmt.setInt(2, LIMIT_SZ);
            try (ResultSet rs = stmt.executeQuery()) {
                List<PastHit> ret = new ArrayList<>();
                while (rs.next()) {
                    PastHit hit = new PastHit();
                    hit.serverID = rs.getShort("server_id");
                    hit.dimension = rs.getShort("dimension");
                    hit.x = rs.getInt("x");
                    hit.z = rs.getInt("z");
                    hit.id = rs.getLong("id");
                    hit.created_at = rs.getLong("created_at");
                    ret.add(hit);
                }
                return ret;
            }
        }
    }

    public synchronized void multiAggregate() {
        Set<String> marked = new HashSet<>();
        long start = System.currentTimeMillis();
        long num = 0;
        while (aggregateHits(marked)) {
            num += LIMIT_SZ;
            System.out.println("MS per was approx " + (double) (System.currentTimeMillis() - start) / (double) num);
        }
    }

    private boolean aggregateHits(Set<String> alreadyMarked) {
        System.out.println("DBSCAN aggregator triggered");
        try (Connection connection = Database.getConnection()) {
            connection.setAutoCommit(false);
            long lastProcessedHitID = 0;
            try (PreparedStatement stmt = connection.prepareStatement("SELECT last_processed_hit_id FROM dbscan_progress");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    lastProcessedHitID = rs.getLong("last_processed_hit_id");
                }
            }
            long lastRealHitID;
            try (PreparedStatement stmt = connection.prepareStatement("SELECT MAX(id) AS max_id FROM hits");
                 ResultSet rs = stmt.executeQuery()) {
                rs.next();
                lastRealHitID = rs.getLong("max_id");
            }
            long lag = lastRealHitID - lastProcessedHitID;
            aggregatorLag.set(lag);
            if (lag < 10000) {
                System.out.println("DBSCAN aggregator not running, only " + lag + " hits behind real time");
                return false;
            }
            System.out.println("DBSCAN aggregator running " + lag + " hits behind real time");
            long maxHitIDProcessed = 0;
            List<PastHit> past = query(lastProcessedHitID, connection);
            if (past.isEmpty()) {
                return false;
            }
            for (PastHit hit : past) {
                // the common case is a brand new hit, so, counterintuitively, do that BEFORE the select
                maxHitIDProcessed = Math.max(maxHitIDProcessed, hit.id);
                try (PreparedStatement stmt = connection.prepareStatement("" +
                        // 1 count will NEVER pass filter layer 2 and graduate to is_node
                        "INSERT INTO dbscan (server_id, dimension, x, z, is_node, is_core, cluster_parent, disjoint_rank, disjoint_size, first_init_hit, last_init_hit, ts_ranges) VALUES" +
                        "                   (?,         ?,         ?, ?, FALSE,   FALSE,   NULL,           0,             1,             ?,              ?,             ARRAY[]::INT8RANGE[]) ON CONFLICT (server_id, dimension, x, z) DO NOTHING RETURNING first_init_hit")) {
                    stmt.setShort(1, hit.serverID);
                    stmt.setShort(2, hit.dimension);
                    stmt.setInt(3, hit.x);
                    stmt.setInt(4, hit.z);
                    stmt.setLong(5, hit.created_at);
                    stmt.setLong(6, hit.created_at);
                    try (ResultSet rs = stmt.executeQuery()) {
                        // we get 0 rows if duplicate, and 1 row if brand new
                        if (rs.next()) {
                            // it's brand new (no conflict)
                            continue;
                        }
                    }
                }

                int dbID;
                long lastInitHit;
                long occupancyDuration;
                OptionalLong lastRangeEnd = OptionalLong.empty();
                boolean isNode;
                OptionalInt clusterParent = OptionalInt.empty();
                try (PreparedStatement stmt = connection.prepareStatement("SELECT is_node, id, cluster_parent, last_init_hit, _range_union_cardinality(ts_ranges) AS occupancy_duration, UPPER(ts_ranges[ARRAY_UPPER(ts_ranges, 1)]) AS last_range FROM dbscan WHERE server_id = ? AND dimension = ? AND x = ? AND z = ?")) {
                    stmt.setShort(1, hit.serverID);
                    stmt.setShort(2, hit.dimension);
                    stmt.setInt(3, hit.x);
                    stmt.setInt(4, hit.z);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new IllegalStateException("The insert failed due to unique, but the select also has no rows?!??! " + hit);
                        }
                        isNode = rs.getBoolean("is_node");
                        dbID = rs.getInt("id");
                        int parent = rs.getInt("cluster_parent");
                        if (!rs.wasNull()) {
                            clusterParent = OptionalInt.of(parent);
                        }
                        lastInitHit = rs.getLong("last_init_hit");
                        occupancyDuration = rs.getLong("occupancy_duration");
                        if (rs.wasNull()) {
                            occupancyDuration = 0;
                        }
                        long lastRange = rs.getLong("last_range");
                        if (!rs.wasNull()) {
                            lastRangeEnd = OptionalLong.of(lastRange);
                        }
                    }
                }
                if (clusterParent.isPresent()) {
                    long committedUpdate = parentAgeCache.getOrDefault(clusterParent.getAsInt(), 0L);
                    long now = hit.created_at;
                    if (committedUpdate < now - DBSCAN_TIMESTAMP_INTERVAL) {
                        try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET root_updated_at = ? WHERE id = ?")) {
                            stmt.setLong(1, now);
                            stmt.setInt(2, clusterParent.getAsInt());
                            stmt.executeUpdate();
                        }
                        parentAgeCache.put(clusterParent.getAsInt(), now);
                    }
                }

                // we need to update duration information, and count information
                boolean guaranteedCore = occupancyDuration > MIN_DURATION_FOR_IGNORE;
                if (guaranteedCore) {
                    if (!isNode) {
                        throw new IllegalStateException("Impossible " + hit);
                    }
                    // nothing to do
                    continue;
                }
                long gap = hit.created_at - lastInitHit;
                if (gap <= 0) {
                    continue;
                }
                if (gap > MAX_GAP) {
                    // we don't want to do anything except for update last_init_hit
                    // this doesn't change ts_ranges, so it can't change is_node
                    try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET last_init_hit = ? WHERE id = ?")) {
                        stmt.setLong(1, hit.created_at);
                        stmt.setInt(2, dbID);
                        stmt.executeUpdate();
                    }
                    continue;
                }
                long newOccupancy = occupancyDuration + gap;
                boolean newNode = isNode || newOccupancy > MIN_DURATION_FOR_NODE;
                // okay, time to add this time range (from lastInitHit to hit.created_at) to ts_ranges!
                // two possible ways:
                if (lastRangeEnd.isPresent() && lastRangeEnd.getAsLong() == lastInitHit) { // first way: increase the upper of the last array element
                    // if lastRangeEnd is present, that means that ts_ranges already has entries
                    // it also means that the last range end is actually our most recent hit, so we want to extend that by gap, to hit.created_at
                    try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET last_init_hit = ?, is_node = ?, ts_ranges = ARRAY_APPEND((SELECT ARRAY(SELECT UNNEST(ts_ranges) LIMIT (SELECT ARRAY_UPPER(ts_ranges, 1) - 1))), INT8RANGE(LOWER(ts_ranges[ARRAY_UPPER(ts_ranges, 1)]), ?)) WHERE id = ?")) {
                        stmt.setLong(1, hit.created_at);
                        stmt.setBoolean(2, newNode);
                        stmt.setLong(3, hit.created_at);
                        stmt.setInt(4, dbID);
                        stmt.executeUpdate();
                    }
                } else { // otherwise, we need to add a new element, from lastInitHit to hit.created_at
                    try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET last_init_hit = ?, is_node = ?, ts_ranges = ARRAY_APPEND(ts_ranges, INT8RANGE(?, ?)) WHERE id = ?")) {
                        stmt.setLong(1, hit.created_at);
                        stmt.setBoolean(2, newNode);
                        stmt.setLong(3, lastInitHit);
                        stmt.setLong(4, hit.created_at);
                        stmt.setInt(5, dbID);
                        stmt.executeUpdate();
                    }
                }
                if (newNode) {
                    // if anything changed AND we are currently a node, queue a ranged update in an hour
                    // if we were previously a node (so, no count change) and duration didn't change (i.e. we just updated last_init_hit), then we don't though
                    String dedupeKey = hit.serverID + " " + hit.dimension + " " + hit.x + " " + hit.z;
                    if (alreadyMarked.add(dedupeKey)) {
                        // within a single run of aggregator, there was no consumer of dbscan_to_update, by contract
                        // therefore this only needs to run once each
                        DBSCAN.markForUpdateAllWithinRadius(hit.serverID, hit.dimension, hit.x, hit.z, connection);
                    }
                }
            }
            try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan_progress SET last_processed_hit_id = ?")) {
                stmt.setLong(1, maxHitIDProcessed);
                stmt.execute();
            }
            System.out.println("DBSCAN aggregator committing");
            connection.commit();
            Database.incrementCommitCounter("aggregator");
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
