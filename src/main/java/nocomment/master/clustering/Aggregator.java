package nocomment.master.clustering;

import nocomment.master.db.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Aggregator {
    private static final int THRESHOLD = 3;

    private static class AggregatedHits {
        int serverID;
        int dimension;
        int x;
        int z;
        int count;
        boolean anyLegacy;
        long maxID;
    }

    private List<AggregatedHits> queryAggregates(long startID, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("" +
                "        SELECT                                                                      " +
                "            server_id,                                                              " +
                "            dimension,                                                              " +
                "            x,                                                                      " +
                "            z,                                                                      " +
                "            COUNT(*) AS cnt,                                                        " +
                "            BOOL_OR(legacy) AS any_legacy,                                          " +
                "            MAX(id) AS max_id                                                       " +
                "        FROM                                                                        " +
                "            (                                                                       " +
                "                SELECT                                                              " +
                "                    *                                                               " +
                "                FROM                                                                " +
                "                    hits                                                            " +
                "                WHERE                                                               " +
                "                    id > ?                                                          " +
                "                    AND ABS(x) > 100                                                " +
                "                    AND ABS(z) > 100                                                " +
                "                    AND ABS(ABS(x) - ABS(z)) > 100                                  " +
                "                    AND x::BIGINT * x::BIGINT + z::BIGINT * z::BIGINT > 1000 * 1000 " +
                "                ORDER BY id                                                         " +
                "                LIMIT 50000                                                         " +
                "             ) tmp                                                                  " +
                "        GROUP BY                                                                    " +
                "            server_id, dimension, x, z                                              "
        )) {
            stmt.setLong(1, startID);
            try (ResultSet rs = stmt.executeQuery()) {
                List<AggregatedHits> ret = new ArrayList<>();
                while (rs.next()) {
                    AggregatedHits aggr = new AggregatedHits();
                    aggr.serverID = rs.getInt("server_id");
                    aggr.dimension = rs.getInt("dimension");
                    aggr.x = rs.getInt("x");
                    aggr.z = rs.getInt("z");
                    aggr.count = rs.getInt("cnt");
                    aggr.anyLegacy = rs.getBoolean("any_legacy");
                    aggr.maxID = rs.getLong("max_id");
                    ret.add(aggr);
                }
                return ret;
            }
        }
    }

    public boolean aggregateHits() {
        try (Connection connection = Database.getConnection()) {
            try {
                connection.setAutoCommit(false);
                long lastProcessedHitID;
                try (PreparedStatement stmt = connection.prepareStatement("SELECT last_processed_hit_id FROM dbscan_progress");
                     ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    lastProcessedHitID = rs.getLong("last_processed_hit_id");
                }
                long maxHitIDProcessed = 0;
                List<AggregatedHits> aggregated = queryAggregates(lastProcessedHitID, connection);
                if (aggregated.isEmpty()) {
                    return false;
                }
                for (int i = 0; i < aggregated.size(); i++) {
                    AggregatedHits aggr = aggregated.get(i);
                    maxHitIDProcessed = Math.max(aggr.maxID, maxHitIDProcessed);
                    int syntheticCount = aggr.count;
                    if (aggr.anyLegacy) {
                        syntheticCount += THRESHOLD + 1; // bump it up to THRESHOLD+1 so that it's >THRESHOLD, not just >=
                    }

                    int prevCountInDB = 0;
                    int dbID = 0;
                    boolean wasInDB;
                    boolean dbIsCore = false;

                    try (PreparedStatement stmt = connection.prepareStatement("SELECT cnt, id, is_core FROM dbscan WHERE server_id = ? AND dimension = ? AND x = ? AND z = ?")) {
                        stmt.setInt(1, aggr.serverID);
                        stmt.setInt(2, aggr.dimension);
                        stmt.setInt(3, aggr.x);
                        stmt.setInt(4, aggr.z);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                prevCountInDB = rs.getInt("cnt");
                                syntheticCount += prevCountInDB;
                                dbID = rs.getInt("id");
                                dbIsCore = rs.getBoolean("is_core");
                                wasInDB = true;
                            } else {
                                wasInDB = false;
                            }
                        }
                    }

                    boolean needsRangedUpdate = false;
                    if (!wasInDB) {
                        try (PreparedStatement stmt = connection.prepareStatement("" +
                                "INSERT INTO dbscan (cnt, server_id, dimension, x, z, needs_update, is_core, cluster_parent, disjoint_rank) VALUES" +
                                "                   (?,   ?,         ?,         ?, ?, TRUE,         ?,       NULL,           0)")) {
                            stmt.setInt(1, syntheticCount);
                            stmt.setInt(2, aggr.serverID);
                            stmt.setInt(3, aggr.dimension);
                            stmt.setInt(4, aggr.x);
                            stmt.setInt(5, aggr.z);
                            stmt.setBoolean(6, aggr.anyLegacy);
                            stmt.execute();
                        }
                        needsRangedUpdate = true;
                    } else {
                        // was in db
                        if (aggr.anyLegacy && !dbIsCore) {
                            needsRangedUpdate = true;
                            try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET is_core = TRUE WHERE id = ?")) {
                                stmt.setInt(1, dbID);
                                stmt.execute();
                            }
                        }
                        if (prevCountInDB <= THRESHOLD) { // if it's already >THRESHOLD, then there's nothing to do at all
                            try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan SET cnt = ? WHERE id = ?")) {
                                stmt.setInt(1, syntheticCount);
                                stmt.setInt(2, dbID);
                                stmt.execute();
                            }
                            if (syntheticCount > THRESHOLD) {
                                needsRangedUpdate = true;
                            }
                        }
                    }
                    if (needsRangedUpdate) { // NOTE: the ranged update will include the centered point itself!
                        DBSCAN.markForUpdateAllWithinRadius(aggr.serverID, aggr.dimension, aggr.x, aggr.z, connection);
                    }
                    if (i % 250 == 0 || i == aggregated.size() - 1) {
                        System.out.println("Processing aggregated hits " + i + " of " + aggregated.size());
                        connection.commit();
                    }
                }
                try (PreparedStatement stmt = connection.prepareStatement("UPDATE dbscan_progress SET last_processed_hit_id = ?")) {
                    stmt.setLong(1, maxHitIDProcessed);
                    stmt.execute();
                }
                connection.commit();
                return true;
            } catch (SQLException ex) {
                connection.rollback();
                throw ex;
            } catch (Throwable th) {
                connection.rollback();
                th.printStackTrace();
                throw new RuntimeException(th);
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
