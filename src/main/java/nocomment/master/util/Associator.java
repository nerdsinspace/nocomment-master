package nocomment.master.util;

import nocomment.master.clustering.DBSCAN;
import nocomment.master.db.Database;
import nocomment.master.tracking.TrackyTrackyManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public enum Associator {
    INSTANCE;

    private static final long UNTIL = TimeUnit.HOURS.toMillis(24);
    private static final long INTERVAL = TimeUnit.HOURS.toMillis(1);

    public void beginIncrementalAssociatorThread() {
        // schedule with fixed delay is Very Important, so that we get no overlaps
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::incrementalRun), 0, 5, TimeUnit.MINUTES);
    }

    private synchronized void incrementalRun() {
        if (!DBSCAN.INSTANCE.hasCompletedAnIteration()) {
            return; // wait for our friend
        }
        while (run()) ;
    }

    private static class TrackEnding {
        private final int id;
        private final long updatedAt;
        private final short serverID;
        private final short dimension;
        private final int x;
        private final int z;

        private TrackEnding(ResultSet rs) throws SQLException {
            this.id = rs.getInt("id");
            this.updatedAt = rs.getLong("updated_at");
            this.serverID = rs.getShort("server_id");
            this.dimension = rs.getShort("dimension");
            this.x = rs.getInt("x");
            this.z = rs.getInt("z");
        }
    }

    public boolean run() {
        try (Connection connection = Database.getConnection()) {
            connection.setAutoCommit(false);
            long prevFence = 0;
            boolean needsInsert = false;
            try (PreparedStatement stmt = connection.prepareStatement("SELECT max_updated_at_processed FROM track_associator_progress");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    prevFence = rs.getLong("max_updated_at_processed");
                } else {
                    needsInsert = true;
                }
            }
            if (prevFence == 0) {
                // calculate it for real
                // this query is sorta slow (50ms) but it'll only run once, ever, so I don't care
                try (PreparedStatement stmt = connection.prepareStatement("SELECT MIN(updated_at) AS first_track_timestamp FROM tracks WHERE NOT legacy");
                     ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    prevFence = rs.getLong("first_track_timestamp");
                }
            }
            long fence = prevFence + INTERVAL;
            if (System.currentTimeMillis() - fence < UNTIL) {
                System.out.println("We are associated up till less than 1 day ago so, no");
                return false;
            }
            System.out.println(fence + " " + prevFence + " " + (fence - prevFence));
            List<TrackEnding> toProcess = new ArrayList<>();
            try (PreparedStatement stmt = connection.prepareStatement("SELECT tracks.id, tracks.updated_at, tracks.server_id, tracks.dimension, last.x, last.z FROM tracks LEFT OUTER JOIN hits AS first ON first.id = tracks.first_hit_id LEFT OUTER JOIN hits AS last ON last.id = tracks.last_hit_id WHERE NOT tracks.legacy AND ABS(last.x) > 100 AND ABS(last.z) > 100 AND ABS(ABS(last.x) - ABS(last.z)) > 100 AND last.x::bigint * last.x::bigint + last.z::bigint * last.z::bigint > 1000 * 1000 AND last.created_at - first.created_at > 3 * 60 * 1000 AND tracks.updated_at >= ? AND tracks.updated_at < ?")) {
                stmt.setLong(1, prevFence);
                stmt.setLong(2, fence);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        toProcess.add(new TrackEnding(rs));
                    }
                }
            }
            System.out.println("toProcess size " + toProcess.size());
            for (TrackEnding track : toProcess) {
                Set<Integer> possiblePlayers = Database.allPlayerIDsThatLeftBetween(track.updatedAt - TimeUnit.SECONDS.toMillis(6), track.updatedAt + TimeUnit.SECONDS.toMillis(1), track.serverID, connection);
                if (possiblePlayers.size() > 10) {
                    continue; // bad data, probably a server restart that kicked everyone
                }
                OptionalInt cluster = DBSCAN.INSTANCE.broadlyFetchAReasonablyCloseClusterIDFor(track.serverID, track.dimension, track.x, track.z, connection);
                if (!cluster.isPresent()) {
                    continue;
                }
                int clusterID = cluster.getAsInt();
                double association = 1.0d / possiblePlayers.size();
                for (int playerID : possiblePlayers) {
                    try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO associations (cluster_id, player_id, association, created_at) VALUES (?, ?, ?, ?)")) {
                        stmt.setInt(1, clusterID);
                        stmt.setInt(2, playerID);
                        stmt.setDouble(3, association);
                        stmt.setLong(4, track.updatedAt);
                        stmt.execute();
                    }
                }
            }

            try (PreparedStatement stmt = connection.prepareStatement(needsInsert ?
                    "INSERT INTO track_associator_progress (max_updated_at_processed) VALUES (?)"
                    : "UPDATE track_associator_progress SET max_updated_at_processed = ?")) {
                stmt.setLong(1, fence);
                stmt.execute();
            }
            connection.commit();
            Database.incrementCommitCounter("associator");
            return true;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}