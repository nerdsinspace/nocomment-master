package nocomment.master.db;

import io.prometheus.client.Counter;
import nocomment.master.NoComment;
import nocomment.master.clustering.DBSCAN;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.*;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class Database {

    private static final Counter databaseCommits = Counter.build()
            .name("database_commits_total")
            .help("Number of commits made to Postgres")
            .labelNames("type")
            .register();

    private static final BasicDataSource POOL;

    static {
        POOL = connect();
        if (!NoComment.DRY_RUN) {
            Maintenance.scheduleMaintenance();
            DBSCAN.INSTANCE.beginIncrementalDBSCANThread();
            Associator.INSTANCE.beginIncrementalAssociatorThread();
            ChatProcessor.INSTANCE.beginIncrementalChatProcessorThread();
            TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(() -> pruneStaleStatuses(POOL)), 0, 1, TimeUnit.MINUTES);
            TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(TableSizeMetrics::update), 0, 5, TimeUnit.SECONDS);
        }
    }

    private static BasicDataSource connect() {
        for (int i = 0; i < 60; i++) {
            try {
                return tryConnect();
            } catch (Throwable th) {
                System.out.println("Waiting 1 second then trying again...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {}
            }
        }
        System.out.println("Unable to connect, giving up.");
        System.exit(-1);
        return null;
    }

    private static BasicDataSource tryConnect() {
        System.out.println("Connecting to database...");
        BasicDataSource POOL = new BasicDataSource();
        POOL.setUsername(Objects.requireNonNull(Config.getRuntimeVariable("PSQL_USER", null),
                "Missing username for database"));
        POOL.setPassword(Objects.requireNonNull(Config.getRuntimeVariable("PSQL_PASS", null),
                "Missing password for database"));
        POOL.setDriverClassName("org.postgresql.Driver");
        POOL.setUrl(Objects.requireNonNull(Config.getRuntimeVariable("PSQL_URL", null),
                "Missing url for database"));
        POOL.setInitialSize(1);
        POOL.setMaxTotal(75);
        POOL.setAutoCommitOnReturn(true); // make absolutely sure
        POOL.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        POOL.setRollbackOnReturn(true);
        POOL.setDefaultReadOnly(NoComment.DRY_RUN);
        try {
            if (!NoComment.DRY_RUN) {
                pruneStaleStatuses(POOL);
            }
        } catch (Throwable th) {
            th.printStackTrace();
            System.out.println("Database ping failed!");
            try {
                POOL.close();
            } catch (SQLException ex) {}
            throw th;
        }
        System.out.println("Connected.");
        return POOL;
    }

    static void saveHit(Hit hit, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO hits (created_at, x, z, dimension, server_id, track_id) VALUES (?, ?, ?, ?, ?, ?) RETURNING id")) {
            stmt.setLong(1, hit.createdAt);
            stmt.setInt(2, hit.pos.x);
            stmt.setInt(3, hit.pos.z);
            stmt.setShort(4, hit.dimension);
            stmt.setShort(5, hit.serverID);
            if (hit.getTrackID().isPresent()) {
                stmt.setInt(6, hit.getTrackID().getAsInt());
            } else {
                stmt.setNull(6, Types.INTEGER);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                hit.setHitID(rs.getLong("id"));
            }
        }
        if (hit.getTrackID().isPresent()) {
            updateTrackWithMostRecentHit(hit, connection);
        }
    }

    public static void clearSessions(short serverID) {
        if (NoComment.DRY_RUN) {
            throw new IllegalStateException();
        }
        long mostRecent = mostRecentEvent(serverID);
        long setLeaveTo = mostRecent + 1; // range inclusivity
        if (setLeaveTo >= System.currentTimeMillis()) {
            // this will crash later, as soon as we try and add a player and the range overlaps
            // might as well crash early
            throw new RuntimeException("lol server clock went backwards");
        }
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("UPDATE player_sessions SET leave = ? WHERE range @> ? AND server_id = ?")) {
            stmt.setLong(1, setLeaveTo);
            stmt.setLong(2, Long.MAX_VALUE - 1); // must be -1 since postgres ranges are exclusive on the upper end
            stmt.setShort(3, serverID);
            stmt.executeUpdate();
            Database.incrementCommitCounter("clear_sessions");
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.exit(-1);
            throw new RuntimeException(ex);
        }
    }

    private static long mostRecentEvent(short serverID) {
        try (Connection connection = POOL.getConnection()) {
            long mostRecent;
            try (PreparedStatement stmt = connection.prepareStatement("SELECT created_at FROM last_by_server WHERE server_id = ?")) {
                stmt.setShort(1, serverID);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    mostRecent = rs.getLong(1);
                }
            }
            try (PreparedStatement stmt = connection.prepareStatement("SELECT MAX(\"join\") FROM player_sessions WHERE range @> ? AND server_id = ?")) {
                stmt.setLong(1, Long.MAX_VALUE - 1); // must be -1 since postgres ranges are exclusive on the upper end
                stmt.setShort(2, serverID);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    mostRecent = Math.max(mostRecent, rs.getLong(1));
                }
            }
            return mostRecent;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private static OptionalInt idForExistingPlayer(OnlinePlayer player) {
        boolean hasUsername = player.hasUsername();
        try (
                Connection connection = POOL.getConnection();
                PreparedStatement stmt = connection.prepareStatement(
                        hasUsername ?
                                "UPDATE players SET username = ? WHERE uuid = ? RETURNING id"
                                : "SELECT id FROM players WHERE uuid = ?"
                )
        ) {
            if (hasUsername) {
                stmt.setString(1, player.username);
                stmt.setObject(2, player.uuid);
            } else {
                stmt.setObject(1, player.uuid);
            }

            try (ResultSet existing = stmt.executeQuery()) {
                if (hasUsername) {
                    Database.incrementCommitCounter("player_username");
                }
                if (existing.next()) {
                    return OptionalInt.of(existing.getInt("id"));
                } else {
                    return OptionalInt.empty();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static int idForPlayer(OnlinePlayer player) {
        OptionalInt existing = idForExistingPlayer(player);
        if (existing.isPresent()) {
            return existing.getAsInt();
        }
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO players (username, uuid) VALUES (?, ?) RETURNING id")) {
            stmt.setString(1, player.username);
            stmt.setObject(2, player.uuid);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                Database.incrementCommitCounter("player");
                return rs.getInt("id");
            }
        } catch (SQLException ex) {
            ex.printStackTrace(); // two threads ask for same player for the first time, at same time
            return idForExistingPlayer(player).getAsInt();
        }
    }

    public static OptionalInt getPlayer(String username) {
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT id FROM players WHERE username = ?")) {
            stmt.setString(1, username);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return OptionalInt.of(rs.getInt("id"));
                } else {
                    return OptionalInt.empty();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static Optional<String> getUsername(int playerID) {
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT username FROM players WHERE id = ?")) {
            stmt.setInt(1, playerID);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getString("username"));
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private static Optional<Short> idForExistingServer(String hostname) {
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT id FROM servers WHERE hostname = ?")) {
            stmt.setString(1, hostname);
            try (ResultSet existing = stmt.executeQuery()) {
                if (existing.next()) {
                    return Optional.of(existing.getShort("id"));
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static short idForServer(String hostname) {
        Optional<Short> existing = idForExistingServer(hostname);
        if (existing.isPresent()) {
            return existing.get();
        }
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO servers (hostname) VALUES (?) RETURNING id")) {
            stmt.setString(1, hostname);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                Database.incrementCommitCounter("server");
                return rs.getShort("id");
            }
        } catch (SQLException ex) {
            ex.printStackTrace(); // two threads ask for same server for the first time, at same time
            return idForExistingServer(hostname).get();
        }
    }

    public static void addPlayers(short serverID, Collection<Integer> playerIDs, long now) {
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO player_sessions (player_id, server_id, \"join\", leave) VALUES (?, ?, ?, NULL)")) {
            for (int playerID : playerIDs) {
                stmt.setInt(1, playerID);
                stmt.setShort(2, serverID);
                stmt.setLong(3, now);
                stmt.execute();
                Database.incrementCommitCounter("player_sessions_join");
            }
            // note: this doesn't use batch because of https://github.com/pgjdbc/pgjdbc/issues/194
            // as of https://github.com/leijurv/nocomment-master/commit/0ed24f993100241d6467d1e21dbbfd5efff82f61
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static void removePlayers(short serverID, Collection<Integer> playerIDs, long now) {
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("UPDATE player_sessions SET leave = ? WHERE range @> ? AND player_id = ? AND server_id = ? AND leave IS NULL")) {
            for (int playerID : playerIDs) {
                stmt.setLong(1, now);
                stmt.setLong(2, Long.MAX_VALUE - 1); // must be -1 since postgres ranges are exclusive on the upper end
                stmt.setInt(3, playerID);
                stmt.setShort(4, serverID);
                int numRows = stmt.executeUpdate();
                if (numRows != 1) {
                    throw new IllegalStateException("player_sessions illegal state " + serverID + " " + playerID + " " + now + " " + numRows);
                }
                Database.incrementCommitCounter("player_sessions_leave");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static int createTrack(Hit initialHit, OptionalInt prevTrackID) {
        // make sure that the initialHit has an assigned hit ID
        initialHit.saveToDBBlocking();
        try (Connection connection = POOL.getConnection()) {
            connection.setAutoCommit(false);
            int trackID;
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO tracks (first_hit_id, last_hit_id, updated_at, prev_track_id, dimension, server_id) VALUES (?, ?, ?, ?, ?, ?) RETURNING id")) {
                stmt.setLong(1, initialHit.getHitID());
                stmt.setLong(2, initialHit.getHitID());
                stmt.setLong(3, initialHit.createdAt);
                if (prevTrackID.isPresent()) {
                    stmt.setInt(4, prevTrackID.getAsInt());
                } else {
                    stmt.setNull(4, Types.INTEGER);
                }
                stmt.setShort(5, initialHit.dimension);
                stmt.setShort(6, initialHit.serverID);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    trackID = rs.getInt("id");
                }
            }
            // use transaction atomicity to maintain the invariant of tracks and hits referring to each other circularly
            try (PreparedStatement stmt = connection.prepareStatement("UPDATE hits SET track_id = ? WHERE id = ?")) {
                // can't call alterHitToBeWithinTrack directly because it requires us to hold the lock on the Hit
                stmt.setInt(1, trackID);
                stmt.setLong(2, initialHit.getHitID());
                stmt.executeUpdate();
            }
            // note: hits will be updated to set the track_id twice, this is a nonissue, and is a sanity check in case the queues back up
            connection.commit();
            Database.incrementCommitCounter("create_track");
            return trackID;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    static void alterHitToBeWithinTrack(Hit hit) {
        try (Connection connection = POOL.getConnection()) {
            connection.setAutoCommit(false);
            updateTrackWithMostRecentHit(hit, connection);
            try (PreparedStatement stmt = connection.prepareStatement("UPDATE hits SET track_id = ? WHERE id = ?")) {
                stmt.setInt(1, hit.getTrackID().getAsInt());
                stmt.setLong(2, hit.getHitID());
                stmt.executeUpdate();
            }
            connection.commit();
            Database.incrementCommitCounter("hit_alter_track");
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private static void updateTrackWithMostRecentHit(Hit hit, Connection connection) throws SQLException {
        // preserve invariant of the last hit in the track having the highest updated_at
        try (PreparedStatement stmt = connection.prepareStatement("UPDATE tracks SET last_hit_id = ?, updated_at = ? WHERE id = ? AND updated_at <= ?")) {
            stmt.setLong(1, hit.getHitID());
            stmt.setLong(2, hit.createdAt);
            stmt.setInt(3, hit.getTrackID().getAsInt());
            stmt.setLong(4, hit.createdAt);
            stmt.executeUpdate();
        }
    }

    public static Set<Integer> trackIDsToResume(Collection<Integer> playerIDs, short serverID) {
        try (Connection connection = POOL.getConnection()) {

            Set<Long> logoutTimestamps = new HashSet<>(); // set because there will be many duplicates

            try (PreparedStatement stmt = connection.prepareStatement("SELECT MAX(UPPER(range)) FROM player_sessions WHERE player_id = ? AND server_id = ?")) {
                for (int playerID : playerIDs) {
                    stmt.setInt(1, playerID);
                    stmt.setShort(2, serverID);
                    try (ResultSet rs = stmt.executeQuery()) {
                        rs.next();
                        long time = rs.getLong(1);
                        if (!rs.wasNull()) {
                            logoutTimestamps.add(time);
                        }
                    }
                }
            }

            Set<Integer> trackIDsToResume = new HashSet<>(); // set because there will be many duplicates

            if (logoutTimestamps.isEmpty()) {
                return trackIDsToResume;
            }

            try (PreparedStatement stmt = connection.prepareStatement("SELECT id FROM tracks WHERE updated_at >= ? AND updated_at <= ? AND server_id = ?")) {
                for (long logoutTimestamp : logoutTimestamps) {
                    stmt.setLong(1, logoutTimestamp - TimeUnit.MINUTES.toMillis(1));
                    stmt.setLong(2, logoutTimestamp + TimeUnit.MINUTES.toMillis(1));
                    stmt.setShort(3, serverID);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            trackIDsToResume.add(rs.getInt(1));
                        }
                    }
                }
            }

            return trackIDsToResume;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static List<TrackResume> resumeTracks(Collection<Integer> trackIDs) {
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT hits.x, hits.z, hits.dimension FROM tracks INNER JOIN hits ON hits.id = tracks.last_hit_id WHERE tracks.id = ?")) {
            List<TrackResume> ret = new ArrayList<>();
            for (int trackID : trackIDs) {
                stmt.setInt(1, trackID);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    ret.add(new TrackResume(rs.getInt("x"), rs.getInt("z"), rs.getShort("dimension"), trackID));
                }
            }
            return ret;
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static int numOnlineAt(short serverID, long timestamp) {
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) AS cnt FROM player_sessions WHERE range @> ? AND server_id = ?")) {
            stmt.setLong(1, timestamp);
            stmt.setShort(2, serverID);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt("cnt");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static OptionalLong sessionJoinedAt(int playerID, short serverID, long wasInAt) {
        long start = System.currentTimeMillis();
        try (Connection connection = POOL.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT \"join\" FROM player_sessions WHERE range @> ? AND player_id = ? AND server_id = ?")) {
            stmt.setLong(1, wasInAt);
            stmt.setInt(2, playerID);
            stmt.setShort(3, serverID);
            try (ResultSet rs = stmt.executeQuery()) {
                long end = System.currentTimeMillis();
                System.out.println("sessionJoinedAt took " + (end - start) + "ms for " + playerID + " " + serverID + " " + wasInAt);
                if (rs.next()) {
                    return OptionalLong.of(rs.getLong("join"));
                } else {
                    return OptionalLong.empty();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static Set<Integer> allPlayerIDsThatLeftBetween(long rangeStart, long rangeEnd, short serverID, Connection connection) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT player_id FROM player_sessions WHERE range && INT8RANGE(?, ?, '[]') AND server_id = ? AND leave IS NOT NULL AND leave >= ? AND leave <= ?")) {
            stmt.setLong(1, rangeStart);
            stmt.setLong(2, rangeEnd);
            stmt.setShort(3, serverID);
            stmt.setLong(4, rangeStart);
            stmt.setLong(5, rangeEnd);
            try (ResultSet rs = stmt.executeQuery()) {
                Set<Integer> ret = new HashSet<>();
                while (rs.next()) {
                    ret.add(rs.getInt("player_id"));
                }
                return ret;
            }
        }
    }

    private static void pruneStaleStatuses(BasicDataSource pool) {
        try (Connection connection = pool.getConnection();
             PreparedStatement stmt = connection.prepareStatement("UPDATE statuses SET curr_status = 'OFFLINE'::statuses_enum, data = NULL, updated_at = ? WHERE updated_at < ? AND curr_status != 'OFFLINE'::statuses_enum")) {
            stmt.setLong(1, System.currentTimeMillis());
            stmt.setLong(2, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));
            stmt.execute();
            Database.incrementCommitCounter("prune_stale_statuses");
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static void updateStatus(Connection connection, int playerID, short serverID, String currStatus, Optional<String> data, long now) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("UPDATE statuses SET curr_status = ?::statuses_enum, updated_at = ?, data = ? WHERE player_id = ? AND server_id = ?")) {
            stmt.setString(1, currStatus);
            stmt.setLong(2, now);
            if (data.isPresent()) {
                stmt.setString(3, data.get());
            } else {
                stmt.setNull(3, Types.VARCHAR);
            }
            stmt.setInt(4, playerID);
            stmt.setShort(5, serverID);
            int numRows = stmt.executeUpdate();
            if (numRows > 0) {
                return; // success
            }
        }
        // update hit 0 rows, so we need to insert
        try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO statuses (player_id, server_id, curr_status, updated_at, data) VALUES (?, ?, ?::statuses_enum, ?, ?)")) {
            stmt.setInt(1, playerID);
            stmt.setShort(2, serverID);
            stmt.setString(3, currStatus);
            stmt.setLong(4, now);
            if (data.isPresent()) {
                stmt.setString(5, data.get());
            } else {
                stmt.setNull(5, Types.VARCHAR);
            }
            stmt.execute();
        }
    }

    public static void setDimension(Connection connection, int playerID, short serverID, short dimension) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("UPDATE statuses SET dimension = ? WHERE player_id = ? AND server_id = ?")) {
            stmt.setShort(1, dimension);
            stmt.setInt(2, playerID);
            stmt.setShort(3, serverID);
            stmt.execute();
        }
    }

    public static void saveChat(Connection connection, String chat, short chatType, int playerID, short serverID, long timestamp) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO chat (data, chat_type, reported_by, created_at, server_id) VALUES (CAST(? AS JSON), ?, ?, ?, ?)")) {
            stmt.setString(1, chat);
            stmt.setShort(2, chatType);
            stmt.setInt(3, playerID);
            stmt.setLong(4, timestamp);
            stmt.setShort(5, serverID);
            stmt.execute();
        }
    }

    static void vacuum() {
        try (Connection connection = POOL.getConnection(); PreparedStatement stmt = connection.prepareStatement("VACUUM ANALYZE")) {
            stmt.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    static void reindex(String indexName) {
        try (Connection connection = POOL.getConnection(); PreparedStatement stmt = connection.prepareStatement("REINDEX INDEX CONCURRENTLY " + indexName)) {
            stmt.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static Connection getConnection() throws SQLException {
        return POOL.getConnection();
    }

    public static void incrementCommitCounter(String type) {
        databaseCommits.labels(type).inc();
    }
}
