package nocomment.master.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.network.Connection;
import nocomment.master.network.QueueStatus;
import nocomment.master.tracking.TrackyTrackyManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class Staggerer {

    private static final Histogram staggererLatencies = Histogram.build()
            .name("staggerer_latencies")
            .help("Staggerer latencies")
            .register();
    private static final Counter kicks = Counter.build()
            .name("stagerer_kicks")
            .help("Staggerer kicks")
            .labelNames("dimension", "server", "identity")
            .register();

    private static final long AUTO_KICK = TimeUnit.HOURS.toMillis(6);
    private static final long CRAP_KICK = TimeUnit.HOURS.toMillis(3);
    private static final long STAGGER = AUTO_KICK / 4; // 90 minutes
    private final World world;
    private final Map<Integer, Long> observedAt = new HashMap<>();

    public Staggerer(World world) {
        this.world = world;
    }

    public void start() {
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(() -> {
            long start = System.currentTimeMillis();
            staggererLatencies.time(LoggingExecutor.wrap(this::run));
            long end = System.currentTimeMillis();
            System.out.println("Staggerer took " + (end - start) + "ms");
        }, 20, 10, TimeUnit.MINUTES);
    }

    private static class QueueStat {

        private final int pos;
        private final int playerID;
        private final long when;

        public QueueStat(ResultSet rs) throws SQLException {
            this.pos = Integer.parseInt(rs.getString("data").split("Queue position: ")[1]);
            this.playerID = rs.getInt("player_id");
            this.when = rs.getLong("updated_at");
        }
    }

    private List<QueueStat> getInQueue() {
        try (java.sql.Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT data, player_id, updated_at FROM statuses WHERE curr_status = 'QUEUE'::statuses_enum AND dimension = ?")) {
            stmt.setShort(1, world.dimension);
            try (ResultSet rs = stmt.executeQuery()) {
                List<QueueStat> ret = new ArrayList<>();
                while (rs.next()) {
                    ret.add(new QueueStat(rs));
                }
                return ret;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private void run() {
        System.out.println("Staggerer for " + world.dimension);
        System.out.println("Staggerer observations: " + observedAt);
        observedAt.values().removeIf(ts -> ts < System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30));

        Collection<Connection> onlineNow = world.getOpenConnections();
        onlineNow.forEach(conn -> observedAt.put(conn.getIdentity(), System.currentTimeMillis()));
        Map<Integer, Long> playerJoinTS = new HashMap<>();
        if (world.server.hostname.equals("2b2t.org")) { // i mean this is sorta rart idk
            for (QueueStat qs : getInQueue()) {
                // this is safe since statuses will be reset from QUEUE to OFFLINE within 60s of kick
                // and getInQueue will only return statuses that are currently QUEUE
                observedAt.merge(qs.playerID, qs.when, Math::max);
                playerJoinTS.put(qs.playerID, qs.when + qs.pos * QueueStatus.getEstimatedMillisecondsPerQueuePosition());
            }
        }
        if (onlineNow.size() < 2) {
            // absolutely never stagger with just one currently active
            System.out.println("Only one");
            return;
        }

        Set<Integer> harem = observedAt.keySet();

        for (Connection conn : onlineNow) {
            OptionalLong join = currentSessionJoinedAt(conn.getIdentity(), world.server.serverID);
            if (!join.isPresent()) {
                System.out.println("Exceptional situation (i put that in so it would show up when i grep for Exception in the server logs) where a connection is here but not actually on the server " + System.currentTimeMillis() + " " + conn + " " + conn.getIdentity());
                return;
            }
            playerJoinTS.put(conn.getIdentity(), join.getAsLong());
        }

        if (harem.stream().anyMatch(id -> !playerJoinTS.containsKey(id))) {
            // a player id is in harem but not in playerJoinTS

            // this means that we know of an account, but it isn't queued or in game

            // if any harem accounts are currently OFFLINE, exit
            // (this means that we're in a transient state awaiting a server restart or reconnect)
            // only after 30 minutes of OFFLINE do we remove from harem
            System.out.println("Staggerer transient harem overlap " + harem + " " + playerJoinTS.keySet());
            return;
        }

        if (getInQueue().stream().anyMatch(qs -> qs.pos < 250)) {
            System.out.println("Awaiting requeue");
            return;
        }
        Map<Integer, Long> leaveAtTS = new HashMap<>();
        Long prevLeaveAt = null;
        System.out.println(System.currentTimeMillis());
        for (int pid : harem.stream().sorted(Comparator.comparingLong(pid -> -(playerJoinTS.get(pid) + inDuration(pid)))).collect(Collectors.toList())) {
            long joinAt = playerJoinTS.get(pid);
            long serverLeaveAt = joinAt + inDuration(pid);
            long ourLeaveAt = serverLeaveAt;
            if (prevLeaveAt != null) {
                // this is the actual staggering algorithm
                // it just depends on this for loop being in sorted order from youngest to oldest (aka: last connection first, first connection last)
                long down = prevLeaveAt - STAGGER;
                if (down < ourLeaveAt) {
                    leaveAtTS.put(pid, down);
                    ourLeaveAt = down;
                }
            }
            prevLeaveAt = ourLeaveAt;
            System.out.println("Player " + pid + " joined at " + format(joinAt) + " would be kicked at " + format(serverLeaveAt) + " but we'll kick at " + format(ourLeaveAt));
        }
        for (Connection conn : onlineNow) {
            long leaveAt = leaveAtTS.get(conn.getIdentity());
            if (leaveAt < System.currentTimeMillis()) {
                kicks.labels(world.dim(), world.server.hostname, conn.getIdentity() + "").inc();
                System.out.println("Therefore, kicking " + conn.getIdentity());
                conn.dispatchDisconnectRequest();
                return;
            }
        }
    }

    private static long inDuration(int pid) {
        if (pid == 904 || pid == 33170 || pid == 102440 || pid == 102436) { // jewishbanker oremongoloid xz_9 p7k
            return CRAP_KICK;
        } else {
            return AUTO_KICK;
        }
    }

    private static String format(long ts) {
        double h = (ts - System.currentTimeMillis()) / (double) TimeUnit.HOURS.toMillis(1);
        return Math.round(h * 100) / 100d + "hours";
    }

    public static OptionalLong currentSessionJoinedAt(int playerID, short serverID) {
        return currentSessionJoinedAt(playerID, serverID, Long.MAX_VALUE - 1);
    }

    public static OptionalLong currentSessionJoinedAt(int playerID, short serverID, long ts) {
        OptionalLong joinedAt = Database.sessionJoinedAt(playerID, serverID, ts);
        if (!joinedAt.isPresent()) {
            return joinedAt;
        }
        long timestamp = joinedAt.getAsLong();
        // check if this was a BS master restart
        if (!anyEmpty(serverID, timestamp - 100, timestamp - 200, timestamp - 300, timestamp - 400, timestamp - 500)) {
            // nope, server was well populated
            return joinedAt;
        }
        long tentative = timestamp - TimeUnit.SECONDS.toMillis(20);
        OptionalLong whatWeDoHereIsGoBack = currentSessionJoinedAt(playerID, serverID, tentative);
        if (whatWeDoHereIsGoBack.isPresent()) {
            return whatWeDoHereIsGoBack;
        }
        return joinedAt;
    }

    private static boolean anyEmpty(short serverID, long... timestamps) {
        for (long timestamp : timestamps) {
            if (Database.numOnlineAt(serverID, timestamp) == 0) {
                return true;
            }
        }
        return false;
    }
}
