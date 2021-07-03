package nocomment.master.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
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
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

public final class Staggerer {

    private static final Histogram staggererLatencies = Histogram.build()
            .name("staggerer_latencies")
            .help("Staggerer latencies")
            .register();
    private static final Counter kicks = Counter.build()
            .name("stagerer_kicks")
            .help("Staggerer kicks")
            .labelNames("dimension", "server", "username")
            .register();

    private static final long AUTO_KICK = TimeUnit.HOURS.toMillis(6);
    private static final long STAGGER = AUTO_KICK / 3; // 2 hours
    private static final long STARTUP = System.currentTimeMillis();
    private final World world;
    private final Map<Integer, Long> observedAt = new HashMap<>();
    private final IntPredicate identityFilter;

    private Staggerer(World world, IntPredicate identityFilter) {
        this.world = world;
        this.identityFilter = identityFilter;
    }

    public static void beginStaggerer(World world, int[]... staggerGroups) {
        IntOpenHashSet inAny = new IntOpenHashSet();
        for (int[] group : staggerGroups) {
            IntOpenHashSet thisGroup = new IntOpenHashSet(group);
            new Staggerer(world, thisGroup::contains).start();
            inAny.addAll(thisGroup);
        }
        new Staggerer(world, $ -> !inAny.contains($)).start();
    }

    public static void beginStaggerer2b2tPreset(World world) {
        beginStaggerer(world,
                new int[]{167548, 132678}, // 100010, liejurv
                new int[]{47299, 1026}, // smibby_smouse, ufocrossing
                new int[]{6806, 162223, 306345}, // babbaj, elon_musk, reaganandbush
                new int[]{51246, 227868}, // parrotbot, humanwrongs

                new int[]{} // empty, for aesthetic reasons
        );
    }

    private void start() {
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

    private boolean nearLodge() {
        long meetingTime = 248400;
        long stopKicking = meetingTime - TimeUnit.HOURS.toSeconds(2);
        long startDoingItAgain = meetingTime + TimeUnit.HOURS.toSeconds(2);

        long nowSecs = System.currentTimeMillis() / TimeUnit.SECONDS.toMillis(1);
        long offsetIntoEpochWeek = nowSecs % TimeUnit.DAYS.toSeconds(7);

        return offsetIntoEpochWeek > stopKicking && offsetIntoEpochWeek < startDoingItAgain;
    }

    private void run() {
        System.out.println("Staggerer for " + world.dimension);
        System.out.println("Staggerer observations: " + observedAt);
        observedAt.values().removeIf(ts -> ts < System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30));

        List<Connection> onlineNow = new ArrayList<>(world.getOpenConnections());
        onlineNow.removeIf(conn -> !identityFilter.test(conn.getIdentity()));
        onlineNow.forEach(conn -> observedAt.put(conn.getIdentity(), System.currentTimeMillis()));
        Map<Integer, Long> playerJoinTS = new HashMap<>();
        if (world.server.hostname.equals("2b2t.org")) { // i mean this is sorta rart idk
            for (QueueStat qs : getInQueue()) {
                if (!identityFilter.test(qs.playerID)) {
                    continue;
                }
                // this is safe since statuses will be reset from QUEUE to OFFLINE within 60s of kick
                // and getInQueue will only return statuses that are currently QUEUE
                observedAt.merge(qs.playerID, qs.when, Math::max);
                playerJoinTS.put(qs.playerID, qs.when + qs.pos * QueueStatus.getEstimatedMillisecondsPerQueuePosition());
            }
            if (world.dimension == 0 && nearLodge()) {
                System.out.println("Moratorium on kicking enabled due to lodge time in the overworld on 2b2t");
                return;
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

        if (getInQueue().stream().filter(qs -> identityFilter.test(qs.playerID)).anyMatch(qs -> qs.pos < 250)) {
            System.out.println("Awaiting requeue");
            return;
        }
        Map<Integer, Long> leaveAtTS = new HashMap<>();
        Long prevLeaveAt = null;
        long now = System.currentTimeMillis();
        for (int pid : harem.stream().sorted(Comparator.comparingLong(pid -> -leaveAt(playerJoinTS.get(pid), pid, now))).collect(Collectors.toList())) {
            long joinAt = playerJoinTS.get(pid);
            long serverLeaveAt = leaveAt(joinAt, pid, now);
            long ourLeaveAt = serverLeaveAt;
            if (prevLeaveAt != null) {
                // this is the actual staggering algorithm
                // it just depends on this for loop being in sorted order from youngest to oldest (aka: last connection first, first connection last)
                long down = prevLeaveAt - Math.min(STAGGER, AUTO_KICK / harem.size());
                if (down < ourLeaveAt) {
                    leaveAtTS.put(pid, down);
                    ourLeaveAt = down;
                }
            }
            prevLeaveAt = ourLeaveAt;
            System.out.println("Player " + pid + " joined at " + format(joinAt) + " would be kicked at " + format(serverLeaveAt) + " but we'll kick at " + format(ourLeaveAt));
            /*long missedTime = serverLeaveAt - ourLeaveAt;
            if (missedTime > TimeUnit.MINUTES.toMillis(121)) {
                leaveAtTS.remove(pid);
            }*/
        }
        for (Connection conn : onlineNow) {
            Long leaveAt = leaveAtTS.get(conn.getIdentity());
            if (leaveAt != null && leaveAt < System.currentTimeMillis()) {
                kicks.labels(world.dim(), world.server.hostname, Database.getUsername(conn.getIdentity()).get()).inc();
                System.out.println("Therefore, kicking " + conn.getIdentity());
                conn.dispatchDisconnectRequest();
                return;
            }
        }
    }

    private static long leaveAt(long joinTS, int pid, long now) {
        long leave = joinTS;
        /*if (pid == 904 || pid == 33170 || pid == 102440 || pid == 102436) { // jewishbanker oremongoloid xz_9 p7k
            leave += CRAP_KICK;
        } else {*/
        leave += AUTO_KICK;
        //}
        if (pid == 25510) { // bias towards not kicking hollywoodx
            leave += TimeUnit.MINUTES.toMillis(2); // only a small bias
        }
        if (leave < now) {
            leave = now;
        }
        return leave;
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
        if (tentative < STARTUP) {
            tentative -= TimeUnit.SECONDS.toMillis(30);
        }
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
