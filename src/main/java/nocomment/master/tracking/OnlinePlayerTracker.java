package nocomment.master.tracking;

import io.prometheus.client.Gauge;
import nocomment.master.NoComment;
import nocomment.master.Server;
import nocomment.master.db.Database;
import nocomment.master.network.Connection;
import nocomment.master.network.QueueStatus;
import nocomment.master.util.OnlinePlayer;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class OnlinePlayerTracker {
    private static final Gauge onlinePlayers = Gauge.build()
            .name("online_players")
            .help("Number of online players")
            .labelNames("server")
            .register();

    public final Server server;
    private Set<OnlinePlayer> onlinePlayerSet;
    private static final long REMOVAL_QUELL_DURATION_MS = TimeUnit.SECONDS.toMillis(30);
    private final Map<OnlinePlayer, Long> removalTimestamps = new HashMap<>();

    public OnlinePlayerTracker(Server server) {
        this.server = server;
        this.onlinePlayerSet = new HashSet<>();
        Database.clearSessions(server.serverID);
    }

    public synchronized void update() {
        // anything could have changed
        // we have lock on Server, but not on World nor any Connections

        long now = System.currentTimeMillis();
        if (onlinePlayerSet.isEmpty()) {
            try {
                // when a player joins for the first time, they send us all their online players all at once
                // this, in practice, ends up being exactly two batches in this functions, since the function takes some time the first time, so that by the second time they're all in
                // just wait so it's all in one batch
                // this is on an executor thread so this is Fine™
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Set<OnlinePlayer> current = coalesceFromConnections();
        List<Integer> toAdd = minus(current, onlinePlayerSet);
        List<Integer> toRemove = minus(onlinePlayerSet, current);
        if (!toAdd.isEmpty()) {
            System.out.println("Players joined: " + toAdd);

            Set<Integer> trackIDs = Database.trackIDsToResume(toAdd, server.serverID); // have to do this on this thread, strictly before addPlayers ruins their last-leave times :)

            NoComment.executor.execute(() -> {
                trackIDs.removeIf(server.tracking::hasActiveFilter);
                Database.resumeTracks(trackIDs).forEach(server.tracking::attemptResume);
            });

            Database.addPlayers(server.serverID, toAdd, now);
            toAdd.forEach(pid -> QueueStatus.markIngame(pid, server.serverID));
        }
        if (!toRemove.isEmpty()) {
            System.out.println("Players left: " + toRemove);
            Database.removePlayers(server.serverID, toRemove, now);
        }
        onlinePlayerSet = current;
        onlinePlayers.labels(server.hostname).set(onlinePlayerSet.size());
    }

    private static List<Integer> minus(Set<OnlinePlayer> a, Set<OnlinePlayer> b) {
        Set<OnlinePlayer> subtracted = new HashSet<>(a);
        subtracted.removeAll(b);

        List<Integer> IDs = new ArrayList<>();
        subtracted.forEach(player -> IDs.add(Database.idForPlayer(player)));
        return IDs;
    }


    private Set<OnlinePlayer> coalesceFromConnections() {

        // while this is thread safe, the connections can change at any time. collect them into one list just for these two consecutive for loops. god i'm paranoid!
        List<Connection> conns = new ArrayList<>();
        server.getLoadedWorlds().forEach(world -> conns.addAll(world.getOpenConnections()));

        HashSet<OnlinePlayer> online = new HashSet<>();
        for (Connection conn : conns) {
            conn.addOnlinePlayers(online);
            conn.addQuelledFromRemoval(removalTimestamps);
        }
        clearQuelled();
        online.removeAll(removalTimestamps.keySet());
        return online;
    }

    private void clearQuelled() {
        for (OnlinePlayer player : new ArrayList<>(removalTimestamps.keySet())) { // god i hate concurrentmodificationexception
            if (removalTimestamps.get(player) < System.currentTimeMillis() - REMOVAL_QUELL_DURATION_MS) {
                removalTimestamps.remove(player);
            }
        }
    }


    // the actual list is a UNION of all active connections lists, which are separately maintained by each connection
    // with an additional "stickiness" clause, which is that a "remove player" also quells any connection contributing that player to the pot, for the next 30 seconds

    // why is it designed like this?

    // its the only way i can think of to handle these two vexing edge cases

    // edge case A
    // player X is on 2b
    // bot A connects to the master
    // player X disconnects from 2b
    // bot A is still sending a player list including X
    // bot B has been connected to the server, and bot B forwards this player_remove event immediately
    // master receives the "remove X" event from B before the "X is on the server" from A

    // edge case B
    // player X is on 2b
    // player X disconnects
    // bot B connects to master, and sends a player list not containing X
    // bot A's connection to master drops, and the "X disconnected" message never goes through
    // master never removes X from the list
}
