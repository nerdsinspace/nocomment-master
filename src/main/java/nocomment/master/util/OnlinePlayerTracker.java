package nocomment.master.util;

import nocomment.master.Server;
import nocomment.master.network.Connection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OnlinePlayerTracker {
    public final Server server;
    private Set<OnlinePlayer> onlinePlayerSet;

    public OnlinePlayerTracker(Server server) {
        this.server = server;
        this.onlinePlayerSet = new HashSet<>();
        // TODO clear database of currently active ppl
    }

    public void update() {
        // anything could have changed
        // we have lock on Server, but not on World nor any Connections

        Set<OnlinePlayer> current = coalesceFromConnections();
        // TODO diff current with onlinePlayerSet
        // TODO make necessary changes to database
    }

    private Set<OnlinePlayer> coalesceFromConnections() {

        // while this is thread safe, the connections can change at any time. collect them into one list just for these two consecutive for loops. god i'm paranoid!
        List<Connection> conns = new ArrayList<>();
        server.getLoadedWorlds().forEach(world -> conns.addAll(world.getOpenConnections()));

        HashSet<OnlinePlayer> online = new HashSet<>();
        for (Connection conn : conns) {
            online.addAll(conn.onlinePlayers());
        }
        for (Connection conn : conns) {
            online.removeAll(conn.quelledFromRemoval());
        }
        return online;
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
