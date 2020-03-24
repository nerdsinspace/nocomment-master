package nocomment.master;

import nocomment.master.db.Database;
import nocomment.master.util.OnlinePlayerTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Server {
    private static final Map<String, Server> servers = new HashMap<>();

    public static synchronized Server getServer(String serverName) {
        return servers.computeIfAbsent(serverName, Server::new);
    }

    private final Map<Integer, World> worlds = new HashMap<>();
    public final String hostname;
    public final int databaseID;
    private final OnlinePlayerTracker onlinePlayers;

    private Server(String hostname) {
        this.hostname = hostname;
        this.databaseID = Database.idForHostname(hostname);
        this.onlinePlayers = new OnlinePlayerTracker(this);
        System.out.println("Constructed server " + hostname + " ID " + databaseID);
    }

    public synchronized World getWorld(int dimension) {
        return worlds.computeIfAbsent(dimension, d -> new World(this, d));
    }

    public synchronized void update() {
        // called whenever anything changes with a connection
        // also called like, all the time lol

        // currently all this needs to do is update onlinePlayers
        onlinePlayers.update();
    }

    public synchronized Collection<World> getLoadedWorlds() {
        return new ArrayList<>(worlds.values());
    }
}