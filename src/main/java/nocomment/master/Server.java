package nocomment.master;

import nocomment.master.db.Database;
import nocomment.master.tracking.TrackyTrackyManager;
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

    public final String hostname;
    public final int serverID;

    private final Map<Integer, World> worlds = new HashMap<>();
    private final OnlinePlayerTracker onlinePlayers;
    public final TrackyTrackyManager tracking;

    private Server(String hostname) {
        if (true)
            throw new IllegalStateException();
        this.hostname = hostname;
        this.serverID = Database.idForServer(hostname);
        this.onlinePlayers = new OnlinePlayerTracker(this);
        this.tracking = new TrackyTrackyManager(this);
        System.out.println("Constructed server " + hostname + " ID " + serverID);
    }

    public synchronized World getWorld(int dimension) {
        return worlds.computeIfAbsent(dimension, d -> new World(this, d));
    }

    public void update() {
        // called whenever anything changes with a connection
        // also called like, all the time lol

        // currently all this needs to do is update onlinePlayers, so only synchronize there
        onlinePlayers.update();
    }

    public synchronized Collection<World> getLoadedWorlds() {
        return new ArrayList<>(worlds.values());
    }
}