package nocomment.master;

import nocomment.master.db.Database;

import java.util.HashMap;
import java.util.Map;

public class Server {
    private static final Map<String, Server> servers = new HashMap<>();

    public static synchronized Server getServer(String serverName) {
        return servers.computeIfAbsent(serverName, host -> new Server(host));
    }

    private final Map<Integer, World> worlds = new HashMap<>();
    public final String hostname;
    public final int databaseID;

    private Server(String hostname) {
        this.hostname = hostname;
        this.databaseID = Database.idForHostname(hostname);
        System.out.println("Constructed server " + hostname + " ID " + databaseID);
    }

    public synchronized World getWorld(int dimension) {
        return worlds.computeIfAbsent(dimension, d -> new World(this, d));
    }

    // TODO handle online and offline players
}