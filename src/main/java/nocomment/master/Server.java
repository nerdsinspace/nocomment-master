package nocomment.master;

import java.util.HashMap;
import java.util.Map;

public class Server {
    private static final Map<String, Server> servers = new HashMap<>();

    public static synchronized Server getServer(String serverName) {
        return servers.computeIfAbsent(serverName, sn -> new Server());
    }

    private final Map<Integer, World> worlds = new HashMap<>();

    public synchronized World getWorld(int dimension) {
        return worlds.computeIfAbsent(dimension, d -> new World(this, d));
    }

    // TODO handle online and offline players
}