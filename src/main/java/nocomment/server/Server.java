package nocomment.server;

import java.util.HashMap;
import java.util.Map;

public class Server {
    private final Map<Integer, World> worlds = new HashMap<>();

    public synchronized World getWorld(int dimension) {
        return worlds.computeIfAbsent(dimension, d -> new World(this, d));
    }
}