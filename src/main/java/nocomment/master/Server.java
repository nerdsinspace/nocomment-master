package nocomment.master;

import nocomment.master.db.Database;
import nocomment.master.network.Connection;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.LoggingExecutor;
import nocomment.master.util.OnlinePlayerTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final Map<String, Server> servers = new HashMap<>();

    public static synchronized Server getServer(String serverName) {
        return servers.computeIfAbsent(serverName, Server::new);
    }

    public final String hostname;
    public final short serverID;

    private final Map<Short, World> worlds = new HashMap<>();
    private final OnlinePlayerTracker onlinePlayers;
    public final TrackyTrackyManager tracking;

    private Server(String hostname) {
        if (NoComment.DRY_RUN) {
            throw new IllegalStateException();
        }
        this.hostname = hostname;
        this.serverID = Database.idForServer(hostname);
        this.onlinePlayers = new OnlinePlayerTracker(this);
        this.tracking = new TrackyTrackyManager(this);
        System.out.println("Constructed server " + hostname + " ID " + serverID);
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(() -> {
            StringBuilder resp = new StringBuilder("\nStatus of server " + hostname + " ID " + serverID + ":");
            for (World world : getLoadedWorlds()) {
                resp.append("\nDimension ").append(world.dimension);
                resp.append(", pending tasks ");
                Collection<Task> pending = world.getPendingTasks();
                resp.append(pending.size());
                resp.append(", total checks ");
                resp.append(pending.stream().mapToInt(task -> task.count).sum());
                resp.append(", connections: ");
                for (Connection conn : world.getOpenConnections()) {
                    resp.append("\nConnection ").append(conn);
                }
            }
            resp.append("\nEnd status");
            System.out.println(resp.toString().replace("\n", "\n>  "));
        }), 0, 30, TimeUnit.SECONDS);
    }

    public synchronized World getWorld(short dimension) {
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