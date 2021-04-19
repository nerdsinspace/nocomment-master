package nocomment.master;

import nocomment.master.db.Database;
import nocomment.master.network.Connection;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.tracking.OnlinePlayerTracker;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.Config;
import nocomment.master.util.LoggingExecutor;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class Server {

    private static final Map<String, Server> servers = new HashMap<>();

    public static synchronized Server getServer(String serverName) {
        return servers.computeIfAbsent(serverName, Server::new);
    }

    public static synchronized Optional<Server> getServerIfLoaded(short serverID) {
        return servers.values().stream().filter(server -> server.serverID == serverID).findFirst();
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
                resp.append(", pending ");
                Collection<PriorityDispatchable> pending = world.getPending();
                resp.append(pending.size());
                resp.append(", total checks ");
                resp.append(pending.stream().mapToInt(PriorityDispatchable::size).sum());
                resp.append(", connections: ");
                for (Connection conn : world.getOpenConnections()) {
                    resp.append("\n Connection ").append(conn);
                }
                resp.append("\nStats:");
                long start = System.currentTimeMillis();
                resp.append(world.stats.report().replace("\n", " \n"));
                long end = System.currentTimeMillis();
                resp.append("\nStats lock held for ");
                resp.append(end - start);
                resp.append("ms");
            }
            resp.append("\nEnd status");
            if ("true".equalsIgnoreCase(Config.getRuntimeVariable("SERVER_STATS", "false"))) {
                System.out.println(resp.toString().replace("\n", "\n>  "));
            }
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