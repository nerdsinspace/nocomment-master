package nocomment.master.util;

import nocomment.master.World;
import nocomment.master.db.Database;
import nocomment.master.network.Connection;
import nocomment.master.tracking.TrackyTrackyManager;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Staggerer {
    private static final long MAX_AGE = 12_600_000; // 3.5 hours
    private final World world;

    public Staggerer(World world) {
        this.world = world;
    }

    public void start() {
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::run), 5, 10, TimeUnit.MINUTES);
    }

    private void run() {
        Collection<Connection> conns = world.getOpenConnections();
        if (conns.size() < 2) {
            return;
        }
        Map<Connection, Long> joins = new HashMap<>();
        for (Connection conn : conns) {
            OptionalLong join = Database.currentSessionJoinedAt(conn.getIdentity(), world.server.serverID);
            if (!join.isPresent()) {
                System.out.println("Exceptional situation (i put that in so it would show up when i grep for Exception in the server logs) where a connection is here but not actually on the server " + System.currentTimeMillis() + " " + conn + " " + conn.getIdentity());
                return;
            }
            joins.put(conn, join.getAsLong());
        }
        Connection oldest = conns.stream().min(Comparator.comparingLong(joins::get)).get();
        long joinedAt = joins.get(oldest);
        long age = System.currentTimeMillis() - joinedAt;
        System.out.println("Oldest account is " + oldest + " which has been on for " + age + "ms");
        if (age > MAX_AGE) {
            System.out.println("KICKING " + System.currentTimeMillis() + " " + oldest);
            oldest.requestServerDisconnect();
        }
    }
}
