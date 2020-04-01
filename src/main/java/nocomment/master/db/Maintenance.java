package nocomment.master.db;

import nocomment.master.tracking.TrackyTrackyManager;

import java.util.concurrent.TimeUnit;

public class Maintenance {
    static void scheduleMaintenance() {
        onceADay(Database::vacuum, "vacuum");
        scheduleReindex("hits_time_and_place");
        scheduleReindex("hits_tracks");
        scheduleReindex("hits_pkey");
        scheduleReindex("track_endings");
        scheduleReindex("player_sessions_range");
    }

    private static void scheduleReindex(String indexName) {
        onceADay(() -> Database.reindex(indexName), "reindex " + indexName);
    }

    // a reindex on hits_time_and_place cut it down from 300mb to 120mb, so this is actually useful and not just a meme
    private static void onceADay(Runnable runnable, String name) {
        int period = 86400;
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(() -> {
            synchronized (Maintenance.class) { // dont run more than one maintenance at a time, even at random. it causes database deadlock
                long start = System.currentTimeMillis();
                runnable.run();
                long end = System.currentTimeMillis();
                System.out.println("Took " + (end - start) + "ms to run maintenance task " + name);
            }
        }, (long) (period * Math.random()), period, TimeUnit.SECONDS);
    }
}
