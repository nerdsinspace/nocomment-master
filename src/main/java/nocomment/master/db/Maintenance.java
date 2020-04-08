package nocomment.master.db;

import nocomment.master.tracking.TrackyTrackyManager;

import java.util.concurrent.TimeUnit;

public class Maintenance {
    static void scheduleMaintenance() {
        onceADay(Database::vacuum, "vacuum");
        scheduleReindex("hits_time_and_place");
        scheduleReindex("hits_tracks");
        scheduleReindex("hits_pkey");
        scheduleReindex("tracks_pkey");
        scheduleReindex("track_endings");
        scheduleReindex("player_sessions_range");
        scheduleReindex("player_sessions_by_leave");
        scheduleReindex("player_sessions_server_id_player_id_range_excl");
        scheduleReindex("dbscan_pkey");
        scheduleReindex("dbscan_cluster_roots");
        scheduleReindex("dbscan_ingest");
        scheduleReindex("dbscan_process");
        scheduleReindex("dbscan_to_update");
        scheduleReindex("dbscan_disjoint_traversal");
    }

    private static void scheduleReindex(String indexName) {
        onceADay(() -> Database.reindex(indexName), "reindex " + indexName);
    }

    // a reindex on hits_time_and_place cut it down from 300mb to 120mb, so this is actually useful and not just a meme
    private static void onceADay(Runnable runnable, String name) {
        int period = 86400;
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(() ->
                new Thread(() -> { // dont clog up scheduler's fixed thread pool
                    synchronized (Maintenance.class) { // dont run more than one maintenance at a time, even at random. it causes database deadlock
                        long start = System.currentTimeMillis();
                        runnable.run();
                        long end = System.currentTimeMillis();
                        System.out.println("Took " + (end - start) + "ms to run maintenance task " + name);
                    }
                }).start(), (long) (period * Math.random()), period, TimeUnit.SECONDS);
    }
}
