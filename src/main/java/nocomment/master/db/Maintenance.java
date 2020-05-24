package nocomment.master.db;

import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.LoggingExecutor;

import java.util.concurrent.TimeUnit;

public class Maintenance {
    static void scheduleMaintenance() {
        schedule(Database::vacuum, "vacuum", 86400);
        scheduleReindex("hits_by_track_id");
        scheduleReindex("hits_pkey");
        scheduleReindex("tracks_pkey");
        scheduleReindex("track_endings");
        //scheduleReindex("player_sessions_range");
        scheduleReindex("player_sessions_by_leave");
        scheduleReindex("dbscan_pkey");
        scheduleReindex("dbscan_cluster_roots");
        scheduleReindex("dbscan_ingest");
        scheduleReindex("dbscan_process");
        scheduleReindex("dbscan_disjoint_traversal");
        scheduleReindex("dbscan_to_update_pkey", 3600);
    }

    private static void scheduleReindex(String indexName) {
        scheduleReindex(indexName, 86400);
    }

    private static void scheduleReindex(String indexName, int period) {
        schedule(() -> Database.reindex(indexName), "reindex " + indexName, period);
    }

    // a reindex on hits_time_and_place cut it down from 300mb to 120mb, so this is actually useful and not just a meme
    private static void schedule(Runnable runnable, String name, int period) {
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(() ->
                new Thread(LoggingExecutor.wrap(() -> { // dont clog up scheduler's fixed thread pool
                    synchronized (Maintenance.class) { // dont run more than one maintenance at a time, even at random. it causes database deadlock
                        long start = System.currentTimeMillis();
                        runnable.run();
                        long end = System.currentTimeMillis();
                        System.out.println("Took " + (end - start) + "ms to run maintenance task " + name);
                    }
                })).start(), (long) (period * Math.random()), period, TimeUnit.SECONDS);
    }
}
