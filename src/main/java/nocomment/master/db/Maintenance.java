package nocomment.master.db;

import io.prometheus.client.Histogram;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.LoggingExecutor;

import java.util.concurrent.TimeUnit;

public final class Maintenance {

    private static final Histogram maintenanceLatencies = Histogram.build()
            .name("maintenance_latencies")
            .help("Maintenance latencies")
            .labelNames("name")
            .register();

    static void scheduleMaintenance() {
        schedule(Database::vacuum, "vacuum", 86400);
        //scheduleReindex("hits_by_track_id");
        //scheduleReindex("hits_pkey");
        scheduleReindex("tracks_pkey");
        scheduleReindex("track_endings"); // this one really needs it
        scheduleReindex("tracks_by_first");
        scheduleReindex("tracks_by_prev");
        scheduleReindex("tracks_by_last");
        scheduleReindex("player_sessions_range", 86400 * 8); // this really does not grow fast at all because most of it is from legacy which never changes
        scheduleReindex("player_sessions_by_leave");
        scheduleReindex("dbscan_pkey", 86400 * 4); // this is a big index, but the pkey to dbscan rarely grows (comparatively)
        scheduleReindex("dbscan_cluster_roots");
        scheduleReindex("dbscan_ingest", 86400 * 4); // same as dbscan_pkey
        scheduleReindex("dbscan_process");
        scheduleReindex("dbscan_disjoint_traversal"); // small index, only reindex it for query performance reasons
        scheduleReindex("dbscan_to_update_by_schedule", 3600);
        scheduleReindex("dbscan_to_update_pkey", 3600);
        //scheduleReindex("chat_by_time"); // this index does not bloat
        scheduleReindex("signs_by_loc");
        scheduleReindex("notes_server_id_dimension_x_z_key");
        /*scheduleReindex("blocks_by_loc");
        scheduleReindex("blocks_by_time");
        scheduleReindex("blocks_by_chunk");*/
        scheduleReindex("associations_cluster_id");
        scheduleReindex("associations_player_and_cluster");
        scheduleReindex("associations_player_id");
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
                        maintenanceLatencies.labels(name).time(runnable);
                        long end = System.currentTimeMillis();
                        System.out.println("Took " + (end - start) + "ms to run maintenance task " + name);
                    }
                })).start(), (long) (period * Math.random()), period, TimeUnit.SECONDS);
    }
}
