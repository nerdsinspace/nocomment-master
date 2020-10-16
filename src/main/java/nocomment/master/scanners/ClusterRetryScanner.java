package nocomment.master.scanners;

import nocomment.master.World;
import nocomment.master.clustering.HitRetry;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class ClusterRetryScanner {
    private final World world;
    private final int priority;
    private final Consumer<Hit> onHit;
    private final long rerunDelayMS;
    private final int volume;

    public ClusterRetryScanner(World world, int priority, int volume, long rerunDelayMS, Consumer<Hit> onHit) {
        System.out.println("Constructing cluster retry scanner with priority " + priority + " and rerun delay " + rerunDelayMS);
        this.world = world;
        this.priority = priority;
        this.onHit = onHit;
        this.rerunDelayMS = rerunDelayMS;
        this.volume = volume;
        if (rerunDelayMS <= 0) {
            throw new IllegalArgumentException();
        }
    }

    public void submitTasks() {
        for (int i = 0; i < volume; i++) {
            schedule(); // don't run the cluster traversal algorithm on the "main" thread for this connection, blocking server construction
        }
    }

    private void schedule() {
        TrackyTrackyManager.scheduler.schedule(LoggingExecutor.wrap(this::submitTask), rerunDelayMS, TimeUnit.MILLISECONDS);
    }

    private void submitTask() {
        Optional<ChunkPos> pos = HitRetry.clusterTraverse(world.server.serverID, world.dimension);
        if (!pos.isPresent()) {
            System.out.println("DBSCAN not run yet?? ClusterRetry has no results! Waiting 100x the normal duration");
            TrackyTrackyManager.scheduler.schedule(LoggingExecutor.wrap(this::submitTask), rerunDelayMS * 100, TimeUnit.MILLISECONDS);
            return;
        }
        world.submit(new Task(priority, pos.get(), 0, 0, 1) {
            @Override
            public void hitReceived(Hit hit) {
                System.out.println("Cluster retry hit " + hit.pos + " in dimension " + world.dimension);
                onHit.accept(hit);
            }

            @Override
            public void completed() {
                schedule();
            }
        });
    }
}
