package nocomment.master.scanners;

import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class RingScanner {
    private static final int INTERVAL = 9;
    private final World world;
    private final int priority;
    private final Consumer<Hit> onHit;
    private final long rerunDelayMS;
    private final int radius;

    public RingScanner(World world, int priority, int distanceBlocks, long rerunDelayMS, Consumer<Hit> onHit) {
        System.out.println("Constructing ring scanner with priority " + priority + " block distance " + distanceBlocks + " and rerun delay " + rerunDelayMS);
        this.world = world;
        this.priority = priority;
        this.onHit = onHit;
        this.rerunDelayMS = rerunDelayMS;
        this.radius = 1 + (int) Math.ceil(distanceBlocks / 16f / INTERVAL);
        int tot = 4 * (2 * radius + 1);
        System.out.println("Total count: " + tot);
        System.out.println("Estimated time in seconds: " + tot / 160.0);
        System.out.println("Fraction of time: " + tot / 160.0 / (rerunDelayMS / 1000.0));
    }

    public void submitTasks() {
        int pos = radius * INTERVAL;
        submitTask(-pos, -pos, INTERVAL, 0, 2 * radius);
        submitTask(pos, -pos, 0, INTERVAL, 2 * radius);
        submitTask(pos, pos, -INTERVAL, 0, 2 * radius);
        submitTask(-pos, pos, 0, -INTERVAL, 2 * radius);
    }

    private void submitTask(int startX, int startZ, int directionX, int directionZ, int count) {
        world.submit(new Task(priority, new ChunkPos(startX, startZ), directionX, directionZ, count) {

            private void resubmit() {
                // we cannot just call submitTask(this) because our task seq is low, which would make highway scanning stay on one highway since it's always the lowest seq
                // instead, cycle through all highways as they complete, round robin
                submitTask(startX, startZ, directionX, directionZ, count);
                // INCORRECT OLD CODE: HighwayScanner.this.world.submitTask(this);
            }

            @Override
            public void hitReceived(Hit hit) {
                System.out.println("Ring scanner hit " + hit.pos + " in dimension " + world.dimension);
                onHit.accept(hit);
            }

            @Override
            public void completed() {
                if (rerunDelayMS >= 0) {
                    TrackyTrackyManager.scheduler.schedule(LoggingExecutor.wrap(this::resubmit), rerunDelayMS, TimeUnit.MILLISECONDS);
                }
            }
        });
    }
}
