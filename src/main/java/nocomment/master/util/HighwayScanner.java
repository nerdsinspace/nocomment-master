package nocomment.master.util;

import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class HighwayScanner {
    private static final int AXIS_INTERVAL = 9;
    private static final int DIAG_INTERVAL = 7; // overlap because otherwise there's a diagonal catty corner

    private static final int OW_WB = 1_000_000; // TEMP: only scan 1/300th of the way to the WB lol
    private static final int AXIS_COUNT = 1 + (int) Math.ceil(OW_WB / 16f / 8f / AXIS_INTERVAL);
    private static final int DIAG_COUNT = 1 + (int) Math.ceil(OW_WB / 16f / 8f / AXIS_INTERVAL);
    private static final ChunkPos SPAWN = new ChunkPos(0, 0);

    private final World world;
    private final int priority;
    private final Consumer<Hit> onHit;
    private final long rerunDelayMS;

    public HighwayScanner(World world, int priority, Consumer<Hit> onHit) {
        this.world = world;
        this.priority = priority;
        this.onHit = onHit;
        this.rerunDelayMS = -1;
    }

    public void submitTasks() {
        createInitialDetectionTask(AXIS_INTERVAL, 0, AXIS_COUNT);
        createInitialDetectionTask(-AXIS_INTERVAL, 0, AXIS_COUNT);
        createInitialDetectionTask(0, AXIS_INTERVAL, AXIS_COUNT);
        createInitialDetectionTask(0, -AXIS_INTERVAL, AXIS_COUNT);

        createInitialDetectionTask(DIAG_INTERVAL, DIAG_INTERVAL, DIAG_COUNT);
        createInitialDetectionTask(DIAG_INTERVAL, -DIAG_INTERVAL, DIAG_COUNT);
        createInitialDetectionTask(-DIAG_INTERVAL, DIAG_INTERVAL, DIAG_COUNT);
        createInitialDetectionTask(-DIAG_INTERVAL, -DIAG_INTERVAL, DIAG_COUNT);
    }

    private void createInitialDetectionTask(int directionX, int directionZ, int count) {
        world.submitTask(new Task(priority, SPAWN, directionX, directionZ, count) {

            private void resubmit() {
                HighwayScanner.this.world.submitTask(this);
            }

            @Override
            public void hitReceived(Hit hit) {
                System.out.println("Highway scanner hit " + hit.pos);
                onHit.accept(hit);
            }

            @Override
            public void completed() {
                if (rerunDelayMS >= 0) {
                    TrackyTrackyManager.scheduler.schedule(this::resubmit, rerunDelayMS, TimeUnit.MILLISECONDS);
                }
            }
        });
    }
}
