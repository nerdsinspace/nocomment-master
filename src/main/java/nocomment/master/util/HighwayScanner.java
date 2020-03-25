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

    private static final int OW_WB = 2_500_000; // TEMP: only scan 1/300th of the way to the WB lol
    private static final int AXIS_COUNT = 1 + (int) Math.ceil(OW_WB / 16f / 8f / AXIS_INTERVAL);
    private static final int DIAG_COUNT = 1 + (int) Math.ceil(OW_WB / 16f / 8f / AXIS_INTERVAL);

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
        int intervalSize = 100;
        for (int start = 0; start < AXIS_COUNT || start < DIAG_COUNT; start += intervalSize) {
            submitAxisTasks(start, start + intervalSize);
            submitDiagTasks(start, start + intervalSize);
        }
    }

    public void submitAxisTasks(int startIndex, int endIndex) {
        if (endIndex >= AXIS_COUNT) {
            endIndex = AXIS_COUNT - 1;
        }
        runSection(AXIS_INTERVAL, 0, startIndex, endIndex);
        runSection(-AXIS_INTERVAL, 0, startIndex, endIndex);
        runSection(0, AXIS_INTERVAL, startIndex, endIndex);
        runSection(0, -AXIS_INTERVAL, startIndex, endIndex);
    }

    public void submitDiagTasks(int startIndex, int endIndex) {
        if (endIndex >= DIAG_COUNT) {
            endIndex = DIAG_COUNT - 1;
        }
        runSection(DIAG_INTERVAL, DIAG_INTERVAL, startIndex, endIndex);
        runSection(DIAG_INTERVAL, -DIAG_INTERVAL, startIndex, endIndex);
        runSection(-DIAG_INTERVAL, DIAG_INTERVAL, startIndex, endIndex);
        runSection(-DIAG_INTERVAL, -DIAG_INTERVAL, startIndex, endIndex);
    }

    private void runSection(int directionX, int directionZ, int startIndex, int endIndex) {
        int count = endIndex - startIndex;
        if (count <= 0) {
            return;
        }
        submitTask(directionX * startIndex, directionZ * startIndex, directionX, directionZ, count);
    }

    private void submitTask(int startX, int startZ, int directionX, int directionZ, int count) {
        world.submitTask(new Task(priority, new ChunkPos(startX, startZ), directionX, directionZ, count) {

            private void resubmit() {
                // we cannot just call submitTask(this) because our task seq is low, which would make highway scanning stay on one highway since it's always the lowest seq
                // instead, cycle through all highways as they complete, round robin
                submitTask(startX, startZ, directionX, directionZ, count);
                // INCORRECT OLD CODE: HighwayScanner.this.world.submitTask(this);
            }

            @Override
            public void hitReceived(Hit hit) {
                System.out.println("Highway scanner hit " + hit.pos);
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
