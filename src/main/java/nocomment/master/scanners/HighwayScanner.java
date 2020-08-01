package nocomment.master.scanners;

import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class HighwayScanner {

    private static final int AXIS_INTERVAL = 9;
    private static final int DIAG_INTERVAL = 7; // overlap because otherwise there's a diagonal catty corner

    private final World world;
    private final int priority;
    private final Consumer<Hit> onHit;
    private final long rerunDelayMS;
    private final int axisCount;
    private final int diagCount;

    public HighwayScanner(World world, int priority, int distanceBlocks, long rerunDelayMS, Consumer<Hit> onHit) {
        System.out.println("Constructing highway scanner with priority " + priority + " block distance " + distanceBlocks + " and rerun delay " + rerunDelayMS);
        this.world = world;
        this.priority = priority;
        this.onHit = onHit;
        this.rerunDelayMS = rerunDelayMS;
        this.axisCount = 1 + (int) Math.ceil(distanceBlocks / 16f / AXIS_INTERVAL);
        this.diagCount = 1 + (int) Math.ceil(distanceBlocks / 16f / DIAG_INTERVAL);
        System.out.println("Axis count: " + axisCount);
        System.out.println("Diag count: " + diagCount);
        int tot = axisCount * 4 + diagCount * 4;
        System.out.println("Total count: " + tot);
        System.out.println("Estimated time in seconds: " + tot / 160.0);
        System.out.println("Fraction of time: " + tot / 160.0 / (rerunDelayMS / 1000.0));
    }

    public void submitTasks() {
        int intervalSize = 100;
        for (int start = 0; start < axisCount || start < diagCount; start += intervalSize) {
            submitAxisTasks(start, start + intervalSize);
            submitDiagTasks(start, start + intervalSize);
        }
    }

    public void submitAxisTasks(int startIndex, int endIndex) {
        if (endIndex >= axisCount) {
            endIndex = axisCount - 1;
        }
        runSection(AXIS_INTERVAL, 0, startIndex, endIndex);
        runSection(-AXIS_INTERVAL, 0, startIndex, endIndex);
        runSection(0, AXIS_INTERVAL, startIndex, endIndex);
        runSection(0, -AXIS_INTERVAL, startIndex, endIndex);
    }

    public void submitDiagTasks(int startIndex, int endIndex) {
        if (endIndex >= diagCount) {
            endIndex = diagCount - 1;
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
        world.submit(new Task(priority, new ChunkPos(startX, startZ), directionX, directionZ, count) {

            private void resubmit() {
                // we cannot just call submitTask(this) because our task seq is low, which would make highway scanning stay on one highway since it's always the lowest seq
                // instead, cycle through all highways as they complete, round robin
                submitTask(startX, startZ, directionX, directionZ, count);
                // INCORRECT OLD CODE: HighwayScanner.this.world.submitTask(this);
            }

            @Override
            public void hitReceived(Hit hit) {
                System.out.println("Highway scanner hit " + hit.pos + " in dimension " + world.dimension);
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
