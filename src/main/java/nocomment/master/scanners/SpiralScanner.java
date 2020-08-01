package nocomment.master.scanners;

import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.util.ChunkPos;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class SpiralScanner {
    private static final int INTERVAL = 9;
    private final World world;
    private final int priority;
    private final Consumer<Hit> onHit;
    private final int maxRadius;

    private final List<Task> toSend;
    private int nextRadius;

    public SpiralScanner(World world, int priority, int distanceBlocks, Consumer<Hit> onHit) {
        this.world = world;
        this.priority = priority;
        this.onHit = onHit;
        this.maxRadius = 1 + (int) Math.ceil(distanceBlocks / 16f / INTERVAL);
        this.toSend = new ArrayList<>();
        this.nextRadius = 1;
    }

    public void submitTasks() {
        for (int i = 0; i < 10; i++) {
            // honestly this could be Literally Any Number
            // picked 10 out of a hat
            world.submit(getTask());
        }
    }

    private synchronized Task getTask() {
        if (toSend.isEmpty()) {
            makeRing(nextRadius);
            nextRadius++;
            if (nextRadius > maxRadius) {
                nextRadius = 1;
            }
        }
        return toSend.remove(toSend.size() - 1);
    }

    private void makeRing(int radius) {
        int pos = radius * INTERVAL;
        submitTask(-pos, pos, 0, -INTERVAL, 2 * radius);
        submitTask(pos, pos, -INTERVAL, 0, 2 * radius);
        submitTask(pos, -pos, 0, INTERVAL, 2 * radius);
        submitTask(-pos, -pos, INTERVAL, 0, 2 * radius);
    }

    private void submitTask(int x, int z, int dx, int dz, int c) {
        toSend.add(new Task(priority, new ChunkPos(x, z), dx, dz, c) {

            @Override
            public void hitReceived(Hit hit) {
                System.out.println("Spiral scanner hit " + hit.pos + " in dimension " + world.dimension);
                onHit.accept(hit);
            }

            @Override
            public void completed() {
                world.submit(getTask());
            }
        });
    }
}
