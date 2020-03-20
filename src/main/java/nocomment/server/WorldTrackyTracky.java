package nocomment.server;

import java.util.concurrent.TimeUnit;

public class WorldTrackyTracky {
    public final World world;
    public final TrackyTrackyManager parent;

    public WorldTrackyTracky(World world, TrackyTrackyManager parent) {
        this.world = world;
        this.parent = parent;
    }

    public void ingestGeneric(ChunkPos hit) {
        System.out.println("Tracky tracky " + hit);
        if (Math.abs(hit.x) < 100 && Math.abs(hit.z) < 100) {
            return;
        }
        // TODO deduplicate with hits in the last 5 secs so we dont 2x whenever they have 9+epsilon chunks loaded in a row
        TrackyTrackyManager.scheduler.schedule(() -> {
            System.out.println("Gridding");
            grid(9, 2, hit);
        }, 5, TimeUnit.SECONDS);
    }

    private void grid(int gridInterval, int gridRadius, ChunkPos center) {
        for (int x = -gridRadius; x <= gridRadius; x++) { // iterate X, sweep Z
            // i'm sorry
            createCatchupTask(10, center.add(x * gridInterval, -gridRadius * gridInterval), 0, gridInterval, 2 * gridRadius + 1);
        }
    }

    private void createCatchupTask(int priority, ChunkPos center, int directionX, int directionZ, int count) {
        world.submitTask(new Task(priority, center, directionX, directionZ, count) {
            @Override
            public void hitReceived(ChunkPos pos) {
                ingestGeneric(pos);
            }

            @Override
            public void completed() {
            }
        });
    }

}
