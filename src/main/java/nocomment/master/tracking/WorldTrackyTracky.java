package nocomment.master.tracking;


import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.util.ChunkPos;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class WorldTrackyTracky {
    public final World world;
    public final TrackyTrackyManager parent;
    private final List<Filter> activeFilters;
    private final Consumer<ChunkPos> onLost;

    public WorldTrackyTracky(World world, TrackyTrackyManager parent, Consumer<ChunkPos> onLost) {
        this.world = world;
        this.parent = parent;
        this.activeFilters = new ArrayList<>();
        this.onLost = onLost;
    }

    public synchronized void ingestGenericKnownHit(Hit hit) { // for example, from a highway scanner
        if (Math.abs(hit.pos.x) < 100 && Math.abs(hit.pos.z) < 100) {
            return;
        }
        for (Filter filter : activeFilters) {
            if (filter.includes(hit.pos)) {
                filter.insertHit(hit);
                return;
            }
        }
        Filter filter = new Filter(hit.pos, this);
        activeFilters.add(filter);
        filter.start();
    }

    public void ingestApprox(ChunkPos pos) { // for example, if tracking was lost in another dimension
        // 11 by 11 grid pattern, spacing of 7 between each one
        // so, 121 checks
        // plus or minus 480 blocks (6*5*16) in any direction
        grid(7, 5, pos);
    }

    public synchronized void filterFailure(Filter filter) {
        activeFilters.remove(filter);
        ChunkPos last = filter.getMostRecentHit();
        System.out.println("Filter failed. Last hit at " + last + " dimension " + world.dimension);
        onLost.accept(last);
        ingestApprox(last); // one last hail mary
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
            public void hitReceived(Hit hit) {
                ingestGenericKnownHit(hit);
            }

            @Override
            public void completed() {
            }
        });
    }
}
