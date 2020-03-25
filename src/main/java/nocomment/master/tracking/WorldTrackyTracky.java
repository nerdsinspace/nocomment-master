package nocomment.master.tracking;


import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.TaskHelper;
import nocomment.master.util.ChunkPos;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Consumer;

public class WorldTrackyTracky {
    public final World world;
    public final TrackyTrackyManager parent;
    private final List<Filter> activeFilters;
    private final Consumer<Filter> onLost;

    public WorldTrackyTracky(World world, TrackyTrackyManager parent, Consumer<Filter> onLost) {
        this.world = world;
        this.parent = parent;
        this.activeFilters = new ArrayList<>();
        this.onLost = onLost;
    }

    public synchronized void ingestGenericKnownHit(Hit hit, OptionalLong prevTrack) { // for example, from a highway scanner
        if (Math.abs(hit.pos.x) < 30 && Math.abs(hit.pos.z) < 30) {
            return;
        }
        for (Filter filter : activeFilters) {
            if (filter.includes(hit.pos)) {
                filter.insertHit(hit);
                return;
            }
        }
        Filter filter = new Filter(hit, this, prevTrack);
        activeFilters.add(filter);
        filter.start();
    }

    public void ingestApprox(ChunkPos pos, OptionalLong prevTrack) { // for example, if tracking was lost in another dimension
        // 11 by 11 grid pattern, spacing of 7 between each one
        // so, 121 checks
        // plus or minus 480 blocks (6*5*16) in any direction
        grid(7, 5, pos, hit -> ingestGenericKnownHit(hit, prevTrack));
    }

    public synchronized void filterFailure(Filter filter) {
        activeFilters.remove(filter);
        ChunkPos last = filter.getMostRecentHit();
        System.out.println("Filter failed. Last hit at " + last + " dimension " + world.dimension);
        onLost.accept(filter);
        ingestApprox(last, OptionalLong.of(filter.getTrackID())); // one last hail mary
    }

    private void grid(int gridInterval, int gridRadius, ChunkPos center, Consumer<Hit> onHit) {
        for (int x = -gridRadius; x <= gridRadius; x++) { // iterate X, sweep Z
            // i'm sorry
            createCatchupTask(10, center.add(x * gridInterval, -gridRadius * gridInterval), 0, gridInterval, 2 * gridRadius + 1, onHit);
        }
    }

    private void createCatchupTask(int priority, ChunkPos center, int directionX, int directionZ, int count, Consumer<Hit> onHit) {
        world.submitTask(new TaskHelper(priority, center, directionX, directionZ, count, onHit, i -> {}));
    }

    public synchronized boolean hasActiveFilter(long trackID) {
        return activeFilters.stream().anyMatch(filter -> filter.getTrackID() == trackID);
    }
}
