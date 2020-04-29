package nocomment.master.tracking;


import nocomment.master.World;
import nocomment.master.db.Hit;
import nocomment.master.task.Task;
import nocomment.master.task.TaskHelper;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;
import nocomment.master.util.SelfTrack;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
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
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::pairwiseFilterCheck), 0, 250, TimeUnit.MILLISECONDS);
    }

    public void pairwiseFilterCheck() {
        List<Filter> copy;
        synchronized (this) {
            copy = new ArrayList<>(activeFilters);
        }
        for (Filter A : copy) {
            ChunkPos Ahit = A.getMostRecentHit();
            if (Math.abs(Ahit.x) < 30 && Math.abs(Ahit.z) < 30) {
                System.out.println("Too close to spawn");
                // too close to spawn and the permaloaded area
                fail(A);
                return;
            }
            if (SelfTrack.tooCloseToCoolLocation(Ahit.x, Ahit.z, world.dimension)) {
                System.out.println("Too close to us " + Ahit);
                fail(A);
                return;
            }
            for (Filter B : copy) {
                if (A == B) {
                    continue;
                }
                if (Ahit.distSq(B.getMostRecentHit()) < 20L * 20L) {
                    if (A.includesBroadly(B.getMostRecentHit()) || B.includesBroadly(Ahit)) {
                        System.out.println("Too close to another filter");
                        fail(A.getTrackID() < B.getTrackID() ? B : A);
                        return;
                    }
                }
            }
        }
    }

    private void fail(Filter filter) {
        filter.failed(false);
        synchronized (this) {
            activeFilters.remove(filter);
        }
    }

    public void ingestGenericNewHit(Hit hit) { // for example, from a highway scanner
        ingestGenericKnownHit(hit, OptionalInt.empty());
    }

    public synchronized void ingestGenericKnownHit(Hit hit, OptionalInt prevTrack) {
        if (Math.abs(hit.pos.x) < 30 && Math.abs(hit.pos.z) < 30) {
            return;
        }
        if (SelfTrack.tooCloseToCoolLocation(hit.pos.x, hit.pos.z, world.dimension)) {
            System.out.println("Too close to us " + hit.pos);
            return;
        }
        for (Filter filter : activeFilters) {
            if (hit.pos.distSq(filter.getMostRecentHit()) < 50L * 50L && filter.includesBroadly(hit.pos)) {
                filter.insertHit(hit);
                return;
            }
        }
        Filter filter = new Filter(hit, this, prevTrack);
        System.out.println("Success. Starting new filter from confirmed hit at " + hit.pos + " dimension " + world.dimension + " track id " + filter.getTrackID());
        activeFilters.add(filter);
        filter.start();
    }

    public void ingestApprox(ChunkPos pos, OptionalInt prevTrack, boolean doWeCare) { // for example, if tracking was lost in another dimension
        if (doWeCare) {
            // 11 by 11 grid pattern, spacing of 7 between each one
            // so, 121 checks
            // plus or minus 560 blocks (7*5*16) in any direction
            List<Task> largerGrid = grid(10, 7, 5, pos, hit -> ingestGenericKnownHit(hit, prevTrack));
            // also, with slightly higher priority, hit the exact location (9 checks)
            grid(9, 9, 1, pos, hit -> {
                // if we get a hit in the center 9 checks, then cancel the other 121 checks
                largerGrid.forEach(Task::cancel);

                ingestGenericKnownHit(hit, prevTrack);
            });
        } else {
            // 3 by 3 grid pattern, we don't care all that much
            // 9 checks
            // plus or minus 144 blocks (9*1*16) in any direction
            grid(11, 9, 1, pos, hit -> ingestGenericKnownHit(hit, prevTrack));
        }
    }

    public synchronized void filterFailure(Filter filter) {
        activeFilters.remove(filter);
        ChunkPos last = filter.getMostRecentHit();
        System.out.println("Filter failed. Last hit at " + last + " dimension " + world.dimension);
        onLost.accept(filter);
        ingestApprox(last, OptionalInt.of(filter.getTrackID()), true); // one last hail mary
    }

    public List<Task> grid(int priority, int gridInterval, int gridRadius, ChunkPos center, Consumer<Hit> onHit) {
        List<Task> tasks = new ArrayList<>();
        for (int x = 0; x <= gridRadius; x++) {
            tasks.add(createCatchupTask(priority, center.add(-x * gridInterval, -gridRadius * gridInterval), 0, gridInterval, 2 * gridRadius + 1, onHit));
            if (x == 0) {
                continue;
            }
            tasks.add(createCatchupTask(priority, center.add(x * gridInterval, -gridRadius * gridInterval), 0, gridInterval, 2 * gridRadius + 1, onHit));
        }
        return tasks;
    }

    private Task createCatchupTask(int priority, ChunkPos center, int directionX, int directionZ, int count, Consumer<Hit> onHit) {
        return world.submitTaskUnlessAlreadyPending(new TaskHelper(priority, center, directionX, directionZ, count, onHit, i -> {}));
    }

    public synchronized boolean hasActiveFilter(int trackID) {
        return activeFilters.stream().anyMatch(filter -> filter.getTrackID() == trackID);
    }
}
