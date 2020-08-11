package nocomment.master.tracking;

import nocomment.master.util.ChunkPos;

import java.util.Collections;
import java.util.List;

public class StationaryFilterMode extends AbstractFilterMode {

    public static final int INGRESS_MIN_DURATION = 30;

    private static final int INTERVAL = 8;

    private boolean awaitingResponse;
    private int hitCount;
    private int clock;
    private int totalClock;
    private int iterationsWithoutAnything;
    private final ChunkPos pos;
    private final Track parent;

    public StationaryFilterMode(ChunkPos pos, Track parent) {
        this.pos = pos;
        this.hitCount = 0;
        this.parent = parent;
    }

    @Override
    public List<ChunkPos> updateStep(List<ChunkPos> hits, List<ChunkPos> misses) {
        hits.retainAll(Collections.singleton(pos));
        misses.retainAll(Collections.singleton(pos));
        if (hits.isEmpty() && misses.isEmpty()) {
            if (iterationsWithoutAnything++ > 120) {
                System.out.println("Offine for 120 seconds, killing track");
                return null;
            }
        } else {
            iterationsWithoutAnything = 0;
        }
        if (!misses.isEmpty()) {
            return null;
        }
        totalClock++;
        hitCount += hits.size();
        if (!hits.isEmpty()) {
            awaitingResponse = false;
        }
        if (!awaitingResponse && clock-- < 0) {
            clock = Math.min(INTERVAL, hitCount); // <-- yeah, you read that right. lol
            awaitingResponse = true;
            return Collections.singletonList(pos);
        }
        return Collections.emptyList();
    }

    @Override
    public boolean includesBroadly(ChunkPos pos) {
        return MonteCarloParticleFilterMode.wouldLoadChk(pos.x, pos.z, this.pos, 9);
    }

    @Override
    public void decommission() {
        System.out.println("Stationary tracked for " + totalClock + " seconds, used " + hitCount + " checks, " + Math.round(1000 * (float) hitCount / (float) totalClock) / 1000f + " cps, dimension " + parent.context.world.dimension + ", at " + pos);
    }
}
