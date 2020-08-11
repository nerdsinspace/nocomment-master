package nocomment.master.tracking;

import nocomment.master.util.ChunkPos;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class FilterModeTransitionController {

    private final Track parent;
    private final FilterRetTrack hist;

    public FilterModeTransitionController(Track parent) {
        this.parent = parent;
        this.hist = new FilterRetTrack();
    }

    public AbstractFilterMode calculateTransition(List<ChunkPos> hits, List<ChunkPos> misses, List<ChunkPos> newGuesses) {
        hist.timestep(hits, misses);
        AbstractFilterMode curr = parent.getFilterMode();
        FilterModeEnum currMode = curr.getEnum();
        switch (currMode) {
            case STATIONARY_FILTER: {
                // the only transition for stationary filter is if it misses
                if (newGuesses == null && !misses.isEmpty()) {
                    // most recent hit is guaranteed to be the stationary chunk check location
                    return swap(FilterModeEnum.MONTE_CARLO_PARTICLE_FILTER);
                }
                break;
            }
            case MONTE_CARLO_PARTICLE_FILTER: {
                if (hist.hitHistory.size() < StationaryFilterMode.INGRESS_MIN_DURATION) {
                    break;
                }
                if (hist.hitHistory.subList(0, StationaryFilterMode.INGRESS_MIN_DURATION).stream().anyMatch(Collection::isEmpty)) {
                    break;
                }
                for (ChunkPos candidate : hits) {
                    if (allHitsWithin(StationaryFilterMode.INGRESS_MIN_DURATION, candidate)) {
                        System.out.println("Track " + parent.getTrackID() + " switching to stationary mode at " + candidate);
                        return new StationaryFilterMode(candidate, parent);
                    }
                }
                break;
            }
        }
        return curr;
    }

    private boolean allHitsWithin(int range, ChunkPos center) {
        // intentionally 5 not 4 so that we grab ppl who are walking around in just a small area
        return hist.hitHistory.subList(0, range).stream().flatMap(Collection::stream).allMatch(pos -> MonteCarloParticleFilterMode.wouldLoadChk(center.x, center.z, pos, 5));
    }

    private AbstractFilterMode swap(FilterModeEnum mode) {
        return mode.constructor.apply(parent.getMostRecentHit(), parent);
    }

    public AbstractFilterMode startup() {
        return swap(FilterModeEnum.MONTE_CARLO_PARTICLE_FILTER);
    }

    private static class FilterRetTrack {

        private static final int HIST_SIZE = 60;

        LinkedList<List<ChunkPos>> hitHistory = new LinkedList<>();
        LinkedList<List<ChunkPos>> missHistory = new LinkedList<>();

        private void timestep(List<ChunkPos> hits, List<ChunkPos> misses) {
            add(hitHistory, hits);
            add(missHistory, misses);
        }

        private static void add(LinkedList<List<ChunkPos>> dest, List<ChunkPos> data) {
            dest.addFirst(data);
            while (dest.size() > HIST_SIZE) {
                dest.removeLast();
            }
        }
    }
}
