package nocomment.master.tracking;

import nocomment.master.NoComment;
import nocomment.master.db.Database;
import nocomment.master.db.Hit;
import nocomment.master.task.SingleChunkTask;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Track {

    public final WorldTrackyTracky context;
    private final int trackID;
    private ChunkPos mostRecentHit;
    private List<ChunkPos> hits = new ArrayList<>();
    private List<ChunkPos> misses = new ArrayList<>();
    private ScheduledFuture<?> updater;
    private FilterModeTransitionController transitionController;

    private AbstractFilterMode mode;

    public Track(Hit hit, WorldTrackyTracky context, OptionalInt prevTrackID) {
        this.context = context;
        this.trackID = Database.createTrack(hit, prevTrackID);
        hit(hit);
        this.transitionController = new FilterModeTransitionController(this);
    }

    public synchronized void start() {
        if (updater != null) {
            throw new IllegalStateException();
        }
        mode = transitionController.startup();
        updater = TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::update), 0, 1, TimeUnit.SECONDS);
    }

    public synchronized void hit(Hit hit) {
        hits.add(hit.pos);
        mostRecentHit = hit.pos;
        NoComment.executor.execute(() -> hit.associateWithTrack(trackID));
    }

    private synchronized void miss(ChunkPos pos) {
        misses.add(pos);
    }

    synchronized boolean includesBroadly(ChunkPos pos) {
        return mode.includesBroadly(pos);
    }

    private synchronized void update() {
        //System.out.println("Update step");
        List<ChunkPos> checksToRun = mode.updateStep(new ArrayList<>(hits), new ArrayList<>(misses));
        AbstractFilterMode newMode = transitionController.calculateTransition(new ArrayList<>(hits), new ArrayList<>(misses), checksToRun);
        if (newMode != mode) { // intentional == not .equals
            System.out.println("Mode transition from " + mode.getEnum() + " to " + newMode.getEnum());
            this.mode.decommission();
            this.mode = newMode;
            return;
        }
        hits.clear();
        misses.clear();
        if (checksToRun == null) {
            System.out.println("No mode transition, null checks. Failed.");
            failed(true);
            return;
        }
        checksToRun.forEach(this::runCheck);
    }

    void failed(boolean callUpwards) {
        System.out.println("Track " + trackID + " has FAILED");
        updater.cancel(false);
        if (callUpwards) {
            NoComment.executor.execute(() -> context.trackFailure(this));
        }
    }

    AbstractFilterMode getFilterMode() {
        return mode;
    }

    ChunkPos getMostRecentHit() {
        return mostRecentHit;
    }

    int getTrackID() {
        return trackID;
    }

    private void runCheck(ChunkPos pos) {
        NoComment.executor.execute(() ->
                context.world.submit(new SingleChunkTask(mode.getEnum().priority(), pos, this::hit, () -> miss(pos)))
        );
    }
}
