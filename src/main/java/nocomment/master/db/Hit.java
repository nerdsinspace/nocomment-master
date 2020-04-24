package nocomment.master.db;

import nocomment.master.World;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public class Hit {
    public final ChunkPos pos;
    public final short serverID;
    public final short dimension;
    public final long createdAt;
    private OptionalLong hitID;
    private OptionalInt trackID;

    private boolean savedToHitsTable;

    public Hit(World world, ChunkPos pos) {
        this.pos = pos;
        this.serverID = world.server.serverID;
        this.dimension = world.dimension;
        this.createdAt = System.currentTimeMillis();
        this.hitID = OptionalLong.empty();
        this.trackID = OptionalInt.empty();
    }

    public synchronized void associateWithTrack(int trackID) {
        if (this.trackID.isPresent()) {
            throw new IllegalStateException(this.trackID + " " + trackID);
        }
        this.trackID = OptionalInt.of(trackID);
        if (savedToHitsTable) {
            System.out.println("Adding a hit to a track after the fact");
            Database.addHitToTrack(this);
        } else {
            saveToDB();
        }
    }

    public void saveToDBSoon() {
        TrackyTrackyManager.scheduler.schedule(LoggingExecutor.wrap(this::saveToDB), 1, TimeUnit.SECONDS);
    }

    synchronized void saveToDB() { // call this to ensure we have a hitID
        if (savedToHitsTable) {
            return; // already done
        }
        savedToHitsTable = true;
        Database.saveHit(this);
    }

    void setHitID(long hitID) {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException();
        }
        if (this.hitID.isPresent()) {
            throw new IllegalStateException(this.hitID + " " + hitID);
        }
        this.hitID = OptionalLong.of(hitID);
    }

    OptionalInt getTrackID() {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException();
        }
        return trackID;
    }

    synchronized long getHitID() {
        if (!hitID.isPresent()) {
            throw new IllegalStateException();
        }
        return hitID.getAsLong();
    }
}
