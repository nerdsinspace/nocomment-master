package nocomment.master.db;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class Hit {
    public final ChunkPos pos;
    public final short serverID;
    public final short dimension;
    public final long createdAt;
    private OptionalLong hitID;
    private OptionalInt trackID;
    private State state;
    private static final LinkedBlockingQueue<Hit> toSave = new LinkedBlockingQueue<>();
    private static final int BATCH_SIZE = 25;

    private enum State {
        PRISTINE,
        SCHEDULED,
        QUEUED,
        DONE,
    }

    public Hit(World world, ChunkPos pos) {
        this.pos = pos;
        this.serverID = world.server.serverID;
        this.dimension = world.dimension;
        this.createdAt = System.currentTimeMillis();
        this.hitID = OptionalLong.empty();
        this.trackID = OptionalInt.empty();
        this.state = State.PRISTINE;
    }

    static {
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(Hit::save), 0, 250, TimeUnit.MILLISECONDS);
    }

    private static void save() {
        if (toSave.isEmpty()) {
            return;
        }
        if (NoComment.DRY_RUN) {
            throw new IllegalStateException();
        }
        try (Connection connection = Database.getConnection()) {
            connection.setAutoCommit(false);
            List<Hit> hits = new ArrayList<>(BATCH_SIZE);
            do {
                toSave.drainTo(hits, BATCH_SIZE);
                saveRecursively(hits.iterator(), connection);
                hits.clear();
            } while (toSave.size() >= BATCH_SIZE);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private static void saveRecursively(Iterator<Hit> hits, Connection connection) throws SQLException {
        if (!hits.hasNext()) {
            connection.commit();
            Database.incrementCommitCounter("hit_batched");
            return;
        }
        Hit hit = hits.next();
        synchronized (hit) {
            if (hit.state != State.DONE) {
                Database.saveHit(hit, connection);
            }
            // Y E P
            // that's right, we HOLD ALL PAST SYNCHRONIZED LOCKS
            saveRecursively(hits, connection);
            if (hit.state != State.DONE) {
                hit.state = State.DONE;
            }
        }
    }

    public synchronized void associateWithTrack(int trackID) { // this is the hot path
        if (this.trackID.isPresent()) {
            throw new IllegalStateException(this.trackID + " " + trackID);
        }
        this.trackID = OptionalInt.of(trackID);
        if (state == State.DONE) {
            Database.alterHitToBeWithinTrack(this); // this branch is only taken for the first hit in each track
        } else {
            saveToDBAsync();
        }
    }

    public synchronized void saveToDBSoon() {
        if (state == State.PRISTINE) {
            state = State.SCHEDULED;
            TrackyTrackyManager.scheduler.schedule(LoggingExecutor.wrap(this::saveToDBAsync), 1, TimeUnit.SECONDS);
        }
    }

    synchronized void saveToDBAsync() {
        if (state == State.PRISTINE || state == State.SCHEDULED) {
            state = State.QUEUED;
            toSave.add(this);
        }
    }

    synchronized void saveToDBBlocking() { // called by createTrack to ensure we have a hitID
        if (state == State.DONE) {
            return; // already done
        }
        state = State.DONE;
        if (NoComment.DRY_RUN) {
            throw new IllegalStateException();
        }
        try (Connection connection = Database.getConnection()) {
            connection.setAutoCommit(false);
            Database.saveHit(this, connection);
            connection.commit();
            Database.incrementCommitCounter("hit_blocking");
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
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
