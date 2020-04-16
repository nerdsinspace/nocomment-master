package nocomment.master.task;

import nocomment.master.db.Hit;
import nocomment.master.util.ChunkPos;

import java.util.concurrent.atomic.AtomicLong;

public abstract class Task implements Comparable<Task> {
    public final int priority;
    public final ChunkPos start;
    public final int directionX;
    public final int directionZ;
    public final int count;

    private boolean canceled;

    private static final AtomicLong globalSeq = new AtomicLong();
    private final long seq = globalSeq.incrementAndGet(); // int would overflow after like a month

    public Task(int priority, ChunkPos start, int directionX, int directionZ, int count) {
        if (count == 0) {
            throw new IllegalArgumentException();
        }
        this.priority = priority;
        this.start = start;
        this.directionX = directionX;
        this.directionZ = directionZ;
        this.count = count;
    }

    public abstract void hitReceived(Hit hit);

    public abstract void completed(); // anything not hit is a miss

    /**
     * Compare by priority, and tiebreak with seq (order of construction, earlier is lower)
     */
    @Override
    public int compareTo(Task t) {
        if (priority != t.priority) {
            return Integer.compare(priority, t.priority);
        }
        return Long.compare(seq, t.seq);
    }

    public boolean interchangable(Task other) {
        return priority == other.priority && start.equals(other.start) && directionX == other.directionX && directionZ == other.directionZ && count == other.count;
    }

    public void cancel() {
        canceled = true;
    }

    public boolean isCanceled() {
        return canceled;
    }
}
