package nocomment.server;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class Task implements Comparable<Task> {
    public final int priority;
    public final ChunkPos start;
    public final int directionX;
    public final int directionZ;
    public final int count;

    private static final AtomicInteger globalSeq = new AtomicInteger();
    private final int seq = globalSeq.incrementAndGet();

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

    public abstract void hitReceived(ChunkPos pos);

    public abstract void completed(); // anything not hit is a miss

    /**
     * Compare by priority, and tiebreak with seq (order of construction, earlier is lower)
     */
    @Override
    public int compareTo(Task t) {
        if (priority != t.priority) {
            return Integer.compare(priority, t.priority);
        }
        return Integer.compare(seq, t.seq);
    }
}
