package nocomment.master.task;

import nocomment.master.network.Connection;

import java.util.concurrent.atomic.AtomicLong;

public abstract class PriorityDispatchable implements Comparable<PriorityDispatchable> {
    public final int priority;

    private boolean canceled;

    private static final AtomicLong globalSeq = new AtomicLong();
    private final long seq = globalSeq.incrementAndGet(); // int would overflow after like a month

    int heapPosition = -1;

    public PriorityDispatchable(int priority) {
        this.priority = priority;
    }

    public abstract void dispatch(Connection onto);

    /**
     * Compare by priority, and tiebreak with seq (order of construction, earlier is lower)
     */
    @Override
    public final int compareTo(PriorityDispatchable t) {
        if (priority != t.priority) {
            return Integer.compare(priority, t.priority);
        }
        return Long.compare(seq, t.seq);
    }

    public void cancel() {
        canceled = true;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public boolean hasAffinity(Connection connection) {
        return false;
    }

    public abstract int size();
}
