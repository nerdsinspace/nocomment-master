package nocomment.master.task;

import nocomment.master.db.Hit;
import nocomment.master.network.Connection;
import nocomment.master.util.ChunkPos;

public abstract class Task extends PriorityDispatchable {

    public final ChunkPos start;
    public final int directionX;
    public final int directionZ;
    public final int count;

    public long dispatchedAt;

    public Task(int priority, ChunkPos start, int directionX, int directionZ, int count) {
        super(priority);
        if (count == 0) {
            throw new IllegalArgumentException();
        }
        this.start = start;
        this.directionX = directionX;
        this.directionZ = directionZ;
        this.count = count;
    }

    public abstract void hitReceived(Hit hit);

    public abstract void completed(); // anything not hit is a miss

    public boolean interchangable(Task other) {
        return priority == other.priority && start.equals(other.start) && directionX == other.directionX && directionZ == other.directionZ && count == other.count;
    }

    @Override
    public void dispatch(Connection onto) {
        dispatchedAt = System.currentTimeMillis();
        onto.acceptTask(this);
    }
}
