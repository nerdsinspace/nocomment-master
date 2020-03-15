package nocomment.server;

public abstract class Task {
    public final int priority;
    public final ChunkPos start;
    public final int directionX;
    public final int directionZ;
    public final int count;

    public Task(int priority, ChunkPos start, int directionX, int directionZ, int count) {
        this.priority = priority;
        this.start = start;
        this.directionX = directionX;
        this.directionZ = directionZ;
        this.count = count;
    }

    public abstract void hitReceived(ChunkPos pos);

    public abstract void completed(); // anything not hit is a miss
}
