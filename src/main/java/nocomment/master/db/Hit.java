package nocomment.master.db;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.util.ChunkPos;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class Hit {
    public final ChunkPos pos;
    public final short serverID;
    public final short dimension;
    public final long createdAt;
    private Future<Long> hitID;

    public Hit(World world, ChunkPos pos) {
        this.pos = pos;
        this.serverID = world.server.serverID;
        this.dimension = world.dimension;
        this.createdAt = System.currentTimeMillis();
    }

    public synchronized void saveToDBAsync() {
        if (hitID != null) {
            throw new IllegalStateException();
        }
        CompletableFuture<Long> future = new CompletableFuture<>();
        hitID = future;
        NoComment.executor.execute(() -> Database.saveHit(this, future));
    }

    public Future<Long> getHitID() {
        return hitID;
    }
}
