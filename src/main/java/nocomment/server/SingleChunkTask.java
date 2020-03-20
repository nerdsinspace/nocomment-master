package nocomment.server;

public class SingleChunkTask extends TaskHelper {
    public SingleChunkTask(int priority, ChunkPos pos, Runnable onHit, Runnable onMiss) {
        super(priority, pos, 0, 0, 1, hitPos -> onHit.run(), numHits -> {
            if (numHits == 0) {
                onMiss.run();
            }
        });
    }
}
