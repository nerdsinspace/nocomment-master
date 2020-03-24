package nocomment.master.task;

import nocomment.master.db.Hit;
import nocomment.master.util.ChunkPos;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class TaskHelper extends Task {
    private final Consumer<Hit> onHit;
    private final Consumer<Integer> onCompletion;
    private final Set<ChunkPos> hitsReceived;

    public TaskHelper(int priority, ChunkPos start, int directionX, int directionZ, int count, Consumer<Hit> onHit, Consumer<Integer> onCompletion) {
        super(priority, start, directionX, directionZ, count);
        this.onHit = onHit;
        this.onCompletion = onCompletion;
        this.hitsReceived = new HashSet<>();
    }

    @Override
    public void hitReceived(Hit hit) {
        if (hitsReceived.add(hit.pos)) {
            onHit.accept(hit);
        }
    }

    @Override
    public void completed() {
        onCompletion.accept(hitsReceived.size());
    }
}
