package nocomment.master.task;

import nocomment.master.db.Hit;

public class CombinedTask extends Task {
    private final Task childA;
    private final Task childB;

    public CombinedTask(Task childA, Task childB) {
        super(childA.priority, childA.start, childA.directionX, childA.directionZ, childA.count);
        this.childA = childA;
        this.childB = childB;
    }

    @Override
    public void hitReceived(Hit hit) {
        childA.hitReceived(hit);
        childB.hitReceived(hit);
    }

    @Override
    public void completed() {
        childA.completed();
        childB.completed();
    }
}
