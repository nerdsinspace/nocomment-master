package nocomment.master.slurp;

import nocomment.master.network.Connection;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.util.BlockPos;

import java.util.OptionalInt;

public final class BlockCheck extends PriorityDispatchable {
    private final BlockCheckManager.BlockCheckStatus parent;

    BlockCheck(int priority, BlockCheckManager.BlockCheckStatus parent) {
        super(priority);
        this.parent = parent;
    }

    public void onCompleted(OptionalInt blockState) { // this can be called from any thread at any time
        if (!isCanceled()) {
            parent.onResponse(blockState);
        }
    }

    public BlockPos pos() {
        return this.parent.pos();
    }

    public long bpos() {
        return this.parent.bpos;
    }

    @Override
    public void dispatch(Connection onto) {
        onto.acceptBlockCheck(this);
    }

    @Override
    public boolean hasAffinity(Connection connection) {
        return connection.blockAffinity(this.bpos());
    }
}
