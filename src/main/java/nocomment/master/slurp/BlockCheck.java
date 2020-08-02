package nocomment.master.slurp;

import io.prometheus.client.Counter;
import nocomment.master.network.Connection;
import nocomment.master.task.PriorityDispatchable;
import nocomment.master.util.BlockPos;

import java.util.OptionalInt;

public final class BlockCheck extends PriorityDispatchable {
    private static final Counter blockCheckCancellations = Counter.build()
            .name("block_check_cancellations_total")
            .help("Number of block check cancellations")
            .labelNames("priority")
            .register();
    private static final Counter blockCheckCancelQueries = Counter.build()
            .name("block_check_cancel_queries_total")
            .help("Number of times a block check cancellation has been queried")
            .labelNames("priority", "outcome")
            .register();
    private final BlockCheckManager.BlockCheckStatus parent;

    BlockCheck(int priority, BlockCheckManager.BlockCheckStatus parent) {
        super(priority);
        this.parent = parent;
    }

    public void onCompleted(OptionalInt blockState) { // this can be called from any thread at any time
        if (!super.isCanceled()) {
            parent.onResponse(blockState);
        }
    }

    @Override
    public boolean isCanceled() {
        blockCheckCancelQueries.labels(priority + "", super.isCanceled() + "").inc();
        return super.isCanceled();
    }

    @Override
    public void cancel() {
        blockCheckCancellations.labels(priority + "").inc();
        super.cancel();
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
