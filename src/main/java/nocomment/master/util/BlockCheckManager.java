package nocomment.master.util;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.network.Connection;
import nocomment.master.task.PriorityDispatchable;

import java.util.*;
import java.util.function.Consumer;

public class BlockCheckManager {
    private final World world;
    private final Map<BlockPos, BlockCheckStatus> statuses = new HashMap<>();

    public BlockCheckManager(World world) {
        this.world = world;
    }

    public synchronized void requestBlockState(BlockPos pos, int priority, Consumer<OptionalInt> onCompleted) {
        BlockCheckStatus status = statuses.get(pos);
        if (status == null) {
            statuses.put(pos, new BlockCheckStatus(priority, onCompleted));
            fireCheck(priority, pos);
            return;
        }
        status.listeners.add(onCompleted);
        if (status.highestSubmittedPriority <= priority) { // remember priority queues are lowest first
            return;
        }
        status.highestSubmittedPriority = priority;
        fireCheck(priority, pos);
    }

    private void fireCheck(int priority, BlockPos pos) {
        NoComment.executor.execute(() -> world.submit(new BlockCheck(priority, pos)));
    }

    private void fireListeners(BlockPos pos, OptionalInt blockState) {
        for (Consumer<OptionalInt> consumer : onCompletion(pos)) {
            consumer.accept(blockState);
        }
    }

    private synchronized List<Consumer<OptionalInt>> onCompletion(BlockPos pos) {
        BlockCheckStatus status = statuses.remove(pos);
        if (status == null) {
            return Collections.emptyList();
        }
        return status.listeners;
    }

    private class BlockCheckStatus {
        private int highestSubmittedPriority;
        private final List<Consumer<OptionalInt>> listeners;

        private BlockCheckStatus(int initialPriority, Consumer<OptionalInt> initial) {
            this.listeners = new ArrayList<>();
            listeners.add(initial);
            this.highestSubmittedPriority = initialPriority;
        }
    }

    public class BlockCheck extends PriorityDispatchable {
        public final BlockPos pos;

        private BlockCheck(int priority, BlockPos pos) {
            super(priority);
            this.pos = pos;
        }

        public void onCompleted(OptionalInt blockState) {
            fireListeners(pos, blockState);
        }

        @Override
        public void dispatch(Connection onto) {
            onto.acceptBlockCheck(this);
        }

        @Override
        public boolean hasAffinity(Connection connection) {
            return connection.blockAffinity(pos);
        }
    }
}
