package nocomment.master.util;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.task.PriorityDispatchable;

import java.util.*;
import java.util.function.Consumer;

public class BlockCheckManager {
    public final World world;
    private final Map<BlockPos, BlockCheckStatus> statuses = new HashMap<>();

    public BlockCheckManager(World world) {
        this.world = world;
    }

    private synchronized BlockCheckStatus get(BlockPos pos) {
        return statuses.computeIfAbsent(pos, BlockCheckStatus::new);
    }

    public void requestBlockState(long mustBeNewerThan, BlockPos pos, int priority, Consumer<OptionalInt> onCompleted) {
        get(pos).requested(mustBeNewerThan, priority, onCompleted);
    }

    public void requestBlockState(BlockPos pos, int priority, Consumer<OptionalInt> onCompleted) {
        requestBlockState(0, pos, priority, onCompleted);
    }

    public class BlockCheckStatus {
        public final BlockPos pos;
        private int highestSubmittedPriority = Integer.MAX_VALUE;
        private final List<Consumer<OptionalInt>> listeners;
        private final List<BlockCheck> inFlight;
        private Optional<OptionalInt> reply;
        private long responseAt;

        private BlockCheckStatus(BlockPos pos) {
            this.listeners = new ArrayList<>();
            this.pos = pos;
            this.inFlight = new ArrayList<>();
            this.reply = Optional.empty();
        }

        public synchronized void requested(long mustBeNewerThan, int priority, Consumer<OptionalInt> listener) {
            if (reply.isPresent() && responseAt > mustBeNewerThan) {
                OptionalInt state = reply.get();
                NoComment.executor.execute(() -> listener.accept(state));
                return;
            }
            listeners.add(listener);
            if (priority < highestSubmittedPriority) {
                highestSubmittedPriority = priority;

                BlockCheck check = new BlockCheck(priority, this);
                inFlight.add(check);
                NoComment.executor.execute(() -> world.submit(check));
            }
        }

        public synchronized void onResponse(OptionalInt state) {
            responseAt = System.currentTimeMillis();
            reply = Optional.of(state);
            highestSubmittedPriority = Integer.MAX_VALUE; // reset
            inFlight.forEach(PriorityDispatchable::cancel); // unneeded
            inFlight.clear();
            for (Consumer<OptionalInt> listener : listeners) {
                NoComment.executor.execute(() -> listener.accept(state));
            }
            listeners.clear();
        }
    }
}
