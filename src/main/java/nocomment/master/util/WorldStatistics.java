package nocomment.master.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import nocomment.master.World;
import nocomment.master.task.Task;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class WorldStatistics {

    private static final Counter taskOutcomes = Counter.build()
            .name("task_outcomes_total")
            .help("Outcomes of task based chunk checks")
            .labelNames("dimension", "priority", "outcome")
            .register();

    private static final Counter taskDispatches = Counter.build()
            .name("task_dispatches_total")
            .help("Dispatches of task based chunk checks")
            .labelNames("dimension", "priority")
            .register();

    private static final Histogram taskLatencies = Histogram.build()
            .name("task_latencies")
            .help("Task latencies")
            .buckets(0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.5, 0.75, 1, 2, 5, 10)
            .labelNames("dimension", "priority")
            .register();

    private static final Counter blockOutcomes = Counter.build()
            .name("block_outcomes_total")
            .help("Outcomes of block checks")
            .labelNames("dimension", "priority", "outcome")
            .register();

    private final World world;
    private final Map<Integer, PriorityLevelStats> stats = new HashMap<>();
    private int numSignHits;
    private int numSignMisses;

    public WorldStatistics(World world) {
        this.world = world;
    }

    private PriorityLevelStats stats(int priority) {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException();
        }
        return stats.computeIfAbsent(priority, x -> new PriorityLevelStats());
    }

    public synchronized String report() {
        StringBuilder resp = new StringBuilder();
        if (numSignMisses + numSignHits > 0) {
            resp
                    .append("\nSign checks: ")
                    .append(numSignHits + numSignMisses)
                    .append(", sign hits: ")
                    .append(numSignHits)
                    .append(", sign misses: ")
                    .append(numSignMisses)
                    .append(", sign hit rate: ")
                    .append(formatPercent(numSignHits, numSignHits + numSignMisses));
        } else {
            resp.append("\nNo sign checks");
        }
        stats.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey)).forEach(entry -> {
            PriorityLevelStats stats = entry.getValue();
            if (stats.numTaskChecksCompleted == 0 && stats.numBlockHits == 0) {
                // nothing yet
                return;
            }
            resp
                    .append("\n")
                    .append("Priority ")
                    .append(entry.getKey())
                    .append(": ");
            if (stats.numTaskChecksCompleted != 0) {
                resp
                        .append("\n Chunk hit rate: ")
                        .append(formatPercent(stats.numTaskHits, stats.numTaskChecksCompleted))
                        .append("\n Chunk checks: ")
                        .append(stats.numTaskChecksCompleted)
                        .append("\n Chunk hits: ")
                        .append(stats.numTaskHits)
                        .append("\n Chunk misses: ")
                        .append(stats.numTaskChecksCompleted - stats.numTaskHits)
                        .append("\n Chunk dispatched: ")
                        .append(stats.numTaskChecksDispatched)
                        .append("\n In flight: ")
                        .append(stats.numTaskChecksDispatched - stats.numTaskChecksCompleted);
                if (!stats.timingData.isEmpty()) {
                    long mostRecentCompletion = stats.timingData.stream().mapToLong(data -> data.completedAt).max().getAsLong();
                    long age = System.currentTimeMillis() - mostRecentCompletion;
                    resp
                            .append("\n Most recent completion was ")
                            .append(age)
                            .append("ms ago");
                    long avg = Math.round(stats.timingData.stream().mapToLong(data -> data.completedAt - data.dispatchedAt).average().getAsDouble());
                    resp
                            .append("\n Average round trip duration was ")
                            .append(avg)
                            .append("ms");
                    long rem = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5); // 5 minutes
                    stats.timingData.stream()
                            .filter(td -> td.completedAt > rem)
                            .findFirst()
                            .map(task -> stats.timingData.indexOf(task))
                            .map(ind -> stats.timingData.subList(0, ind))
                            .ifPresent(List::clear);
                } else {
                    resp.append("\n No completions since last report");
                }

            }
            if (stats.numBlockHits != 0) {
                resp
                        .append("\n Block hit rate: ")
                        .append(formatPercent(stats.numBlockHits, stats.numBlockHits + stats.numBlockMisses))
                        .append("\n Block checks: ")
                        .append(stats.numBlockHits + stats.numBlockMisses)
                        .append("\n Block hits: ")
                        .append(stats.numBlockHits)
                        .append("\n Block misses: ")
                        .append(stats.numBlockMisses);

            }
        });
        return resp.toString();
    }

    private static String formatPercent(int num, int denom) {
        return Math.round(1000 * (float) num / (float) denom) / 10f + "%";
    }

    private class PriorityLevelStats {

        private int numTaskChecksDispatched;
        private int numTaskChecksCompleted;
        private int numTaskHits;
        private int numBlockMisses;
        private int numBlockHits;
        private List<TaskTimingData> timingData = new ArrayList<>();

        private class TaskTimingData {

            private TaskTimingData(Task task) {
                this.dispatchedAt = task.dispatchedAt;
                this.completedAt = System.currentTimeMillis();
            }

            private long dispatchedAt;
            private long completedAt;

            private double elapsedSeconds() {
                return (completedAt - dispatchedAt) / 1000d;
            }
        }
    }

    public synchronized void hitReceived(int priority) {
        stats(priority).numTaskHits++;

        taskOutcomes.labels(world.dim(), priority + "", "hit").inc();
    }

    public synchronized void taskCompleted(Task task) {
        PriorityLevelStats stats = stats(task.priority);
        stats.numTaskChecksCompleted += task.count;
        PriorityLevelStats.TaskTimingData data = stats.new TaskTimingData(task);
        stats.timingData.add(data);

        taskOutcomes.labels(world.dim(), task.priority + "", "done").inc(task.count);
        taskLatencies.labels(world.dim(), task.priority + "").observe(data.elapsedSeconds());
    }

    public synchronized void taskDispatched(int priority, int count) {
        stats(priority).numTaskChecksDispatched += count;

        taskDispatches.labels(world.dim(), priority + "").inc(count);
    }

    public synchronized void blockReceived(int priority) {
        stats(priority).numBlockHits++;

        blockOutcomes.labels(world.dim(), priority + "", "hit").inc();
    }

    public synchronized void blockUnloaded(int priority) {
        stats(priority).numBlockMisses++;

        blockOutcomes.labels(world.dim(), priority + "", "miss").inc();
    }

    public synchronized void signHit() {
        numSignHits++;
    }

    public synchronized void signMiss() {
        numSignMisses++;
    }
}
