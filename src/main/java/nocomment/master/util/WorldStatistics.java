package nocomment.master.util;

import nocomment.master.task.Task;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class WorldStatistics {

    private final Map<Integer, PriorityLevelStats> stats = new HashMap<>();
    private int numSignHits;
    private int numSignMisses;

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
        }
    }

    public synchronized void hitReceived(int priority) {
        stats(priority).numTaskHits++;
    }

    public synchronized void taskCompleted(Task task) {
        PriorityLevelStats stats = stats(task.priority);
        stats.numTaskChecksCompleted += task.count;
        stats.timingData.add(stats.new TaskTimingData(task));
    }

    public synchronized void taskDispatched(int priority, int count) {
        stats(priority).numTaskChecksDispatched += count;
    }

    public synchronized void blockReceived(int priority) {
        stats(priority).numBlockHits++;
    }

    public synchronized void blockUnloaded(int priority) {
        stats(priority).numBlockMisses++;
    }

    public synchronized void signHit() {
        numSignHits++;
    }

    public synchronized void signMiss() {
        numSignMisses++;
    }
}
