package nocomment.master;

import nocomment.master.db.Database;
import nocomment.master.network.NoCommentServer;
import nocomment.master.util.LoggingExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NoComment {

    public static Executor executor = new LoggingExecutor(Executors.newFixedThreadPool(48));
    public static final boolean DRY_RUN = false;

    public static void main(String[] args) throws Exception {
        simulator();
        if (!DRY_RUN) {
            new Database();
            NoCommentServer.listen();
        }
    }

    public static void simulator() {
        double[] joinAt = {0, 4.5, 0};
        double ql = 1.5;
        List<String> events = new ArrayList<>();
        for (int step = 0; step < 100; step++) {
            double[] leaveAt = new double[joinAt.length];
            for (int i = 0; i < leaveAt.length; i++) {
                leaveAt[i] = joinAt[i] + 6;
            }
            List<Integer> indices = IntStream.range(0, leaveAt.length).boxed().sorted(Comparator.comparingDouble(x -> -leaveAt[x])).collect(Collectors.toList());
            for (int i = 1; i < leaveAt.length; i++) {
                leaveAt[indices.get(i)] = Math.min(leaveAt[indices.get(i - 1)] - 2, leaveAt[indices.get(i)]);
            }
            System.out.println("Join leave " + Arrays.toString(joinAt) + " " + Arrays.toString(leaveAt));
            double[] newStep = step(joinAt, leaveAt, ql);
            for (int i = 0; i < leaveAt.length; i++) {
                if (newStep[i] != joinAt[i]) {
                    events.add(joinAt[i] + ": " + i + " joins");
                    events.add(leaveAt[i] + ": " + i + " leaves");
                    System.out.println(i + " online from " + joinAt[i] + " to " + leaveAt[i] + " then queued until " + newStep[i]);
                }
            }
            joinAt = newStep;
        }
        events.stream().sorted(Comparator.comparingDouble(l -> Double.parseDouble(l.split(":")[0]))).forEach(System.out::println);
    }

    public static double[] step(double[] joinAt, double[] leaveAt, double ql) {
        leaveAt = Arrays.copyOf(leaveAt, leaveAt.length);
        double leaveStart = leaveAt[0];
        for (double leave : leaveAt) {
            leaveStart = Math.min(leaveStart, leave);
        }
        // leaveStart = first leave
        double[] newStep = Arrays.copyOf(joinAt, joinAt.length);
        outer:
        while (true) {
            for (int i = 0; i < leaveAt.length; i++) {
                double leave = leaveAt[i];
                if (leave >= leaveStart && leave <= leaveStart + ql) {
                    // this one
                    newStep[i] = leaveAt[i] + ql;
                    leaveAt[i] = Double.MIN_VALUE;
                    continue outer;
                }
            }
            break;
        }
        return newStep;
    }
}
