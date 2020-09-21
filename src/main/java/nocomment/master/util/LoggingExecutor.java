package nocomment.master.util;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import nocomment.master.tracking.TrackyTrackyManager;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public final class LoggingExecutor implements Executor {
    private static final Gauge globalThreads = Gauge.build()
            .name("global_threads")
            .help("Number of executing threads globally")
            .register();

    private static final Gauge pooledThreads = Gauge.build()
            .name("pooled_threads")
            .help("Number of threads by state by pool")
            .labelNames("pool", "state")
            .register();

    private static final Histogram poolLatencies = Histogram.build()
            .name("pool_latencies")
            .help("Pool latencies")
            .labelNames("pool")
            .register();

    private final Executor internal;
    private final String name;

    public LoggingExecutor(Executor internal, String name) {
        this.internal = internal;
        this.name = name;

        TrackyTrackyManager.scheduler.scheduleAtFixedRate(wrap(this::checkHealth), 0, 100, TimeUnit.MILLISECONDS);
    }

    private void checkHealth() {
        Histogram.Timer timer = poolLatencies.labels(name).startTimer();
        execute(timer::observeDuration);
    }

    @Override
    public void execute(Runnable command) {
        pooledThreads.labels(name, "queued").inc();
        Runnable wrapped = wrap(command);
        internal.execute(() -> {
            pooledThreads.labels(name, "queued").dec();
            pooledThreads.labels(name, "executing").inc();
            wrapped.run();
            pooledThreads.labels(name, "executing").dec();
            pooledThreads.labels(name, "done").inc();
        });
    }

    public static Runnable wrap(Runnable runnable) {
        return () -> {
            globalThreads.inc(); // includes scheduled execution!
            try {
                try {
                    runnable.run();
                } catch (OutOfMemoryError oom) {
                    try {
                        oom.printStackTrace();
                        System.out.println("OUT OF MEMORY! EXITING!");
                    } catch (Throwable th) {}
                    System.exit(1);
                }
            } catch (Throwable th) {
                th.printStackTrace();
                Telegram.INSTANCE.complain(th);
                System.exit(1);
            } finally {
                globalThreads.dec();
            }
        };
    }
}
