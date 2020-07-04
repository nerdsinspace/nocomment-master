package nocomment.master.util;

import nocomment.master.tracking.TrackyTrackyManager;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class LoggingExecutor implements Executor {
    private final Executor internal;

    public LoggingExecutor(Executor internal) {
        this.internal = internal;
    }

    public LoggingExecutor(Executor internal, long warnAfter) {
        this(internal);

        TrackyTrackyManager.scheduler.scheduleAtFixedRate(wrap(() -> checkHealth(warnAfter)), 0, 100, TimeUnit.MILLISECONDS);
    }

    private void checkHealth(long warnAfter) {
        long now = System.currentTimeMillis();
        execute(() -> {
            long then = System.currentTimeMillis();
            long duration = then - now;
            if (duration > warnAfter) {
                System.out.println("Warning: executor lag is " + duration + "ms which is more than " + warnAfter + "ms");
            }
            if (new Random().nextInt(100) == 0) {
                System.out.println("Executor delay is " + duration + "ms");
            }
        });
    }

    @Override
    public void execute(Runnable command) {
        internal.execute(wrap(command));
    }

    public static Runnable wrap(Runnable runnable) {
        return () -> {
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
                throw th;
            }
        };
    }
}
