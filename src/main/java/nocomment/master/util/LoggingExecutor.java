package nocomment.master.util;

import java.util.concurrent.Executor;

public class LoggingExecutor implements Executor {
    private final Executor internal;

    public LoggingExecutor(Executor internal) {
        this.internal = internal;
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
