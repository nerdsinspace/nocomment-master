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
                runnable.run();
            } catch (Throwable th) {
                th.printStackTrace();
            }
        };
    }
}
