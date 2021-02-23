package nocomment.master.db;

import nocomment.master.NoComment;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.LoggingExecutor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AsyncBatchCommitter {
    private static final LinkedBlockingQueue<AsyncDatabase> queue = new LinkedBlockingQueue<>();
    private static final int BATCH_SIZE = 100;

    static {
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(AsyncBatchCommitter::save), 0, 1, TimeUnit.SECONDS);
    }

    public static void submit(AsyncDatabase task) {
        queue.add(task);
    }

    private static void save() {
        if (queue.isEmpty()) {
            return;
        }
        if (NoComment.DRY_RUN) {
            throw new IllegalStateException();
        }
        try (Connection connection = Database.getConnection()) {
            connection.setAutoCommit(false);
            List<AsyncDatabase> tasks = new ArrayList<>(BATCH_SIZE);
            do {
                queue.drainTo(tasks, BATCH_SIZE);
                for (AsyncDatabase task : tasks) {
                    task.run(connection);
                }
                Database.incrementCommitCounter("generic_async_batched");
                connection.commit();
                tasks.clear();
            } while (queue.size() >= BATCH_SIZE);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    @FunctionalInterface
    public interface AsyncDatabase {
        void run(Connection connection) throws SQLException;
    }
}
