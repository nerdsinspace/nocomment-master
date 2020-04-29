package nocomment.master;

import nocomment.master.db.Database;
import nocomment.master.network.BlockAPI;
import nocomment.master.network.NoCommentServer;
import nocomment.master.util.LoggingExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class NoComment {
    public static Executor executor = new LoggingExecutor(Executors.newFixedThreadPool(48));
    public static final boolean DRY_RUN = false;

    public static void main(String[] args) throws Exception {
        if (!DRY_RUN) {
            new Database();
            executor.execute(BlockAPI::listen);
            NoCommentServer.listen();
        }
    }
}
