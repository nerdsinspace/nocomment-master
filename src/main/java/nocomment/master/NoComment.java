package nocomment.master;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import nocomment.master.db.Database;
import nocomment.master.network.NoCommentServer;
import nocomment.master.util.LoggingExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class NoComment {

    public static Executor executor = new LoggingExecutor(Executors.newFixedThreadPool(48), "main");
    public static final boolean DRY_RUN = false;

    public static void main(String[] args) throws Exception {
        new HTTPServer(1234);
        DefaultExports.initialize();
        if (!DRY_RUN) {
            new Database();
            LoggingExecutor.wrap(NoCommentServer::listen).run();
            System.exit(1);
        }
    }
}
