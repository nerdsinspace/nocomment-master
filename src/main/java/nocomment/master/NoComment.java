package nocomment.master;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import nocomment.master.db.Database;
import nocomment.master.network.NoCommentServer;
import nocomment.master.util.LoggingExecutor;
import nocomment.master.util.Telegram;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class NoComment {

    public static Executor executor = new LoggingExecutor(Executors.newFixedThreadPool(48), "main");
    public static final boolean DRY_RUN = false;

    public static void main(String[] args) throws Exception {
        Telegram.INSTANCE.startup();
        if (!DRY_RUN) {
            new HTTPServer(1234);
            DefaultExports.initialize();
            new Database();
            LoggingExecutor.wrap(NoCommentServer::listen).run();
            System.exit(1);
        }
    }

    public static String getRuntimeVariable(final String key, final String defaultValue) {
        String value = System.getenv(key);
        if (value == null) {
            value = System.getProperty(key, defaultValue);
        }
        return value;
    }
}
