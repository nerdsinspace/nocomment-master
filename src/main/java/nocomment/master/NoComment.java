package nocomment.master;

import nocomment.master.network.NoCommentServer;
import nocomment.master.util.LoggingExecutor;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class NoComment {
    public static Executor executor = new LoggingExecutor(Executors.newFixedThreadPool(48));

    public static void main(String[] args) throws IOException {
        Server.getServer("2b2t.org");
        Server.getServer("constantiam.net");
        NoCommentServer.listen();
    }
}
