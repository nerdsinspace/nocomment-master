package nocomment.master;

import nocomment.master.network.NoCommentServer;
import nocomment.master.tracking.TrackyTrackyManager;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class NoComment {

    public static Executor executor = Executors.newCachedThreadPool(); // has no maximum

    public static void main(String[] args) throws IOException {
        new TrackyTrackyManager(Server.getServer("2b2t.org"));
        //new TrackyTrackyManager(Server.getServer("constantiam.net"));
        NoCommentServer.listen();
    }
}
