package nocomment.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TrackyTrackyManager {
    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(16); // at most 16 threads because stupid

    private final Server server;
    private final WorldTrackyTracky overworld;
    private final WorldTrackyTracky nether;

    public TrackyTrackyManager(Server server) {
        this.server = server;
        this.overworld = new WorldTrackyTracky(server.getWorld(0), this);
        this.nether = new WorldTrackyTracky(server.getWorld(-1), this);
        //highways();
        new Filter(new ChunkPos(-32, -4), overworld).start();
    }

    private void highways() {
        new HighwayScanner(nether.world, 100, nether::ingestGeneric).submitTasks();
    }
}
