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
        highways();
    }

    private void highways() {
        //new HighwayScanner(nether.world, 100, nether::ingestGeneric).submitTasks();
        new HighwayScanner(nether.world, 100, pos -> {
            if (Math.abs(pos.x) < 100 && Math.abs(pos.z) < 100) {
                return;
            }
            new Filter(pos, nether).start();
            //new Filter(new ChunkPos(pos.x * 8, pos.z * 8), overworld).start();
        }).submitTasks();
    }
}
