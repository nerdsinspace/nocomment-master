package nocomment.master.tracking;

import nocomment.master.Server;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.HighwayScanner;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TrackyTrackyManager {
    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(16); // at most 16 threads because stupid

    private final Server server;
    private final WorldTrackyTracky overworld;
    private final WorldTrackyTracky nether;

    public TrackyTrackyManager(Server server) {
        this.server = server;
        this.overworld = new WorldTrackyTracky(server.getWorld(0), this, this::lostTrackingInOverworld);
        this.nether = new WorldTrackyTracky(server.getWorld(-1), this, this::lostTrackingInNether);
        highways();

        // around my base lmao
        this.overworld.ingestApprox(new ChunkPos(13825, -21235));
        this.overworld.ingestApprox(new ChunkPos(13825, -21278));
        this.overworld.ingestApprox(new ChunkPos(13825, -21200));
        // this.overworld.ingestApprox(new ChunkPos(13825, -21105));
    }

    private void highways() {
        new HighwayScanner(nether.world, 100, nether::ingestGenericKnownHit).submitTasks();
        new HighwayScanner(overworld.world, 100, overworld::ingestGenericKnownHit).submitTasks();
    }

    private void lostTrackingInOverworld(ChunkPos pos) {
        nether.ingestApprox(new ChunkPos(pos.x / 8, pos.z / 8));
    }

    private void lostTrackingInNether(ChunkPos pos) {
        overworld.ingestApprox(new ChunkPos(pos.x * 8, pos.z * 8));
    }
}
