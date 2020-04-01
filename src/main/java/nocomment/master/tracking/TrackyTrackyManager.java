package nocomment.master.tracking;

import nocomment.master.Server;
import nocomment.master.db.TrackResume;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.HighwayScanner;
import nocomment.master.util.RingScanner;

import java.util.OptionalLong;
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
        spiral();
    }

    private void highways() {
        System.out.println("Nether:");
        // scan the entire highway network every four hours
        new HighwayScanner(nether.world, 10001, 30_000_000 / 8, 14_400_000, nether::ingestGenericNewHit).submitTasks();
        // scan up to 250k (2m overworld) every 400 seconds (7 minutes ish)
        new HighwayScanner(nether.world, 1000, 2_000_000 / 8, 400_000, nether::ingestGenericNewHit).submitTasks();
        // scan up to 25k (200k overworld) every 40 seconds
        new HighwayScanner(nether.world, 100, 25_000, 40_000, nether::ingestGenericNewHit).submitTasks();
        // scan the 2k ring road every 4 seconds
        new RingScanner(nether.world, 99, 2000, 4_000, nether::ingestGenericNewHit).submitTasks();
        System.out.println("Overworld:");
        // scan up to 25k overworld every 40 seconds
        new HighwayScanner(overworld.world, 100, 25_000, 40_000, overworld::ingestGenericNewHit).submitTasks();
        // scan the 2k ring road every 4 seconds
        new RingScanner(overworld.world, 99, 2000, 4_000, overworld::ingestGenericNewHit).submitTasks();
    }

    private void spiral() {
        overworld.grid(10000, 9, 250, new ChunkPos(0, 0), overworld::ingestGenericNewHit);
        nether.grid(10000, 9, 250, new ChunkPos(0, 0), nether::ingestGenericNewHit);
    }

    private void lostTrackingInOverworld(Filter lost) {
        nether.ingestApprox(new ChunkPos(lost.getMostRecentHit().x / 8, lost.getMostRecentHit().z / 8), OptionalLong.of(lost.getTrackID()));
    }

    private void lostTrackingInNether(Filter lost) {
        overworld.ingestApprox(new ChunkPos(lost.getMostRecentHit().x * 8, lost.getMostRecentHit().z * 8), OptionalLong.of(lost.getTrackID()));
    }

    public boolean hasActiveFilter(long trackID) {
        return overworld.hasActiveFilter(trackID) || nether.hasActiveFilter(trackID);
    }

    public void attemptResume(TrackResume resumeData) {
        System.out.println("Attempting to resume tracking at " + resumeData.pos + " in dimension " + resumeData.dimension + " in server " + server.hostname + " from track id " + resumeData.prevTrackID);
        switch (resumeData.dimension) {
            case 0: {
                overworld.ingestApprox(resumeData.pos, OptionalLong.of(resumeData.prevTrackID));
                break;
            }
            case -1: {
                nether.ingestApprox(resumeData.pos, OptionalLong.of(resumeData.prevTrackID));
                break;
            }
            default: {
                System.out.println("We don't do that here " + resumeData.dimension);
            }
        }
    }
}
