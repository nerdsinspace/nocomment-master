package nocomment.master.tracking;

import nocomment.master.Server;
import nocomment.master.db.TrackResume;
import nocomment.master.scanners.ClusterRetryScanner;
import nocomment.master.scanners.HighwayScanner;
import nocomment.master.scanners.RingScanner;
import nocomment.master.scanners.SpiralScanner;
import nocomment.master.util.ChunkPos;

import java.util.OptionalInt;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TrackyTrackyManager {
    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(16); // at most 16 threads because stupid

    private final Server server;
    private final WorldTrackyTracky overworld;
    private final WorldTrackyTracky nether;

    public TrackyTrackyManager(Server server) {
        this.server = server;
        this.overworld = new WorldTrackyTracky(server.getWorld((short) 0), this, this::lostTrackingInOverworld);
        this.nether = new WorldTrackyTracky(server.getWorld((short) -1), this, this::lostTrackingInNether);
        highways();
        clusters();
        spiral();
    }

    private void highways() {
        System.out.println("Nether:");
        // scan the entire highway network every four hours
        new HighwayScanner(nether.world, 10000, 30_000_000 / 8, 14_400_000, nether::ingestGenericNewHit).submitTasks();
        // scan up to 250k (2m overworld) every 400 seconds (7 minutes ish)
        new HighwayScanner(nether.world, 1000, 2_000_000 / 8, 400_000, nether::ingestGenericNewHit).submitTasks();
        // scan up to 25k (200k overworld) every 40 seconds
        new HighwayScanner(nether.world, 100, 25_000, 40_000, nether::ingestGenericNewHit).submitTasks();
        // scan the 2k ring road every 4 seconds
        new RingScanner(nether.world, 99, 2000, 16_000, nether::ingestGenericNewHit).submitTasks();
        System.out.println("Overworld:");
        // scan up to 25k overworld every 40 seconds
        new HighwayScanner(overworld.world, 100, 25_000, 40_000, overworld::ingestGenericNewHit).submitTasks();
        // scan the 2k ring road every 16 seconds
        new RingScanner(overworld.world, 99, 2000, 16_000, overworld::ingestGenericNewHit).submitTasks();
    }

    private void clusters() {
        new ClusterRetryScanner(overworld.world, 50, 10, 1000, overworld::ingestGenericNewHit).submitTasks();

        //new ClusterRetryScanner(nether.world, 50, 2, 1000, nether::ingestGenericNewHit).submitTasks();
    }

    private void spiral() {
        new SpiralScanner(overworld.world, 1_000_000, 300_000, overworld::ingestGenericNewHit).submitTasks();

        new SpiralScanner(nether.world, 1_000_000, 150_000, nether::ingestGenericNewHit).submitTasks();
    }

    private void lostTrackingInOverworld(Filter lost) {
        nether.ingestApprox(new ChunkPos(lost.getMostRecentHit().x / 8, lost.getMostRecentHit().z / 8), OptionalInt.of(lost.getTrackID()), true);
    }

    private void lostTrackingInNether(Filter lost) {
        overworld.ingestApprox(new ChunkPos(lost.getMostRecentHit().x * 8, lost.getMostRecentHit().z * 8), OptionalInt.of(lost.getTrackID()), true);
    }

    public boolean hasActiveFilter(int trackID) {
        return overworld.hasActiveFilter(trackID) || nether.hasActiveFilter(trackID);
    }

    public void attemptResume(TrackResume resumeData) {
        boolean interesting = trackInterestingEnoughToGridResume(resumeData);
        System.out.println("Attempting to resume tracking at " + resumeData.pos + " in dimension " + resumeData.dimension + " in server " + server.hostname + " from track id " + resumeData.prevTrackID + " interesting " + interesting);
        switch (resumeData.dimension) {
            case 0: {
                overworld.ingestApprox(resumeData.pos, OptionalInt.of(resumeData.prevTrackID), interesting);
                break;
            }
            case -1: {
                nether.ingestApprox(resumeData.pos, OptionalInt.of(resumeData.prevTrackID), interesting);
                break;
            }
            default: {
                System.out.println("We don't do that here " + resumeData.dimension);
            }
        }
    }

    private static boolean trackInterestingEnoughToGridResume(TrackResume resumeData) {
        long spawnDistanceSq = resumeData.pos.distSq(ChunkPos.SPAWN);
        // within 1600 blocks of spawn = within 100 chunks of spawn = we don't care
        if (spawnDistanceSq <= 100 * 100) {
            return false;
        }
        // within 3 chunks of an axis = highway scanner will get it again, not worth gridding prob
        // unless they're more than 1000 chunks (16000 blocks) away, in which case we won't get to them all that quickly
        int axisDistance = Math.min(Math.min(Math.abs(resumeData.pos.x), Math.abs(resumeData.pos.z)), Math.abs(Math.abs(resumeData.pos.x) - Math.abs(resumeData.pos.z)));
        if (axisDistance <= 3 && spawnDistanceSq <= 1000 * 1000) {
            return false;
        }
        return true;
    }
}
