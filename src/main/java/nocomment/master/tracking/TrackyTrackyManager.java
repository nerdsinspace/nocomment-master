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
import java.util.concurrent.TimeUnit;

public class TrackyTrackyManager {

    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(16); // at most 16 threads because stupid

    private final Server server;
    private final WorldTrackyTracky overworld;
    private final WorldTrackyTracky nether;
    private final WorldTrackyTracky end;

    public TrackyTrackyManager(Server server) {
        this.server = server;
        this.overworld = new WorldTrackyTracky(server.getWorld((short) 0), this, this::lostTrackingInOverworld);
        this.nether = new WorldTrackyTracky(server.getWorld((short) -1), this, this::lostTrackingInNether);
        this.end = new WorldTrackyTracky(server.getWorld((short) 1), this, $ -> {});
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
        new RingScanner(nether.world, 99, 1000, 6_000, nether::ingestGenericNewHit).submitTasks();
        System.out.println("Overworld:");
        // scan up to 25k overworld every 40 seconds
        new HighwayScanner(overworld.world, 100, 25_000, 40_000, overworld::ingestGenericNewHit).submitTasks();
        // scan the 2k ring road every 16 seconds
        new RingScanner(overworld.world, 51, 2000, 16_000, overworld::ingestGenericNewHit).submitTasks();
        System.out.println("End:");
        new RingScanner(end.world, 99, 1250, 5_000, end::ingestGenericNewHit).submitTasks();
    }

    private void clusters() {
        new ClusterRetryScanner(overworld.world, 50, 5, 1000, overworld::ingestGenericNewHit).submitTasks();

        //new ClusterRetryScanner(nether.world, 50, 2, 1000, nether::ingestGenericNewHit).submitTasks();
        new ClusterRetryScanner(end.world, 50, 1, 1000, end::ingestGenericNewHit).submitTasks();
    }

    private void spiral() {
        new SpiralScanner(overworld.world, 1_000_000, 300_000, overworld::ingestGenericNewHit).submitTasks();

        new SpiralScanner(nether.world, 1_000_000, 150_000, nether::ingestGenericNewHit).submitTasks();

        new SpiralScanner(end.world, 1_000_000, 200_000, end::ingestGenericNewHit).submitTasks();
    }

    private void lostTrackingInOverworld(Track lost) {
        nether.ingestApprox(new ChunkPos(lost.getMostRecentHit().x / 8, lost.getMostRecentHit().z / 8), OptionalInt.of(lost.getTrackID()), false, 11);
        for (int i = 0; i <= 30; i += 10) {
            scheduler.schedule(() -> nether.ingestApprox(new ChunkPos(lost.getMostRecentHit().x, lost.getMostRecentHit().z), OptionalInt.of(lost.getTrackID()), false, 17), i, TimeUnit.SECONDS);
        }
    }

    private void lostTrackingInNether(Track lost) {
        overworld.ingestApprox(new ChunkPos(lost.getMostRecentHit().x * 8, lost.getMostRecentHit().z * 8), OptionalInt.of(lost.getTrackID()), true, 11);
        overworld.ingestApprox(new ChunkPos(lost.getMostRecentHit().x, lost.getMostRecentHit().z), OptionalInt.of(lost.getTrackID()), false, 19);
    }

    public boolean hasActiveFilter(int trackID) {
        return overworld.hasActiveFilter(trackID) || nether.hasActiveFilter(trackID) || end.hasActiveFilter(trackID);
    }

    public void attemptResume(TrackResume resumeData) {
        boolean interesting = trackInterestingEnoughToGridResume(resumeData, resumeData.dimension == 0);
        System.out.println("Attempting to resume tracking at " + resumeData.pos + " in dimension " + resumeData.dimension + " in server " + server.hostname + " from track id " + resumeData.prevTrackID + " interesting " + interesting);
        WorldTrackyTracky tracky;
        switch (resumeData.dimension) {
            case 0: {
                tracky = overworld;
                break;
            }
            case -1: {
                tracky = nether;
                break;
            }
            case 1: {
                tracky = end;
                interesting = true;
                break;
            }
            default: {
                System.out.println("We don't do that here " + resumeData.dimension);
                return;
            }
        }
        tracky.ingestApprox(resumeData.pos, OptionalInt.of(resumeData.prevTrackID), false, interesting ? 12 : 15);
    }

    private static boolean trackInterestingEnoughToGridResume(TrackResume resumeData, boolean ow) {
        long spawnDistanceSq = resumeData.pos.distSq(ChunkPos.SPAWN);
        // within 1600 blocks of spawn = within 100 chunks of spawn = we don't care
        if (spawnDistanceSq <= 100 * 100) {
            return false;
        }
        if (ow && spawnDistanceSq <= 1000 * 1000) {
            return false;
        }
        int axisDistance = Math.min(Math.min(Math.abs(resumeData.pos.x), Math.abs(resumeData.pos.z)), Math.abs(Math.abs(resumeData.pos.x) - Math.abs(resumeData.pos.z)));
        if (axisDistance <= 5 && spawnDistanceSq <= 1000 * 1000) {
            return false;
        }
        return true;
    }
}
