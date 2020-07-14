package nocomment.master.slurp;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.clustering.DBSCAN;
import nocomment.master.db.Database;
import nocomment.master.tracking.TrackyTrackyManager;
import nocomment.master.util.BlockPos;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SlurpManager {
    private static final long SIGN_AGE = TimeUnit.DAYS.toMillis(3);
    private static final long BRUSH_AGE = TimeUnit.MINUTES.toMillis(30);
    private static final long EXPAND_AGE = TimeUnit.DAYS.toMillis(21);
    private static final long RENEW_AGE = TimeUnit.MINUTES.toMillis(30);
    private static final long RENEW_INTERVAL = RENEW_AGE * 2; // 1 hour
    private static final long CHECK_MAX_GAP = TimeUnit.SECONDS.toMillis(30);
    private static final long CLUSTER_DATA_CACHE_DURATION = TimeUnit.DAYS.toMillis(1);
    private static final long MIN_DIST_SQ_CHUNKS = 6250L * 6250L; // 100k blocks
    private static final long NUM_RENEWALS = 4;
    public final World world;
    private final ChunkManager chunkManager;
    private final Map<ChunkPos, ResumeDataForChunk> askedAndGotUnloadedResponse = new HashMap<>();
    private final Map<BlockPos, AskStatus> allAsks = new HashMap<>();
    private final Set<BlockPos> signsAskedFor = new HashSet<>();
    private final LinkedBlockingQueue<ChunkPosWithTimestamp> ingest = new LinkedBlockingQueue<>();
    private final Object ingestLock = new Object();
    private final Map<ChunkPos, Long> clusterNonmembershipConfirmedAtCache = new HashMap<>();
    private final Map<ChunkPos, Long> clusterHit = new HashMap<>();
    private final Map<ChunkPos, Long> renewalSchedule = new HashMap<>();
    private final Map<ChunkPos, int[][]> heightMapCache = new HashMap<>();

    public SlurpManager(World world) {
        this.world = world;
        if (world.dimension != 0) {
            throw new IllegalArgumentException("Only overworld for the moment");
        }
        if (world.server.hostname.equals("2b2t.org")) {
            this.chunkManager = new ChunkManager();
        } else {
            this.chunkManager = null;
        }
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::pruneAsks), 24 * 60 + 25, 24 * 60, TimeUnit.MINUTES);
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::pruneClusterData), 1, 1, TimeUnit.HOURS);
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::pruneBlocks), 40, 60, TimeUnit.MINUTES);
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(() -> {
            ingestIntoClusterHit();
            scanClusterHit();
        }), 10000, 250, TimeUnit.MILLISECONDS);
    }

    private synchronized void pruneBlocks() {
        int beforeSz = allAsks.size();
        long now = System.currentTimeMillis();
        synchronized (world.blockCheckManager) {
            allAsks.entrySet().removeIf(ask -> ask.getValue().lastDirectAsk < now - BlockCheckManager.PRUNE_AGE && world.blockCheckManager.hasBeenRemoved(ask.getKey()));
        }
        int total = askedAndGotUnloadedResponse.values().stream().mapToInt(rd -> rd.failedSignChecks.size() + rd.failedBlockChecks.size()).sum();
        System.out.println("Took " + (System.currentTimeMillis() - now) + "ms to prune allAsks keySet. Size went from " + beforeSz + " to " + allAsks.size() + ". HMC: " + heightMapCache.size() + ". CNCAC: " + clusterNonmembershipConfirmedAtCache.size() + ". CH: " + clusterHit.size() + ". RS: " + renewalSchedule.size() + ". SAF: " + signsAskedFor.size() + ". AAGUR: " + askedAndGotUnloadedResponse.size() + ". AAGURT: " + total);
    }

    private void scanClusterHit() {
        long now = System.currentTimeMillis();
        renewalSchedule.values().removeIf(ts -> ts < now);
        Optional<ChunkPos> candidates = clusterHit.entrySet()
                .stream()
                .filter(entry -> entry.getValue() > now - CHECK_MAX_GAP)
                .map(Map.Entry::getKey)
                .filter(cpos -> !(renewalSchedule.containsKey(cpos)))
                .max(Comparator.comparingLong(ChunkPos::distSq));
        if (!candidates.isPresent()) {
            return;
        }
        ChunkPos cpos = candidates.get();
        if (cpos.distSq() < MIN_DIST_SQ_CHUNKS) {
            return;
        }
        System.out.println("Beginning slurp on chunk " + cpos);
        // again, no need for a lock on renewalSchedule since only this touches it
        renewalSchedule.put(cpos, now + RENEW_INTERVAL);
        Random random = new Random();
        for (int i = 0; i < NUM_RENEWALS; i++) {
            int dx = random.nextInt(16);
            int dz = random.nextInt(16);
            int x = cpos.getXStart() + dx;
            int z = cpos.getZStart() + dz;

            if (this.chunkManager != null) { // only if we have chunk generation can we do heightmap based queries
                BlockPos pos = new BlockPos(x, heightMapCache.computeIfAbsent(cpos, this::heightMap)[dx][dz], z);
                askFor(pos, 57, now - RENEW_AGE);
                askFor(pos.add(0, 1, 0), 58, now - RENEW_AGE);
            }

            // also keep up to date any sky structures / sky bases...
            interestingYCoordsFromLastTime(x, z).forEach(y -> {
                // no need to check if y==pos.y, if it is equal it's fine since it'll dedup on prio and constant now-renew_age
                BlockPos skybase = new BlockPos(x, y, z);
                if (random.nextBoolean()) { // :zany_face:
                    askFor(skybase, 58, now - RENEW_AGE); // slightly less important
                }
                if (random.nextBoolean()) { // :yum:
                    // randomly grab something a little ways above or below, just for fun
                    askFor(skybase.add(0, (random.nextBoolean() ? 1 : -1) * (2 + random.nextInt(4)), 0), 60, now - RENEW_AGE);
                }
                // the cool part is that both cases (max and min) will overlap in the common case with the standard heightmap coords :)
                // so this is zero cost (except on decorator mismatch) other than when it's a true base to renew :)
                if (random.nextBoolean()) { // :woozy_face:
                    askFor(skybase.add(0, 1, 0), 59, now - RENEW_AGE);
                }
            });
        }
        // also one more just for fun
        askFor(cpos.origin().add(random.nextInt(16), random.nextInt(256), random.nextInt(16)), 60, now - RENEW_AGE);
    }

    private int[][] heightMap(ChunkPos cpos) {
        // return: the Y coordinates of the first non-air block. 0 if fully air
        int[][] ret = new int[16][16];
        int[] data;
        try {
            data = chunkManager.getChunk(cpos).get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
        for (int x = 0; x < 16; x++) {
            for (int z = 0; z < 16; z++) {
                for (int y = 255; y >= 0; y--) {
                    if (!isAir(data[y * 256 + x * 16 + z])) {
                        ret[x][z] = y;
                        break;
                    }
                }
            }
        }
        return ret;
    }

    private List<Integer> interestingYCoordsFromLastTime(int x, int z) {
        // this will include any "manual" slurping i've done in singleplayer
        // ALL skybases should be caught by this, if we've slurped them at any point in history, I think?
        try (Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("WITH col AS (SELECT block_state, y, ROW_NUMBER() OVER (PARTITION BY y ORDER BY created_at DESC) AS age FROM blocks WHERE x = ? AND z = ? AND dimension = ? AND server_id = ?) SELECT (SELECT MAX(y) FROM col WHERE age = 1 AND block_state <> 0) AS max_y, (SELECT MIN(y) FROM col WHERE age = 1 AND block_state = 0) AS min_y")) {
            stmt.setInt(1, x);
            stmt.setInt(2, z);
            stmt.setShort(3, world.dimension);
            stmt.setShort(4, world.server.serverID);
            try (ResultSet rs = stmt.executeQuery()) {
                List<Integer> ret = new ArrayList<>();
                if (rs.next()) {
                    int maxY = rs.getInt("max_y");
                    if (!rs.wasNull()) {
                        ret.add(maxY);
                    }
                    int minY = rs.getInt("min_y");
                    if (!rs.wasNull()) {
                        ret.add(minY - 1); // the caller does (y, y+1), and we want the air and the block below it, so we start at that non-air block which is at y-1
                    }
                }
                return ret;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private void ingestIntoClusterHit() {
        List<ChunkPosWithTimestamp> tmpBuffer = new ArrayList<>(ingest.size());
        try {
            tmpBuffer.add(ingest.take()); // blockingly take at least one
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        ingest.drainTo(tmpBuffer); // non blockingly take any remainings
        Map<ChunkPos, Long> chunkTimestamps = tmpBuffer.stream().collect(Collectors.groupingBy(cpwt -> cpwt.pos, Collectors.reducing(0L, cpwt -> cpwt.timestamp, Math::max)));
        // first remove non cluster members, because otherwise clusterHit would get HUGE, instantly
        // (it would get like, every path taken by everyone for a day)
        // and that would suck to sort by distance
        try (Connection connection = Database.getConnection()) {
            synchronized (ingestLock) {
                long now = System.currentTimeMillis();
                Iterator<ChunkPos> it = chunkTimestamps.keySet().iterator();
                while (it.hasNext()) {
                    ChunkPos cpos = it.next();
                    if (clusterHit.containsKey(cpos)) {
                        continue; // this pos has already passed this check previously
                    }
                    Long confirmedAt = clusterNonmembershipConfirmedAtCache.get(cpos);
                    if (confirmedAt != null && confirmedAt > now - CLUSTER_DATA_CACHE_DURATION) {
                        it.remove();
                        continue;
                    }
                    if (!DBSCAN.INSTANCE.fetch(world.server.serverID, world.dimension, cpos.x, cpos.z, connection).isPresent()) {
                        it.remove();
                        clusterNonmembershipConfirmedAtCache.put(cpos, now);
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
        // no need for a lock on clusterHit, since this is the only function that touches it, and this function is single threaded
        chunkTimestamps.forEach((pos, timestamp) -> {
            for (int dx = -4; dx <= 4; dx++) {
                for (int dz = -4; dz <= 4; dz++) {
                    clusterHit.merge(pos.add(dx, dz), timestamp, Math::max);
                }
            }
        });
        long now = System.currentTimeMillis();
        clusterHit.values().removeIf(ts -> ts < now - RENEW_AGE);
    }

    public void clusterUpdate(ChunkPos cpos) {
        synchronized (ingestLock) {
            clusterNonmembershipConfirmedAtCache.remove(cpos);
        }
    }

    private synchronized void pruneAsks() {
        // re-paint-bucket everything
        allAsks.values().forEach(stat -> {
            stat.response = OptionalInt.empty();
            stat.clearedManually = true;
        });
        askedAndGotUnloadedResponse.clear();
    }

    private void pruneClusterData() {
        // note: technically shouldn't be necessary due to live update but node stuff is complicated and dbscan might brainfart or miss the schedule who knows
        synchronized (ingestLock) {
            long now = System.currentTimeMillis();
            clusterNonmembershipConfirmedAtCache.values().removeIf(ts -> ts < now - CLUSTER_DATA_CACHE_DURATION);
        }
    }

    public void arbitraryHit(ChunkPos cpos) {
        world.blockCheckManager.loaded(cpos);
        arbitraryHit(cpos, false);
    }

    private synchronized void arbitraryHit(ChunkPos cpos, boolean internal) {
        if (internal || DBSCAN.INSTANCE.aggregateEligible(cpos)) {
            ingest.add(new ChunkPosWithTimestamp(cpos));
        }
        ResumeDataForChunk data = askedAndGotUnloadedResponse.remove(cpos);
        if (data == null) {
            return;
        }
        // bypass allAsks add since we took the data from there in the first place
        // simple retransmit
        data.failedBlockChecks.forEach((otherPos, failedAsk) -> doRawAsk(failedAsk.mustBeNewerThan, otherPos, failedAsk.priority));
        data.failedSignChecks.forEach((otherPos, mustBeNewerThan) -> doRawSign(mustBeNewerThan, otherPos));
    }

    private synchronized ResumeDataForChunk getData(ChunkPos cpos) {
        return askedAndGotUnloadedResponse.computeIfAbsent(cpos, cpos0 -> new ResumeDataForChunk());
    }

    private synchronized void blockRecv(BlockPos pos, OptionalInt state, boolean updated, int[] chunkData) {
        ChunkPos cpos = new ChunkPos(pos);
        ResumeDataForChunk data = getData(cpos);

        if (!state.isPresent()) {
            // a miss
            // mark it as such, and we'll retry if we ever see this chunk reloaded! :)
            data.failedBlockChecks.put(pos, new FailedAsk(allAsks.get(pos)));
            allAsks.get(pos).response = OptionalInt.empty();
            return;
        }
        // a hit
        // first, fixup unloaded responses
        data.failedBlockChecks.remove(pos); // don't double ask
        arbitraryHit(cpos, true); // ask for the OTHER pending checks on this chunk

        // now, what were we expecting?
        int blockState = state.getAsInt();
        if (isSign(blockState)) {
            if (signsAskedFor.add(pos)) {
                System.out.println("NOT asking for sign at " + pos);
                //doRawSign(signBrushNewer(), pos);
            }
        }
        if (isShulker(blockState)) {
            System.out.println("Shulker (blockstate " + blockState + ") at " + pos);
        }
        int expected = expected(pos, chunkData);
        allAsks.get(pos).response = state;
        // important to remember that there are FOUR things at play here:
        // previous (stored in DB)
        // current
        // expected (current cached value)
        // expected (from fresh world gen)

        // for previous, all we get is the "updated" boolean which says if current!=previous
        // we currently 100% ignore the 4th one btw

        boolean expand = false;
        boolean expandBrush = false;
        // if "updated" is true, we will always expand in brush mode, because that means the actual world changed
        if (updated) {
            expand = true;
            expandBrush = true;
        } else { // either previous is null, or previous==current
            if (chunkData != null && blockState != expected) { // only if we actually have chunkData
                if (!(isStone(blockState) && isStone(expected))) { // stone variants are a troll, don't expand them
                    expand = true;
                }
            }
            // if current == expected, and updated is false, then there is absolutely no new information here
        }
        if (expand) {
            if (expandBrush) {
                System.out.println("Expanding " + pos + " in brush mode");
            }
            for (int x = -1; x <= 1; x++) {
                for (int y = -1; y <= 1; y++) {
                    for (int z = -1; z <= 1; z++) {
                        askFor(pos.add(x, y, z), Math.abs(x) + Math.abs(y) + Math.abs(z) + 57, calcNewer(expandBrush ? BRUSH_AGE : EXPAND_AGE));
                    }
                }
            }
        }
    }

    private static long calcNewer(long interval) {
        // avoid spurious updates to askFor
        return System.currentTimeMillis() / (interval / 10) * (interval / 10) - interval;
    }

    private synchronized int expected(BlockPos pos, int[] chunkData) {
        AskStatus stat = allAsks.get(pos);
        if (stat != null && stat.response.isPresent()) {
            return stat.response.getAsInt();
        }
        if (chunkData == null) {
            return -1;
        }
        int x = pos.x & 0x0f;
        int y = pos.y & 0xff;
        int z = pos.z & 0x0f;
        return chunkData[y * 256 + x * 16 + z];
    }

    private synchronized void signRecv(BlockPos pos, long mustBeNewerThan, Optional<byte[]> nbt) {
        ChunkPos cpos = new ChunkPos(pos);
        if (!nbt.isPresent()) {
            // schedule for retry
            getData(cpos).failedSignChecks.put(pos, mustBeNewerThan);
            return;
        }
        getData(cpos).failedSignChecks.remove(pos); // success
        arbitraryHit(cpos, true);
    }

    private static boolean isSign(int blockState) {
        return (blockState >= 1008 && blockState < 1024) || (blockState >= 1090 && blockState < 1094);
    }

    private static boolean isStone(int blockState) {
        return blockState >= 16 && blockState < 23;
    }

    private static boolean isAir(int blockState) {
        return blockState == 0;
    }

    private static final Set<Integer> SHULKER_BLOCK_STATES = new HashSet<>();

    static {
        for (int i : new int[]{3504, 3505, 3506, 3507, 3508, 3509, 3520, 3521, 3522, 3523, 3524, 3525, 3536, 3537, 3538, 3539, 3540, 3541, 3552, 3553, 3554, 3555, 3556, 3557, 3568, 3569, 3570, 3571, 3572, 3573, 3584, 3585, 3586, 3587, 3588, 3589, 3600, 3601, 3602, 3603, 3604, 3605, 3616, 3617, 3618, 3619, 3620, 3621, 3632, 3633, 3634, 3635, 3636, 3637, 3648, 3649, 3650, 3651, 3652, 3653, 3664, 3665, 3666, 3667, 3668, 3669, 3680, 3681, 3682, 3683, 3684, 3685, 3696, 3697, 3698, 3699, 3700, 3701, 3712, 3713, 3714, 3715, 3716, 3717, 3728, 3729, 3730, 3731, 3732, 3733, 3744, 3745, 3746, 3747, 3748, 3749}) {
            SHULKER_BLOCK_STATES.add(i);
        }
    }

    private static boolean isShulker(int blockState) {
        return SHULKER_BLOCK_STATES.contains(blockState);
    }

    private synchronized void askFor(BlockPos pos, int priority, long mustBeNewerThan) {
        if (pos.y >= 256 || pos.y < 0) {
            return;
        }
        AskStatus cur = allAsks.get(pos);
        outer:
        {
            if (cur != null) {
                if (cur.highestPriorityAskedAt <= priority && cur.mustBeNewerThan >= mustBeNewerThan) {
                    if (cur.clearedManually) {
                        cur.clearedManually = false;
                        break outer;
                    }
                    return;
                }
            } else {
                cur = new AskStatus();
            }
            cur.highestPriorityAskedAt = priority;
            cur.mustBeNewerThan = mustBeNewerThan;
        }
        cur.response = OptionalInt.empty();
        cur.lastDirectAsk = System.currentTimeMillis();
        allAsks.put(pos, cur);
        doRawAsk(mustBeNewerThan, pos, priority);
    }

    private void doRawAsk(long mustBeNewerThan, BlockPos pos, int priority) {
        world.blockCheckManager.requestBlockState(mustBeNewerThan, pos, priority, (state, updated) -> {
            if (chunkManager == null) {
                blockRecv(pos, state, updated, null);
            } else {
                chunkManager.getChunk(new ChunkPos(pos)).thenAcceptAsync(chunkData -> blockRecv(pos, state, updated, chunkData), NoComment.executor);
            }
        });
    }

    private void doRawSign(long mustBeNewerThan, BlockPos pos) {
        world.signManager.requestAsync(mustBeNewerThan, pos, nbt -> signRecv(pos, mustBeNewerThan, nbt));
    }

    private static class FailedAsk {
        int priority;
        long mustBeNewerThan;

        public FailedAsk(AskStatus stat) {
            this.priority = stat.highestPriorityAskedAt;
            this.mustBeNewerThan = stat.mustBeNewerThan;
        }
    }

    private static class ResumeDataForChunk {
        Map<BlockPos, FailedAsk> failedBlockChecks = new HashMap<>();
        Map<BlockPos, Long> failedSignChecks = new HashMap<>();
    }

    private static class AskStatus {
        int highestPriorityAskedAt;
        long mustBeNewerThan;
        OptionalInt response;
        long lastDirectAsk;
        boolean clearedManually;
    }

    private static class ChunkPosWithTimestamp {
        public final ChunkPos pos;
        public final long timestamp;

        public ChunkPosWithTimestamp(ChunkPos pos) {
            this.pos = pos;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
