package nocomment.master.slurp;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;
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
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class SlurpManager {

    private static final Gauge slurpData = Gauge.build()
            .name("slurp_data")
            .help("Sizes of various slurp data structures")
            .labelNames("field")
            .register();
    private static final Counter slurpedChunks = Counter.build()
            .name("slurped_chunks_total")
            .help("Number of chunks we've seeded slurping in")
            .register();
    private static final Counter slurpChunkSeeds = Counter.build()
            .name("slurp_chunk_seeds_total")
            .help("Number of seed blocks we have checked in total")
            .register();
    private static final Counter slurpDelay = Counter.build()
            .name("slurped_delay_total")
            .help("Number of times seeding a chunk has been delayed")
            .labelNames("reason")
            .register();
    private static final Counter clusterHitDirectPrune = Counter.build()
            .name("slurp_cluster_hit_direct_prune_total")
            .help("Number of times a chunk has been removed from clusterHitDirect")
            .register();
    private static final Histogram asksPruneLatencies = Histogram.build()
            .name("slurp_asks_prune_latencies")
            .help("Asks prune latencies")
            .register();
    private static final Histogram metricsUpdateLatencies = Histogram.build()
            .name("slurp_metrics_update_latencies")
            .help("Metrics update latencies")
            .register();
    private static final Histogram clusterHitIngestLatencies = Histogram.build()
            .name("slurp_cluster_hit_ingest_latencies")
            .help("Cluster hit ingest latencies")
            .register();
    private static final Histogram chunkSeedScanLatencies = Histogram.build()
            .name("slurp_chunk_seed_scan_latencies")
            .help("Chunk seed scan latencies")
            .register();
    private static final Gauge allAsksOffHeap = Gauge.build()
            .name("all_asks_off_heap")
            .help("Size of the allAsks map off heap")
            .register();
    private static final Counter blacklistedClusterSkips = Counter.build()
            .name("blacklisted_cluster_skips_total")
            .help("Number of times we have skipped a blacklisted cluster in total")
            .register();
    private static final long SIGN_AGE = TimeUnit.DAYS.toMillis(3);
    private static final long EXPAND_AGE = TimeUnit.DAYS.toMillis(21);
    private static final long RENEW_AGE = TimeUnit.MINUTES.toMillis(15);
    private static final long RENEW_INTERVAL = RENEW_AGE * 2; // 30 minutes
    private static final long BRUSH_AGE = RENEW_AGE; // it's complicated
    private static final long CHECK_MAX_GAP = TimeUnit.SECONDS.toMillis(30);
    private static final long CLUSTER_DATA_CACHE_DURATION = TimeUnit.DAYS.toMillis(1);
    private static final long PRUNE_AGE = TimeUnit.HOURS.toMillis(2);
    private static final long PENDING_RECHECK_AGE = TimeUnit.MINUTES.toMillis(2);
    private static final long HEIGHT_MAP_CACHE_DURATION = TimeUnit.DAYS.toMillis(1);
    private static final long MIN_DIST_SQ_CHUNKS = 6250L * 6250L; // 100k blocks
    private static final long NUM_RENEWALS = 4;
    private static final int MAX_CHECK_STATUS_QUEUE_LENGTH = 5000;
    private static final int MIN_PENDING_TO_RECHECK = 50;
    private static final int MAX_PENDING_CHECKS = 250_000;
    private static final IntOpenHashSet BLACKLISTED_CLUSTERS = new IntOpenHashSet(new IntArrayList(new int[]{
            218456744, // skymasons 1
            210410667, // skymasons 2
            // TODO add other stupid stuff that we don't want to waste time on
    }));
    private static final Random random = new Random();
    public final World world;
    private final Executor blockRecvExecutor = new LoggingExecutor(Executors.newSingleThreadExecutor(), "block_recv"); // blockRecv is synchronized so we only need one
    private final ChunkManager chunkManager;
    private final Long2ObjectOpenHashMap<ResumeDataForChunk> askedAndGotUnloadedResponse = new Long2ObjectOpenHashMap<>(); // long = chunkpos
    private final ChronicleMap<LongValue, AskStatus> allAsks = ChronicleMap.of(LongValue.class, AskStatus.class) // long = blockpos
            .name("all_asks")
            .constantKeySizeBySample(AskStatus.Helper.LONG_VALUE_KEY)
            .constantValueSizeBySample(AskStatus.Helper.ASK_STATUS_VALUE)
            .entries(25_000_000)
            .maxBloatFactor(4)
            .create();
    private final Set<BlockPos> signsAskedFor = new HashSet<>();
    private final LinkedBlockingQueue<ChunkPosWithTimestamp> ingest = new LinkedBlockingQueue<>();
    private final Object ingestLock = new Object();
    private final Object clusterLock = new Object();
    // long chunkPos
    private final LongOpenHashSet clusterMembershipConfirmed = new LongOpenHashSet();
    // long chunkPos, long time
    private final Long2LongOpenHashMap clusterNonmembershipConfirmedAtCache = new Long2LongOpenHashMap();
    private final Long2LongOpenHashMap clusterHit = new Long2LongOpenHashMap();
    private final Long2LongOpenHashMap clusterHitDirect = new Long2LongOpenHashMap();
    private final LinkedBlockingQueue<Long> clusterHitDirectPrunes = new LinkedBlockingQueue<>();
    private final Long2LongOpenHashMap renewalSchedule = new Long2LongOpenHashMap();
    // long = chunkPos
    private final Long2ObjectOpenHashMap<HeightmapTimestampedCacheEntryWrapper> heightMapCache = new Long2ObjectOpenHashMap<>();
    private long lastHeightMapCachePurgeAt;

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
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::pruneAsks), 12 * 60 + 10, 12 * 60, TimeUnit.MINUTES);
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::pruneClusterData), 1, 1, TimeUnit.HOURS);
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::pruneBlocks), 20, 60, TimeUnit.MINUTES);
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(() -> {
            synchronized (clusterLock) {
                Histogram.Timer timer = clusterHitIngestLatencies.startTimer();
                ingestIntoClusterHit();
                timer.observeDuration();
            }
        }), 10000, 250, TimeUnit.MILLISECONDS);
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(() -> {
            synchronized (clusterLock) {
                Histogram.Timer timer = chunkSeedScanLatencies.startTimer();
                scanClusterHit();
                timer.observeDuration();
            }
        }), 10000, 10, TimeUnit.MILLISECONDS);
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::updateMetrics), 0, 5, TimeUnit.SECONDS);
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::detectUnloaded), 0, 1, TimeUnit.MINUTES);
    }

    private synchronized void pruneBlocks() {
        int beforeSz = allAsks.size();
        long now = System.currentTimeMillis();
        Histogram.Timer timer = asksPruneLatencies.startTimer();
        synchronized (world.blockCheckManager) {
            allAsks.forEachEntry(entry -> {
                if (entry.value().getUsing(AskStatus.Helper.ASK_STATUS_VALUE).getLastDirectAsk() < now - PRUNE_AGE &&
                        world.blockCheckManager.hasBeenRemoved(entry.key().getUsing(AskStatus.Helper.LONG_VALUE_KEY).getValue())) {
                    entry.doRemove();
                }
            });
        }
        timer.observeDuration();
        long mid = System.currentTimeMillis();
        int total = askedAndGotUnloadedResponse.values().stream().mapToInt(rd -> rd.failedSignChecks.size() + rd.failedBlockChecks.size()).sum();
        System.out.println("FASTER? Took " + (mid - now) + "ms to prune allAsks keySet. Took " + (System.currentTimeMillis() - mid) + "ms to check AAGURT. Size went from " + beforeSz + " to " + allAsks.size() + ". HMC: " + heightMapCache.size() + ". CMC: " + clusterMembershipConfirmed.size() + ". CNCAC: " + clusterNonmembershipConfirmedAtCache.size() + ". CH: " + clusterHit.size() + ". RS: " + renewalSchedule.size() + ". SAF: " + signsAskedFor.size() + ". AAGUR: " + askedAndGotUnloadedResponse.size() + ". AAGURT: " + total);
    }

    private synchronized void updateMetrics() {
        Histogram.Timer timer = metricsUpdateLatencies.startTimer();
        allAsksOffHeap.set(allAsks.offHeapMemoryUsed());
        slurpData.labels("all_asks").set(allAsks.size());
        slurpData.labels("height_map_cache").set(heightMapCache.size());
        slurpData.labels("cluster_membership_confirmed").set(clusterMembershipConfirmed.size());
        slurpData.labels("cluster_nonmembership_confirmed_at_cache").set(clusterNonmembershipConfirmedAtCache.size());
        slurpData.labels("cluster_hit").set(clusterHit.size());
        slurpData.labels("cluster_hit_direct").set(clusterHitDirect.size());
        slurpData.labels("renewal_schedule").set(renewalSchedule.size());
        // signs :(
        slurpData.labels("asked_and_got_unloaded_response").set(askedAndGotUnloadedResponse.size());
        int total = askedAndGotUnloadedResponse.values().stream().mapToInt(rd -> rd.failedSignChecks.size() + rd.failedBlockChecks.size()).sum();
        slurpData.labels("asked_and_got_unloaded_response_total").set(total);
        timer.observeDuration();
    }

    private void slurpDelay(String reason) {
        slurpDelay.labels(reason).inc();
        try {
            Thread.sleep(1000); // don't add fuel to the fire
        } catch (InterruptedException ex) {}
    }

    private void detectUnloaded() {
        if (clusterHitDirectPrunes.isEmpty()) {
            return;
        }
        Set<Long> unloaded = new HashSet<>();
        clusterHitDirectPrunes.drainTo(unloaded);
        List<Long> toRecheck = new ArrayList<>();
        world.chunkChecksLookup(unloaded.iterator(), (cpos, count) -> {
            if (count > MIN_PENDING_TO_RECHECK) {
                toRecheck.add(cpos); // don't do it in here, we are holding world lock
            }
        });
        Random loc = new Random();
        toRecheck.forEach(cposSerialized -> {
            ChunkPos cpos = ChunkPos.fromLong(cposSerialized);
            askFor(cpos.origin().add(loc.nextInt(16), loc.nextInt(256), loc.nextInt(16)), 55, System.currentTimeMillis() - PENDING_RECHECK_AGE);
        });
    }

    private void scanClusterHit() {
        if (BlockCheckManager.checkStatusQueue.size() > MAX_CHECK_STATUS_QUEUE_LENGTH) {
            slurpDelay("check_queue_length");
            return;
        }
        if (world.pendingChecks() > MAX_PENDING_CHECKS) {
            slurpDelay("pending_check_count");
            return;
        }
        long now = System.currentTimeMillis();
        renewalSchedule.values().removeIf(ts -> ts < now);
        Optional<Long2LongMap.Entry> candidates = clusterHit.long2LongEntrySet()
                .stream()
                .filter(entry -> {
                    if (entry.getLongValue() < now - CHECK_MAX_GAP) {
                        return false;
                    }
                    long cpos = entry.getLongKey();
                    return !renewalSchedule.containsKey(cpos);
                })
                .max(Comparator.comparingLong(entry -> ChunkPos.distSqSerialized(entry.getLongKey())));
        if (!candidates.isPresent()) {
            slurpDelay("renewal_schedule");
            return;
        }
        long cposSerialized = candidates.get().getLongKey();
        if (ChunkPos.distSqSerialized(cposSerialized) < MIN_DIST_SQ_CHUNKS) {
            slurpDelay("too_close");
            return;
        }
        //System.out.println("Beginning slurp on chunk " + cpos);
        slurpedChunks.inc();
        // again, no need for a lock on renewalSchedule since only this touches it
        renewalSchedule.put(cposSerialized, now + RENEW_INTERVAL);
        // blockingly fetch heightmap in the enclosing scope
        // only if we have chunk generation can we do heightmap based queries
        HeightmapTimestampedCacheEntryWrapper heightMap = this.chunkManager != null ? this.heightMapCache.computeIfAbsent(cposSerialized, this::heightMap) : null;
        if (now > lastHeightMapCachePurgeAt + HEIGHT_MAP_CACHE_DURATION) {
            heightMapCache.values().removeIf(entry -> entry.lastAccess < now - HEIGHT_MAP_CACHE_DURATION);
            lastHeightMapCachePurgeAt = now;
        }
        // first, send a random check with high priority
        ChunkPos cpos = ChunkPos.fromLong(cposSerialized);
        askFor(cpos.origin().add(random.nextInt(16), random.nextInt(256), random.nextInt(16)), 56, now - RENEW_AGE);
        slurpChunkSeeds.inc();
        Set<BlockPos> toSeed = new HashSet<>();
        Set<BlockPos> toSeedHigh = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            // TODO increase this as we get more hits but failed slurp seedings in this location
            toSeed.add(cpos.origin().add(random.nextInt(16), random.nextInt(256), random.nextInt(16)));
        }
        for (int i = 0; i < NUM_RENEWALS; i++) {
            int dx = random.nextInt(16);
            int dz = random.nextInt(16);
            int x = cpos.getXStart() + dx;
            int z = cpos.getZStart() + dz;

            if (heightMap != null) {
                BlockPos pos = new BlockPos(x, heightMap.getHeightMapAt(dx, dz), z);
                // exception for the single most important one; the core gets a priority boost
                toSeedHigh.add(pos);
                toSeed.add(pos.add(0, 1, 0));
            }

            // also keep up to date any sky structures / sky bases...
            interestingYCoordsFromLastTime(x, z).forEach(y -> {
                // no need to check if y==pos.y, if it is equal it's fine since it'll dedup on prio and constant now-renew_age
                BlockPos skybase = new BlockPos(x, y, z);
                if (random.nextBoolean()) { // :zany_face:
                    toSeed.add(skybase);
                }
                if (random.nextBoolean()) { // :yum:
                    // randomly grab something a little ways above or below, just for fun
                    toSeed.add(skybase.add(0, (random.nextBoolean() ? 1 : -1) * (2 + random.nextInt(4)), 0));
                }
                // the cool part is that both cases (max and min) will overlap in the common case with the standard heightmap coords :)
                // so this is zero cost (except on decorator mismatch) other than when it's a true base to renew :)
                if (random.nextBoolean()) { // :woozy_face:
                    toSeed.add(skybase.add(0, 1, 0));
                }
                // adjacent cantilevers, e.g. map art
                if (random.nextBoolean()) { // :catflushed:
                    int offX = 0;
                    int offZ = 0;
                    if (random.nextBoolean()) {
                        offX = random.nextBoolean() ? 1 : -1;
                    } else {
                        offZ = random.nextBoolean() ? 1 : -1;
                    }
                    // note: this can cross chunk boundaries but, honestly, that's completely fine
                    toSeed.add(skybase.add(offX, 0, offZ));
                }
            });
        }
        slurpChunkSeeds.inc(toSeed.size());
        slurpChunkSeeds.inc(toSeedHigh.size());
        // after 2 seconds, we will know if it succeeded or is unloaded
        // so, only send the rest after 2 seconds
        // either it'll work fine (just delayed), or we'll save a dozen or so checks because it'll run up against BlockCheckManager's observedUnloaded cache!
        TrackyTrackyManager.scheduler.schedule(LoggingExecutor.wrap(() -> {
            for (BlockPos pos : toSeedHigh) {
                askFor(pos, 57, now - RENEW_AGE);
            }
            for (BlockPos pos : toSeed) {
                askFor(pos, 58, now - RENEW_AGE);
            }
        }), 2, TimeUnit.SECONDS);
    }

    private HeightmapTimestampedCacheEntryWrapper heightMap(long cpos) {
        // return: the Y coordinates of the first non-air block. 0 if fully air
        int[][] ret = new int[16][16];
        int[] data;
        try {
            data = this.chunkManager.getChunk(cpos).get();
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
        return new HeightmapTimestampedCacheEntryWrapper(ret);
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
        Map<ChunkPos, Long> chunkTimestamps = tmpBuffer.stream().collect(Collectors.groupingBy(cpwt -> ChunkPos.fromLong(cpwt.cpos), Collectors.reducing(0L, cpwt -> cpwt.timestamp, Math::max)));
        // first remove non cluster members, because otherwise clusterHit would get HUGE, instantly
        // (it would get like, every path taken by everyone for a day)
        // and that would suck to sort by distance
        try (Connection connection = Database.getConnection()) {
            synchronized (ingestLock) {
                long now = System.currentTimeMillis();
                Iterator<ChunkPos> it = chunkTimestamps.keySet().iterator();
                while (it.hasNext()) {
                    ChunkPos pos = it.next();
                    long cpos = pos.toLong();
                    if (clusterMembershipConfirmed.contains(cpos)) {
                        continue; // this pos has already passed this check previously
                    }
                    if (clusterHit.containsKey(cpos)) {
                        continue; // a nearby chunk has passed, so allow some expansion
                    }
                    long confirmedAt = clusterNonmembershipConfirmedAtCache.get(cpos);
                    if (confirmedAt != 0 && confirmedAt > now - CLUSTER_DATA_CACHE_DURATION) {
                        it.remove();
                        continue;
                    }
                    OptionalInt cluster = DBSCAN.INSTANCE.clusterMemberWithinRenderDistance(world.server.serverID, world.dimension, pos.x, pos.z, connection);
                    if (cluster.isPresent()) {
                        if (BLACKLISTED_CLUSTERS.contains(cluster.getAsInt())) {
                            blacklistedClusterSkips.inc();
                        } else {
                            clusterMembershipConfirmed.add(cpos);
                            continue;
                        }
                    }
                    it.remove();
                    clusterNonmembershipConfirmedAtCache.put(cpos, now);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
        // no need for a lock on clusterHit, since this is the only function that touches it, and this function is single threaded
        chunkTimestamps.forEach((pos, timestamp) -> {
            long unboxedTimeStamp = timestamp;
            for (int dx = -4; dx <= 4; dx++) {
                for (int dz = -4; dz <= 4; dz++) {
                    clusterHit.merge(pos.serializedAdd(dx, dz), unboxedTimeStamp, Math::max);
                }
            }
            clusterHitDirect.merge(pos.toLong(), unboxedTimeStamp, Math::max);
        });
        long now = System.currentTimeMillis();
        clusterHit.values().removeIf(ts -> ts < now - RENEW_AGE);

        Iterator<Long2LongMap.Entry> it = clusterHitDirect.long2LongEntrySet().fastIterator();
        while (it.hasNext()) {
            Long2LongMap.Entry entry = it.next();
            if (entry.getLongValue() < now - PENDING_RECHECK_AGE) {
                clusterHitDirectPrune.inc();
                clusterHitDirectPrunes.add(entry.getLongKey());
                it.remove();
            }
        }
    }

    public void clusterUpdate(ChunkPos cpos) { // dbscan informing us of a cluster update
        synchronized (ingestLock) {
            for (int dx = -4; dx <= 4; dx++) {
                for (int dz = -4; dz <= 4; dz++) { // due to our ranged check we need to invalidate EVERY nearby CNCAC member
                    clusterNonmembershipConfirmedAtCache.remove(cpos.serializedAdd(dx, dz));
                }
            }
        }
    }

    private synchronized void pruneAsks() {
        // re-paint-bucket everything
        allAsks.clear();
        askedAndGotUnloadedResponse.clear();
    }

    private void pruneClusterData() {
        // note: technically shouldn't be necessary due to live update but node stuff is complicated and dbscan might brainfart or miss the schedule who knows
        synchronized (ingestLock) {
            long now = System.currentTimeMillis();
            clusterNonmembershipConfirmedAtCache.values().removeIf(ts -> ts < now - CLUSTER_DATA_CACHE_DURATION);
        }
    }

    public void arbitraryHitExternal(ChunkPos pos) {
        long cpos = pos.toLong();
        world.blockCheckManager.loaded(cpos);
        arbitraryHit(cpos);
    }

    private synchronized void arbitraryHit(long cpos) {
        if (DBSCAN.INSTANCE.aggregateEligible(cpos)) {
            ingest.add(new ChunkPosWithTimestamp(cpos));
        }
        ResumeDataForChunk data = askedAndGotUnloadedResponse.remove(cpos);
        if (data == null) {
            return;
        }
        // bypass allAsks add since we took the data from there in the first place
        // simple retransmit
        ObjectIterator<Long2ObjectMap.Entry<FailedAsk>> failedBlockChunksIterator = data.failedBlockChecks.long2ObjectEntrySet().fastIterator();
        while (failedBlockChunksIterator.hasNext()) {
            Long2ObjectMap.Entry<FailedAsk> entry = failedBlockChunksIterator.next();
            long otherPos = entry.getLongKey();
            FailedAsk failedAsk = entry.getValue();

            doRawAsk(failedAsk.mustBeNewerThan, BlockPos.fromLong(otherPos), failedAsk.priority);
        }
        data.failedSignChecks.forEach((otherPos, mustBeNewerThan) -> doRawSign(mustBeNewerThan, otherPos));
    }

    private synchronized ResumeDataForChunk getData(long cpos) {
        return askedAndGotUnloadedResponse.computeIfAbsent(cpos, cpos0 -> new ResumeDataForChunk());
    }

    private synchronized void blockRecv(BlockPos pos, OptionalInt state, BlockCheckManager.BlockEventType type, long timestamp, int[] chunkData) {
        long bpos = pos.toLong();
        LongValue keyOffHeap = AskStatus.Helper.getAndSetKey(bpos);
        AskStatus askStat = allAsks.getUsing(keyOffHeap, AskStatus.Helper.ASK_STATUS_VALUE);
        if (askStat != null) {
            if (askStat.getReceivedAt() == timestamp) {
                return;
            }
            askStat.setReceivedAt(timestamp);
        }
        long cpos = BlockPos.blockToChunk(bpos);
        ResumeDataForChunk data = getData(cpos);

        if (!state.isPresent()) {
            // a miss
            // mark it as such, and we'll retry if we ever see this chunk reloaded! :)
            if (askStat != null) {
                data.failedBlockChecks.put(bpos, new FailedAsk(askStat));
                askStat.setResponse(AskStatus.NO_RESPONSE);
                allAsks.put(keyOffHeap, askStat); // also puts setReceivedAt from earlier
            }
            return;
        }
        // a hit
        // first, fixup unloaded responses
        data.failedBlockChecks.remove(bpos); // don't double ask
        if (type != BlockCheckManager.BlockEventType.CACHED) {
            arbitraryHit(cpos); // ask for the OTHER pending checks on this chunk
        }

        // now, what were we expecting?
        int blockState = state.getAsInt();
        if (isSign(blockState)) {
            if (signsAskedFor.add(pos)) {
                //System.out.println("NOT asking for sign at " + pos);
                //doRawSign(signBrushNewer(), pos);
            }
        }
        if (isShulker(blockState)) {
            //System.out.println("Shulker (blockstate " + blockState + ") at " + pos);
        }
        int expected = expected(bpos, pos, chunkData);
        if (askStat != null) {
            askStat.setResponse(blockState);
            allAsks.put(keyOffHeap, askStat); // also puts setReceivedAt from earlier
        }
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
        if (type == BlockCheckManager.BlockEventType.UPDATED) {
            //System.out.println("Expanding " + pos + " in brush mode");
            expand = true;
            expandBrush = true;
        } else { // either previous is null, or previous==current
            if (type == BlockCheckManager.BlockEventType.FIRST_TIME) {
                // first time matching graduates expand to expandBrush but it doesn't change something that matches generator
                expandBrush = true;
            }
            if (chunkData != null && blockState != expected) { // only if we actually have chunkData
                if (!(isStone(blockState) && isStone(expected))) { // stone variants are a troll, don't expand them
                    expand = true;
                }
            }
            // if current == expected, and updated is false, then there is absolutely no new information here
        }
        if (expand) {
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

    private synchronized int expected(long bpos, BlockPos pos, int[] chunkData) {
        AskStatus stat = allAsks.getUsing(AskStatus.Helper.getAndSetKey(bpos), AskStatus.Helper.ASK_STATUS_VALUE);
        if (stat != null) {
            int response = stat.getResponse();
            if (response != AskStatus.NO_RESPONSE) {
                return response;
            }
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
        long cpos = BlockPos.blockToChunk(pos.toLong());
        if (!nbt.isPresent()) {
            // schedule for retry
            getData(cpos).failedSignChecks.put(pos, mustBeNewerThan);
            return;
        }
        getData(cpos).failedSignChecks.remove(pos); // success
        arbitraryHit(cpos);
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
        long bpos = pos.toLong();
        LongValue keyOffHeap = AskStatus.Helper.getAndSetKey(bpos);
        AskStatus valueOffHeap = AskStatus.Helper.ASK_STATUS_VALUE;
        AskStatus cur = allAsks.getUsing(keyOffHeap, valueOffHeap);
        if (cur != null) {
            if (cur.getHighestPriorityAskedAt() <= priority && cur.getMustBeNewerThan() >= mustBeNewerThan) {
                return;
            }
        } else {
            cur = valueOffHeap;
        }
        cur.setHighestPriorityAskedAt(priority);
        cur.setMustBeNewerThan(mustBeNewerThan);
        cur.setResponse(AskStatus.NO_RESPONSE);
        cur.setLastDirectAsk(System.currentTimeMillis());
        cur.setReceivedAt(0);
        allAsks.put(keyOffHeap, cur);
        doRawAsk(mustBeNewerThan, pos, priority);
    }

    private void doRawAsk(long mustBeNewerThan, BlockPos pos, int priority) {
        world.blockCheckManager.requestBlockState(mustBeNewerThan, pos, priority, (state, type, timestamp) -> {
            if (chunkManager == null || !state.isPresent()) {
                blockRecvExecutor.execute(() -> blockRecv(pos, state, type, timestamp, null));
            } else {
                chunkManager.getChunk(BlockPos.blockToChunk(pos.toLong())).thenAcceptAsync(chunkData -> blockRecv(pos, state, type, timestamp, chunkData), blockRecvExecutor);
            }
        });
    }

    private void doRawSign(long mustBeNewerThan, BlockPos pos) {
        world.signManager.requestAsync(mustBeNewerThan, pos, nbt -> signRecv(pos, mustBeNewerThan, nbt));
    }

    private static class FailedAsk {

        private final int priority;
        private final long mustBeNewerThan;

        public FailedAsk(AskStatus stat) {
            this.priority = stat.getHighestPriorityAskedAt();
            this.mustBeNewerThan = stat.getMustBeNewerThan();
        }
    }

    private static class ResumeDataForChunk {

        private final Long2ObjectOpenHashMap<FailedAsk> failedBlockChecks = new Long2ObjectOpenHashMap<>();
        private final Map<BlockPos, Long> failedSignChecks = new HashMap<>();
    }

    public interface AskStatus {

        int NO_RESPONSE = -1;

        int getHighestPriorityAskedAt();

        void setHighestPriorityAskedAt(final int highestPriorityAskedAt);

        long getMustBeNewerThan();

        void setMustBeNewerThan(final long mustBeNewerThan);

        int getResponse();

        void setResponse(final int response);

        long getLastDirectAsk();

        void setLastDirectAsk(final long lastDirectAsk);

        long getReceivedAt();

        void setReceivedAt(final long receivedAt);

        class Helper {

            public static final LongValue LONG_VALUE_KEY = Values.newHeapInstance(LongValue.class);
            public static final AskStatus ASK_STATUS_VALUE = Values.newHeapInstance(AskStatus.class);

            static LongValue getAndSetKey(final long keyValue) {
                final LongValue value = LONG_VALUE_KEY;
                value.setValue(keyValue); // TODO: figure out how to stop proguard from removing this
                final long newValue = value.getValue();
                if (newValue != keyValue) {
                    throw new IllegalStateException("Unexpected value in LongValue? Expected " + keyValue + " but was " + newValue);
                }
                return value;
            }
        }
    }

    private static class ChunkPosWithTimestamp {

        public final long cpos;
        public final long timestamp;

        public ChunkPosWithTimestamp(long cpos) {
            this.cpos = cpos;
            this.timestamp = System.currentTimeMillis();
        }
    }

    private static class HeightmapTimestampedCacheEntryWrapper {

        private final byte[] compactMap;
        private long lastAccess;

        private HeightmapTimestampedCacheEntryWrapper(int[][] data) {
            this.compactMap = new byte[256];
            for (int x = 0; x < 16; x++) {
                for (int z = 0; z < 16; z++) {
                    compactMap[x * 16 + z] = (byte) data[x][z];
                }
            }
            this.lastAccess = System.currentTimeMillis();
        }

        public int getHeightMapAt(int x, int z) {
            lastAccess = System.currentTimeMillis();
            int height = compactMap[x * 16 + z];
            return height & 0xff;
        }
    }
}
