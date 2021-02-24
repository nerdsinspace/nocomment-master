package nocomment.master.slurp;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import nocomment.master.NoComment;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.Config;
import nocomment.master.util.Telegram;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class ChunkManager {
    private static final Gauge chunkCache = Gauge.build()
            .name("chunk_manager_cache")
            .help("Number of chunk positions by state")
            .labelNames("state")
            .register();

    private static final Counter chunkRequests = Counter.build()
            .name("chunk_manager_requests_total")
            .help("Number of chunks requested")
            .register();

    private static final Counter chunkResponses = Counter.build()
            .name("chunk_manager_responses_total")
            .help("Number of chunks received")
            .register();

    private static final int MAX_SIZE = 2048; // about 500MB RAM
    private final Long2LongOpenHashMap lastAccessed = new Long2LongOpenHashMap(); // ChunkPos, Time
    private final Long2ObjectOpenHashMap<CompletableFuture<int[]>> cache = new Long2ObjectOpenHashMap<>(); // ChunkPos
    private final LinkedBlockingQueue<Long> queue = new LinkedBlockingQueue<>();

    public synchronized CompletableFuture<int[]> getChunk(long cpos) {
        chunkRequests.inc();
        lastAccessed.put(cpos, System.currentTimeMillis());
        return cache.computeIfAbsent(cpos, $ -> {
            queue.add(cpos);
            chunkCache.labels("queued").inc();
            CompletableFuture<int[]> ret = new CompletableFuture<>(); // this is OK since we hold the lock until after it's inserted
            ret.thenAccept(ignored -> {
                chunkCache.labels("queued").dec();
                chunkCache.labels("done").inc();
            });
            return ret;
        });
    }

    {
        NoComment.executor.execute(() -> {
            try {
                while (true) {
                    try {
                        fetchLoop();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    Telegram.INSTANCE.sendMessage("Disconnected from generator");
                    Thread.sleep(TimeUnit.SECONDS.toMillis(60));
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        });
    }

    private void fetchLoop() throws IOException, InterruptedException {
        Socket s = new Socket(Config.getRuntimeVariable("GENERATOR_HOST", "localhost"), Integer.parseInt(Config.getRuntimeVariable("GENERATOR_PORT", "5021")));
        DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        while (true) {
            while (queue.isEmpty()) {
                // there is no blocking wait to wait until a queue is nonempty, without actually popping the head
                // and we don't want to pop the head :(
                Thread.sleep(5);
            }
            long cpos = queue.peek();
            ChunkPos pos = ChunkPos.fromLong(cpos);
            //System.out.println("Requesting pos from world gen: " + pos);
            out.writeInt(pos.x);
            out.writeInt(pos.z);
            out.flush();
            int[] ret = new int[65536];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = in.readInt();
            }
            //System.out.println("Received chunk from world gen: " + pos);
            //System.out.println("Cache map size is " + cache.size() + " and total age is " + num);
            chunkResponses.inc();
            synchronized (this) {
                cache.get(cpos).complete(ret);
                while (cache.size() - queue.size() > MAX_SIZE) { // obv can't use cache.size
                    Optional<Long2ObjectMap.Entry<CompletableFuture<int[]>>> toPrune = cache.long2ObjectEntrySet().stream()
                            .filter(entry -> entry.getValue().isDone())
                            .min(Comparator.comparingLong(entry -> lastAccessed.get(entry.getLongKey())));
                    if (!toPrune.isPresent()) {
                        break;
                    }
                    cache.remove(toPrune.get().getLongKey());
                    chunkCache.labels("done").dec();
                }
            }
            queue.poll();
        }
    }
}
