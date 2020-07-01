package nocomment.master.slurp;

import nocomment.master.NoComment;
import nocomment.master.util.ChunkPos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChunkManager {
    private final int MAX_SIZE = 2048; // about 500MB RAM
    private final Map<ChunkPos, Long> lastAccessed = new HashMap<>();
    private final Map<ChunkPos, CompletableFuture<int[]>> cache = new HashMap<>();
    private final LinkedBlockingQueue<ChunkPos> queue = new LinkedBlockingQueue<>();

    public synchronized CompletableFuture<int[]> getChunk(ChunkPos pos) {
        lastAccessed.put(pos, System.currentTimeMillis());
        return cache.computeIfAbsent(pos, $ -> {
            queue.add(pos);
            return new CompletableFuture<>(); // this is OK since we hold the lock until after it's inserted
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
                    Thread.sleep(TimeUnit.SECONDS.toMillis(60));
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        });
    }

    private void fetchLoop() throws IOException, InterruptedException {
        Socket s = new Socket("localhost", 5021);
        DataInputStream in = new DataInputStream(s.getInputStream());
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        int num = 0;
        while (true) {
            while (queue.isEmpty()) {
                // there is no blocking wait to wait until a queue is nonempty, without actually popping the head
                // and we don't want to pop the head :(
                Thread.sleep(5);
            }
            ChunkPos pos = queue.peek();
            System.out.println("Requesting pos from world gen: " + pos);
            out.writeInt(pos.x);
            out.writeInt(pos.z);
            out.flush();
            int[] ret = new int[65536];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = in.readInt();
            }
            System.out.println("Received chunk from world gen: " + pos);
            System.out.println("Cache map size is " + cache.size() + " and total age is " + num);
            synchronized (this) {
                cache.get(pos).complete(ret);
                if (num++ > MAX_SIZE) { // obv can't use cache.size
                    cache.keySet().stream()
                            .sorted(Comparator.comparingLong(lastAccessed::get))
                            .filter(cpos -> cache.get(cpos).isDone())
                            .findFirst()
                            .ifPresent(cache::remove);
                }
            }
            queue.poll();
        }
    }
}
