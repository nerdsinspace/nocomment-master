package nocomment.master.network;

import io.prometheus.client.Gauge;
import nocomment.master.db.AsyncBatchCommitter;
import nocomment.master.db.Database;
import nocomment.master.util.OnlinePlayer;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class QueueStatus {

    private static final Gauge queuePosition = Gauge.build()
            .name("queue_position")
            .help("Position in queue")
            .labelNames("username")
            .register();

    // any "queue" messages are about this server
    private static final short QUEUE_SERVER_ID = Database.idForServer("2b2t.org");

    private static final Map<Integer, IndividualQueueState> cache = new HashMap<>();

    private static final List<QueueDiff> events = new ArrayList<>();

    private static class IndividualQueueState {

        private final int position;
        private final long timestamp;

        private IndividualQueueState(int position, long timestamp) {
            this.position = position;
            this.timestamp = timestamp;
        }
    }

    private static final class QueueDiff {

        private final int pos;
        private final long start;
        private final long end;

        private QueueDiff(int pos, long start, long end) {
            this.pos = pos;
            this.start = start;
            this.end = end;
        }
    }

    public static void markIngame(int playerID, short serverID) {
        if (serverID != QUEUE_SERVER_ID) {
            return;
        }
        queuePosition.remove(playerID + "");
    }

    public static void handle(Socket s, DataInputStream in) throws IOException {
        String uuid = in.readUTF();
        int queuePos = in.readInt();
        long now = System.currentTimeMillis();

        int playerID = Database.idForPlayer(new OnlinePlayer(uuid));
        AsyncBatchCommitter.submit(conn -> Database.updateStatus(conn, playerID, QUEUE_SERVER_ID, "QUEUE", Optional.of("Queue position: " + queuePos), now));
        Database.getUsername(playerID).ifPresent(username -> queuePosition.labels(username).set(queuePos));
        synchronized (cache) {
            if (cache.containsKey(playerID)) {
                long prevTime = cache.get(playerID).timestamp;
                long dist = now - prevTime;
                if (dist > TimeUnit.SECONDS.toMillis(5)) {
                    int prevPos = cache.get(playerID).position;

                    events.add(new QueueDiff(prevPos - queuePos, prevTime, now));
                }
            }
            cache.put(playerID, new IndividualQueueState(queuePos, now));
        }
    }

    public static long getEstimatedMillisecondsPerQueuePosition() {
        synchronized (cache) {
            long rem = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
            events.stream()
                    .filter(td -> td.end > rem)
                    .findFirst()
                    .map(events::indexOf)
                    .map(ind -> events.subList(0, ind))
                    .ifPresent(List::clear);
            if (events.size() > 100) {
                int pos = events.stream().mapToInt(event -> event.pos).sum();
                long duration = events.stream().mapToLong(event -> event.end - event.start).sum();
                long est = Math.round((double) duration / (double) pos);
                System.out.println("MS per queue position estimated as " + est);
                return est;
            }
            return TimeUnit.SECONDS.toMillis(30);
        }
    }
}
