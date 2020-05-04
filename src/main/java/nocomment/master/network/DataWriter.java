package nocomment.master.network;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface DataWriter {
    void write(DataOutputStream out) throws IOException;

    static void writeLoop(LinkedBlockingQueue<DataWriter> queue, Socket sock) throws IOException, InterruptedException {
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
        List<DataWriter> writers = new ArrayList<>();
        while (!sock.isClosed()) {
            // take at least one, blockingly
            DataWriter w = queue.poll(250, TimeUnit.MILLISECONDS); // short timeout
            if (w == null) {
                // recheck if the socket is closed at least every 250ms
                continue;
            }
            w.write(out);
            if (!queue.isEmpty()) {
                queue.drainTo(writers); // if there are more, take them, but don't block waiting for one
                for (DataWriter writer : writers) {
                    writer.write(out);
                }
                writers.clear();
            }
            out.flush();
        }
    }
}