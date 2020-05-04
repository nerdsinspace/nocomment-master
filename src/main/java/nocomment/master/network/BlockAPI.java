package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.util.BlockCheckManager;
import nocomment.master.util.BlockPos;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockAPI {

    private final Socket sock;
    private final BlockCheckManager bcm;
    private final LinkedBlockingQueue<DataWriter> queue;

    private BlockAPI(Socket s, BlockCheckManager bcm) {
        this.sock = s;
        this.bcm = bcm;
        this.queue = new LinkedBlockingQueue<>();
    }

    private void readLoop() throws IOException {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        while (true) {
            int x = in.readInt();
            int y = in.readInt();
            int z = in.readInt();
            int priority = in.readInt();
            long mustBeNewerThan = in.readLong();
            bcm.requestBlockState(mustBeNewerThan, new BlockPos(x, y, z), priority, opt -> queue.add(out -> {
                out.writeInt(x);
                out.writeInt(y);
                out.writeInt(z);
                out.writeBoolean(opt.isPresent());
                if (opt.isPresent()) {
                    out.writeInt(opt.getAsInt());
                }
            }));
        }
    }

    private void writeLoop() {
        try {
            DataWriter.writeLoop(queue, sock);
        } catch (IOException | InterruptedException ex) {
            closeUnderlying();
        }
    }

    public static void handle(Socket s, World world) throws IOException {
        BlockAPI api = new BlockAPI(s, world.blockCheckManager);
        NoComment.executor.execute(api::writeLoop);
        api.readLoop();
    }

    protected void closeUnderlying() {
        try {
            sock.close();
        } catch (Throwable th) {
            th.printStackTrace();
        }
        try {
            sock.getInputStream().close();
        } catch (Throwable th) {
            th.printStackTrace();
        }
        try {
            sock.getOutputStream().close();
        } catch (Throwable th) {
            th.printStackTrace();
        }
    }
}
