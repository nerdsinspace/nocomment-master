package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.World;
import nocomment.master.slurp.BlockCheckManager;
import nocomment.master.slurp.SignManager;
import nocomment.master.util.BlockPos;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public final class BlockAPI {

    private final Socket sock;
    private final BlockCheckManager bcm;
    private final SignManager sm;
    private final LinkedBlockingQueue<DataWriter> queue;

    private BlockAPI(Socket s, World world) {
        this.sock = s;
        this.bcm = world.blockCheckManager;
        this.sm = world.signManager;
        this.queue = new LinkedBlockingQueue<>();
    }

    private void readLoop() throws IOException {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        while (true) {
            byte mode = in.readByte();
            int x = in.readInt();
            int y = in.readInt();
            int z = in.readInt();
            switch (mode) {
                case 0: {
                    int priority = in.readInt();
                    long mustBeNewerThan = in.readLong();
                    bcm.requestBlockState(mustBeNewerThan, new BlockPos(x, y, z), priority, (opt, type, timestamp) -> queue.add(out -> {
                        out.writeByte(0);
                        out.writeInt(x);
                        out.writeInt(y);
                        out.writeInt(z);
                        out.writeBoolean(opt.isPresent());
                        if (opt.isPresent()) {
                            out.writeInt(opt.getAsInt());
                        }
                        out.writeBoolean(type == BlockCheckManager.BlockEventType.UPDATED);
                    }));
                    break;
                }
                case 1: {
                    long mustBeNewerThan = in.readLong();
                    sm.request(mustBeNewerThan, new BlockPos(x, y, z), opt -> queue.add(out -> {
                        out.writeByte(1);
                        out.writeInt(x);
                        out.writeInt(y);
                        out.writeInt(z);
                        out.writeBoolean(opt.isPresent());
                        if (opt.isPresent()) {
                            out.writeInt(opt.get().length);
                            out.write(opt.get());
                        }
                    }));
                    break;
                }
            }
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
        BlockAPI api = new BlockAPI(s, world);
        Connection.networkExecutor.execute(api::writeLoop);
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
