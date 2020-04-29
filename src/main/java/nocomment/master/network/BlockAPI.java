package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.Server;
import nocomment.master.World;
import nocomment.master.util.BlockCheckManager;
import nocomment.master.util.BlockPos;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class BlockAPI {

    private final Socket s;
    private final BlockCheckManager bcm;

    private BlockAPI(Socket s, BlockCheckManager bcm) {
        this.s = s;
        this.bcm = bcm;
    }

    private void run() throws IOException {
        DataInputStream in = new DataInputStream(s.getInputStream());
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
        while (true) {
            int x = in.readInt();
            int y = in.readInt();
            int z = in.readInt();
            int priority = in.readInt();
            long mustBeNewerThan = in.readLong();
            bcm.requestBlockState(mustBeNewerThan, new BlockPos(x, y, z), priority, opt -> {
                try {
                    synchronized (this) {
                        out.writeInt(x);
                        out.writeInt(y);
                        out.writeInt(z);
                        out.writeBoolean(opt.isPresent());
                        if (opt.isPresent()) {
                            out.writeInt(opt.getAsInt());
                        }
                        out.flush();
                    }
                } catch (IOException ex) {
                    try {
                        s.close();
                    } catch (Throwable th) {
                    }
                }
            });
        }
    }

    private static void handleNewSocket(Socket s) {
        try {
            DataInputStream in = new DataInputStream(s.getInputStream());
            String serverName = in.readUTF();
            if (serverName.endsWith(":25565")) {
                serverName = serverName.split(":25565")[0];
            }
            int dim = in.readInt();
            System.out.println("Block API connection! " + serverName + " " + dim);
            World world = Server.getServer(serverName).getWorld((short) dim);
            new BlockAPI(s, world.blockCheckManager).run();
        } catch (IOException ex) {
            try {
                s.close();
            } catch (Throwable th) {
            }
            try {
                s.getInputStream().close();
            } catch (Throwable th) {
            }
            try {
                s.getOutputStream().close();
            } catch (Throwable th) {
            }
        }
    }

    public static void listen() throws IOException {
        int port = 46290;
        ServerSocket ss = new ServerSocket(port);
        System.out.println("Server listening on port " + port);
        while (true) {
            Socket s = ss.accept();
            System.out.println("Server accepted a socket");
            NoComment.executor.execute(() -> handleNewSocket(s));
        }
    }
}
