package nocomment.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class NoComment {

    public static Executor executor = Executors.newCachedThreadPool(); // has no maximum

    private final Map<String, Server> servers = new HashMap<>();

    public Server getServer(String serverName) {
        return servers.computeIfAbsent(serverName, sn -> new Server());
    }


    public void handleNewSocket(Socket s) throws IOException {
        DataInputStream in = new DataInputStream(s.getInputStream());
        String serverName = in.readUTF();
        int dim = in.readInt();
        System.out.println("Connection! " + serverName + " " + dim);
        World world = getServer(serverName).getWorld(dim);
        world.incomingConnection(new SocketConnection(world, s));
    }

    public static void main(String[] args) throws IOException {
        NoComment nc = new NoComment();
        // nc.getServer("constantiam.net").getWorld(0).submitTask(new TestingTask(0, new ChunkPos(0, 0), -9, 0, 26043));

        new TrackyTrackyManager(nc.getServer("2b2t.org"));
        ServerSocket ss = new ServerSocket(42069);
        System.out.println("SS");
        while (true) {
            Socket s = ss.accept();
            System.out.println("S");
            nc.handleNewSocket(s);
        }
        /*DataInputStream in = new DataInputStream(s.getInputStream());
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        System.out.println("Server: " + in.readUTF());
        System.out.println("DIM: " + in.readInt());*/
        /*writeTask(out, 0, 0, 0, 0, 9, 0, 30000000);
        writeTask(out, 1, 1, 0, 0, -9, 0, 30000000);
        writeTask(out, 2, 2, 0, 0, 0, 9, 30000000);
        writeTask(out, 3, 4, 0, 0, 0, -9, 30000000);

        writeTask(out, 4, 5, 0, 0, 8, 8, 30000000);
        writeTask(out, 5, 6, 0, 0, -8, -8, 30000000);
        writeTask(out, 6, 7, 0, 0, -8, 8, 30000000);
        writeTask(out, 7, 8, 0, 0, 8, -8, 30000000);*/

        /*writeTask(out, 3, -10, 0, 0, 3, 0, 1000000);
        writeTask(out, 4, -10, 0, 0, 4, 0, 1000000);
        writeTask(out, 5, -10, 0, 0, 5, 0, 1000000);
        writeTask(out, 6, -10, 0, 0, 6, 0, 1000000);*/
        /*writeTask(out, 7, -10, 0, 0, 7, 7, 1000000);
        writeTask(out, 8, -10, 0, 0, 8, 8, 1000000);
        writeTask(out, 9, -10, 0, 0, 9, 9, 1000000);
        writeTask(out, 10, -10, 0, 0, 10, 10, 1000000);
        writeTask(out, 11, -10, 0, 0, 11, 11, 1000000);
        writeTask(out, 12, -10, 0, 0, 12, 12, 1000000);*/
       /* writeTask(out, 13, -10, 0, 0, 13, 0, 1000000);
        writeTask(out, 14, -10, 0, 0, 14, 0, 1000000);
        writeTask(out, 15, -10, 0, 0, 15, 0, 1000000);
        writeTask(out, 16, -10, 0, 0, 16, 0, 1000000);
        writeTask(out, 17, -10, 0, 0, 17, 0, 1000000);
        writeTask(out, 18, -10, 0, 0, 18, 0, 1000000);
        writeTask(out, 19, -10, 0, 0, 19, 0, 1000000);
        writeTask(out, 20, -10, 0, 0, 20, 0, 1000000);*/
    }

    public static class TestingTask extends Task {

        public TestingTask(int priority, ChunkPos start, int directionX, int directionZ, int count) {
            super(priority, start, directionX, directionZ, count);
        }

        @Override
        public void hitReceived(ChunkPos pos) {
            System.out.println("Hit received at " + pos);
        }

        @Override
        public void completed() {
            System.out.println("Completed");
        }
    }

    public static void writeTask(DataOutputStream out, int taskID, int priority, int startX, int startZ, int directionX, int directionZ, int overworldDistance) throws IOException {
        out.writeLong(taskID);
        out.writeInt(priority);
        out.writeInt(startX);
        out.writeInt(startZ);
        out.writeInt(directionX);
        out.writeInt(directionZ);
        out.writeInt(1 + (int) Math.ceil(overworldDistance / 16f / 8 / Math.max(Math.abs(directionX), Math.abs(directionZ))));
    }
}
