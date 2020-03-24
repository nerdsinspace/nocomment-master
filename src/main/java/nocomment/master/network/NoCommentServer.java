package nocomment.master.network;

import nocomment.master.NoComment;
import nocomment.master.Server;
import nocomment.master.World;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class NoCommentServer {
    private static void handleNewSocket(Socket s) {
        try {
            DataInputStream in = new DataInputStream(s.getInputStream());
            String serverName = in.readUTF();
            int dim = in.readInt();
            System.out.println("Connection! " + serverName + " " + dim);
            World world = Server.getServer(serverName).getWorld(dim);
            world.incomingConnection(new SocketConnection(world, s));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void listen() throws IOException {
        int port = 42069;
        ServerSocket ss = new ServerSocket(port);
        System.out.println("Server listening on port " + port);
        while (true) {
            Socket s = ss.accept();
            System.out.println("Server accepted a socket");
            NoComment.executor.execute(() -> handleNewSocket(s));
        }
    }
}
