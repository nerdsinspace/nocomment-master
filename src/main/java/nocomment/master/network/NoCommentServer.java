package nocomment.master.network;

import nocomment.master.Server;
import nocomment.master.World;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class NoCommentServer {
    private static void handleNewSocket(Socket s) throws IOException {
        DataInputStream in = new DataInputStream(s.getInputStream());
        String serverName = in.readUTF();
        int dim = in.readInt();
        System.out.println("Connection! " + serverName + " " + dim);
        World world = Server.getServer(serverName).getWorld(dim);
        world.incomingConnection(new SocketConnection(world, s));
    }

    public static void listen() throws IOException {
        ServerSocket ss = new ServerSocket(42069);
        System.out.println("SS");
        while (true) {
            Socket s = ss.accept();
            System.out.println("S");
            handleNewSocket(s);
        }
    }
}
