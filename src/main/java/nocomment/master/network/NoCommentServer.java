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
            String password = in.readUTF();
            if (!password.equals("0d7119c0a25e82e5c36d5188dcce4090d5ff9813a36a6fef6a0b3aca051b253a1b3c345452f23f2564403012abe98e20d3eb5f4191d3f8907e9ceb505ba0c2ba")) {
                s.close();
                return;
            }
            String serverName = in.readUTF();
            if (serverName.endsWith(":25565")) {
                serverName = serverName.split(":25565")[0];
            }
            serverName += "_temp_map_apr_1_2020";
            int dim = in.readInt();
            System.out.println("Connection! " + serverName + " " + dim);
            World world = Server.getServer(serverName).getWorld(dim);
            world.incomingConnection(new SocketConnection(world, s));
        } catch (IOException ex) {
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
