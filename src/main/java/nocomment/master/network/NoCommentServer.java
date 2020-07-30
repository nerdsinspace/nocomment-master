package nocomment.master.network;

import io.prometheus.client.Counter;
import nocomment.master.Server;
import nocomment.master.World;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public enum NoCommentServer {
    // cat /dev/random | head -c 100000000 | shasum -a 512

    SLAVE_STATUS("5b7937b0a4a72351455193c9bc0776b84dd5a9b615c1426e4b6c658b12ef41882b401af1ac0adac0d9d5f1ff20e17d4581e0634c2075433a60f0f42d4cfe37f4", "v1", QueueStatus::handle),

    SLAVE_DATA("0d7119c0a25e82e5c36d5188dcce4090d5ff9813a36a6fef6a0b3aca051b253a1b3c345452f23f2564403012abe98e20d3eb5f4191d3f8907e9ceb505ba0c2ba", "v3", withWorld((s, world) -> world.incomingConnection(new SocketConnection(world, s)))),

    BLOCK("316667dc06d7ec96cd090c91dea7092bf43e6639ad53e3d68e8ec30511280af80504ac026490bb194ea1c0309a3c437f479e3de258688ad29d139ff00dcc0911", "v3", withWorld(BlockAPI::handle)),

    SHITPOST("c34d05a79be75c0a003ff9bb063883f5fbcb2026293ed21122c62f770f3e78fcfef969ff7186719e2d54c1891a744f3e678d0b485a847198e337b246b9167161", "v1", ShitpostAPI::handle);

    private static final Counter sockets = Counter.build()
            .name("accepted_sockets_total")
            .help("Number of accepted sockets")
            .register();

    private static final Counter failedHandshakes = Counter.build()
            .name("failed_handshakes_total")
            .help("Number of failed sockets e.g. wrong magic")
            .register();

    private static final Counter acceptedHandshakes = Counter.build()
            .name("accepted_handshakes_total")
            .help("Number of accepted handshakes")
            .labelNames("magic")
            .register();

    private final String magic;
    private final String version;
    private final SocketConsumer onConnection;

    NoCommentServer(String magic, String version, SocketConsumer onConnection) {
        this.magic = magic;
        this.version = version;
        this.onConnection = onConnection;
    }

    private static void handleNewSocket(Socket s) {
        sockets.inc();
        DataInputStream in;
        String recv;
        try {
            in = new DataInputStream(s.getInputStream());
            recv = in.readUTF(); // limits to 65536 so this is fine
        } catch (IOException ex) {
            failedHandshakes.inc();
            reallyClose(s);
            return;
        }
        String magic = recv.split(" ")[0];
        String version = recv.split(" ")[1];
        NoCommentServer server = getByMagic(magic);
        System.out.println(magic + " " + version + " " + server + " " + recv);
        if (server == null) {
            failedHandshakes.inc();
            reallyClose(s);
            return;
        }
        if (!server.version.equals(version)) {
            System.out.println("Received a connection with version \"" + version + "\" while we expected \"" + server.version + "\". Dropping");
            failedHandshakes.inc();
            reallyClose(s);
            return;
        }
        acceptedHandshakes.labels(server.name()).inc();
        try {
            System.out.println(server + " connection");
            server.onConnection.consume(s, in);
        } catch (IOException ex) {
            ex.printStackTrace();
            reallyClose(s);
        }
    }

    private static void reallyClose(Socket s) {
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

    private static NoCommentServer getByMagic(String magic) {
        for (NoCommentServer s : values()) {
            if (s.magic.equals(magic)) {
                return s;
            }
        }
        return null;
    }

    private static SocketConsumer withWorld(WorldContextConsumer wcc) {
        return (s, in) -> {
            String serverName = in.readUTF();
            if (serverName.endsWith(":25565")) {
                serverName = serverName.split(":25565")[0];
            }
            int dim = in.readInt();
            System.out.println("Connection! " + serverName + " " + dim);
            World world = Server.getServer(serverName).getWorld((short) dim);
            wcc.consume(s, world);
        };
    }

    @FunctionalInterface
    private interface SocketConsumer {

        void consume(Socket s, DataInputStream in) throws IOException;
    }

    @FunctionalInterface
    private interface WorldContextConsumer {

        void consume(Socket s, World world) throws IOException;
    }

    public static void listen() {
        try {
            int port = 42069;
            ServerSocket ss = new ServerSocket(port);
            System.out.println("Server listening on port " + port);
            while (true) {
                Socket s = ss.accept();
                System.out.println("Server accepted a socket");
                new Thread(() -> handleNewSocket(s)).start();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
