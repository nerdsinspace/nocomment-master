package nocomment.master.network;

import nocomment.master.db.Database;
import nocomment.master.util.OnlinePlayer;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Optional;

public class QueueStatus {

    // any "queue" messages are about this server
    private static final short QUEUE_SERVER_ID = Database.idForServer("2b2t.org");

    public static void handle(Socket s, DataInputStream in) throws IOException {
        String uuid = in.readUTF();
        int queuePos = in.readInt();
        int playerID = Database.idForPlayer(new OnlinePlayer(uuid));
        Database.updateStatus(playerID, QUEUE_SERVER_ID, "QUEUE", Optional.of("Queue position: " + queuePos));
    }
}
