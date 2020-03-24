package nocomment.master.network;

import nocomment.master.World;
import nocomment.master.task.Task;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.OnlinePlayer;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SocketConnection extends Connection {
    private final Socket sock;
    private final DataOutputStream out;
    private final Object sockWriteLock = new Object();

    public SocketConnection(World world, Socket sock) throws IOException {
        super(world);
        this.sock = sock;
        this.out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
    }

    @Override
    protected void dispatchTask(Task task, int taskID) {
        synchronized (sockWriteLock) {
            try {
                out.writeByte(0);
                out.writeInt(taskID);
                out.writeInt(task.priority);
                out.writeInt(task.start.x);
                out.writeInt(task.start.z);
                out.writeInt(task.directionX);
                out.writeInt(task.directionZ);
                out.writeInt(task.count);
                out.flush();
            } catch (IOException ex) {
                closeUnderlying();
            }
        }
    }

    @Override
    protected void read() throws IOException {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        byte cmd = in.readByte();

        switch (cmd) {
            case 0: { // hit
                int taskID = in.readInt();
                int x = in.readInt();
                int z = in.readInt();
                hitReceived(taskID, new ChunkPos(x, z));
                break;
            }
            case 1: { // done
                int taskID = in.readInt();
                taskCompleted(taskID);
                break;
            }
            case 2: { // player add remove
                boolean join = in.readBoolean();
                OnlinePlayer player = new OnlinePlayer(in);
                playerJoinLeave(join, player);
                break;
            }
            case 69: { // ping
                break;
            }
            default:
                throw new RuntimeException("what " + cmd);
        }
    }

    @Override
    protected void closeUnderlying() {
        try {
            sock.close();
        } catch (Throwable th) {}
    }
}