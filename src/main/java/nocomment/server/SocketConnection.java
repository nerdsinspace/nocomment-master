package nocomment.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SocketConnection extends Connection {
    private final Socket sock;

    public SocketConnection(World world, Socket sock) {
        super(world);
        this.sock = sock;
    }

    @Override
    protected void dispatchTask(Task task, long taskID) {
        try {
            DataOutputStream out = new DataOutputStream(sock.getOutputStream());
            out.writeLong(taskID);
            out.writeInt(task.priority);
            out.writeInt(task.start.x);
            out.writeInt(task.start.z);
            out.writeInt(task.directionX);
            out.writeInt(task.directionZ);
            out.writeInt(task.count);
        } catch (IOException ex) {
            closeUnderlying();
        }
    }

    @Override
    protected void read() throws IOException {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        byte cmd = in.readByte();

        switch (cmd) {
            case 0: { // hit
                long taskID = in.readLong();
                int x = in.readInt();
                int z = in.readInt();
                hitReceived(taskID, new ChunkPos(x, z));
                break;
            }
            case 1: { // done
                long taskID = in.readLong();
                taskCompleted(taskID);
                break;
            }
            case 69: { // ping
                break;
            }
            default:
                throw new RuntimeException("what  " + cmd);
        }
    }

    @Override
    protected void closeUnderlying() {
        try {
            sock.close();
        } catch (Throwable th) {}
    }
}