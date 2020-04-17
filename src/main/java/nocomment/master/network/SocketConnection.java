package nocomment.master.network;

import nocomment.master.World;
import nocomment.master.task.BlockCheckManager;
import nocomment.master.task.Task;
import nocomment.master.util.BlockPos;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.OnlinePlayer;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.OptionalInt;

public class SocketConnection extends Connection {
    private final Socket sock;
    private final DataOutputStream out;
    private final Object sockWriteLock = new Object();
    private final String uuid;

    public SocketConnection(World world, Socket sock) throws IOException {
        super(world);
        this.sock = sock;
        this.uuid = new DataInputStream(sock.getInputStream()).readUTF();
        this.out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
    }

    @Override
    public String getUUID() {
        return uuid;
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
    protected void dispatchBlockCheck(BlockCheckManager.BlockCheck check) {
        synchronized (sockWriteLock) {
            try {
                out.writeByte(1);
                out.writeInt(check.pos.x);
                out.writeInt(check.pos.y);
                out.writeInt(check.pos.z);
                out.writeInt(check.priority);
                out.flush();
            } catch (IOException ex) {
                closeUnderlying();
            }
        }
    }

    @Override
    protected void dispatchDisconnectRequest() {
        synchronized (sockWriteLock) {
            try {
                out.writeByte(2);
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
            case 3: {
                int x = in.readInt();
                int y = in.readInt();
                int z = in.readInt();
                OptionalInt blockState = in.readBoolean() ? OptionalInt.of(in.readInt()) : OptionalInt.empty();
                checkCompleted(new BlockPos(x, y, z), blockState);
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

    @Override
    public String toString() {
        return super.toString() + ", underlying socket " + sock;
    }
}