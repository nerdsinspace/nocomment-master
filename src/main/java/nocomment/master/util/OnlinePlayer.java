package nocomment.master.util;

import java.io.DataInputStream;
import java.io.IOException;

public class OnlinePlayer {
    public final String uuid;
    public final String username;

    public OnlinePlayer(DataInputStream in) throws IOException {
        this.uuid = in.readUTF();
        this.username = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof OnlinePlayer && ((OnlinePlayer) o).uuid.equals(uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }
}
