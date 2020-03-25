package nocomment.master.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.UUID;

public class OnlinePlayer {
    public final UUID uuid;
    public final String username;

    public OnlinePlayer(DataInputStream in) throws IOException {
        this.uuid = UUID.fromString(in.readUTF());
        String usr = in.readUTF();
        this.username = usr.equals("") ? null : usr;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof OnlinePlayer && ((OnlinePlayer) o).uuid.equals(uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    public boolean hasUsername() {
        return username != null;
    }
}
