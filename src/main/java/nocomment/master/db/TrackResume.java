package nocomment.master.db;

import nocomment.master.util.ChunkPos;

public class TrackResume {
    public final ChunkPos pos;
    public final short dimension;
    public final long prevTrackID;

    TrackResume(int x, int z, short dimension, long prevTrackID) {
        this.pos = new ChunkPos(x, z);
        this.dimension = dimension;
        this.prevTrackID = prevTrackID;
    }
}
