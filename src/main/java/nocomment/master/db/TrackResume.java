package nocomment.master.db;

import nocomment.master.util.ChunkPos;

public final class TrackResume {
    public final ChunkPos pos;
    public final short dimension;
    public final int prevTrackID;

    TrackResume(int x, int z, short dimension, int prevTrackID) {
        this.pos = new ChunkPos(x, z);
        this.dimension = dimension;
        this.prevTrackID = prevTrackID;
    }
}
