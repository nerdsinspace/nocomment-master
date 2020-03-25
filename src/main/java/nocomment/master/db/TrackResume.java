package nocomment.master.db;

import nocomment.master.util.ChunkPos;

public class TrackResume {
    public final ChunkPos pos;
    public final int dimension;
    public final long prevTrackID;

    TrackResume(int x, int z, int dimension, long prevTrackID) {
        this.pos = new ChunkPos(x, z);
        this.dimension = dimension;
        this.prevTrackID = prevTrackID;
    }
}
