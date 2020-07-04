package nocomment.master.util;

/**
 * You'll never guess where this was pasted from
 */
public class ChunkPos {

    public static final ChunkPos SPAWN = new ChunkPos(0, 0);

    /**
     * The X position of this Chunk Coordinate Pair
     */
    public final int x;
    /**
     * The Z position of this Chunk Coordinate Pair
     */
    public final int z;

    public ChunkPos(int x, int z) {
        this.x = x;
        this.z = z;
    }

    public ChunkPos(BlockPos pos) {
        this(pos.x >> 4, pos.z >> 4);
    }

    /**
     * Converts the chunk coordinate pair to a long
     */
    public static long asLong(int x, int z) {
        return (long) x & 4294967295L | ((long) z & 4294967295L) << 32;
    }

    public int hashCode() {
        int i = 1664525 * this.x + 1013904223;
        int j = 1664525 * (this.z ^ -559038737) + 1013904223;
        return i ^ j;
    }

    public boolean equals(Object p_equals_1_) {
        if (this == p_equals_1_) {
            return true;
        } else if (!(p_equals_1_ instanceof ChunkPos)) {
            return false;
        } else {
            ChunkPos chunkpos = (ChunkPos) p_equals_1_;
            return this.x == chunkpos.x && this.z == chunkpos.z;
        }
    }

    public long distSq(ChunkPos other) {
        // this could actually overflow int easily
        // world border is 30_000_000/16 chunks, which is about 1.8 million
        // square that? you'll overflow int!
        long dx = this.x - other.x;
        long dz = this.z - other.z;
        return dx * dx + dz * dz;
    }

    public long distSq() {
        return distSq(SPAWN);
    }

    public BlockPos origin() {
        return new BlockPos(getXStart(), 0, getZStart());
    }

    public ChunkPos add(int dx, int dz) {
        return new ChunkPos(this.x + dx, this.z + dz);
    }

    /**
     * Get the first world X coordinate that belongs to this Chunk
     */
    public int getXStart() {
        return this.x << 4;
    }

    /**
     * Get the first world Z coordinate that belongs to this Chunk
     */
    public int getZStart() {
        return this.z << 4;
    }

    /**
     * Get the last world X coordinate that belongs to this Chunk
     */
    public int getXEnd() {
        return (this.x << 4) + 15;
    }

    /**
     * Get the last world Z coordinate that belongs to this Chunk
     */
    public int getZEnd() {
        return (this.z << 4) + 15;
    }

    public String toString() {
        return "chunk [" + this.x + ", " + this.z + "], which is block " + blockPos();
    }

    public String blockPos() {
        return "{" + (16 * this.x) + ", " + (16 * this.z) + "}";
    }
}