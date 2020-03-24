package nocomment.master.util;

/**
 * You'll never guess where this was pasted from
 */
public class ChunkPos {
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
        return "[" + this.x + ", " + this.z + "]";
    }

    public String blockPos() {
        return "{" + (16 * this.x) + ", " + (16 * this.z) + "}";
    }
}