package nocomment.master.util;

/**
 * You'll never guess where this was pasted from
 */
public final class ChunkPos {

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

    public static long toLong(int x, int z) {
        return (long) x & 0xffffffffL | ((long) z & 0xffffffffL) << 32;
    }

    public static long add(long cpos, int dx, int dz) {
        int x = (int) cpos;
        int z = (int) (cpos >> 32);
        return toLong(x + dx, z + dz);
    }

    public long toLong() {
        return toLong(x, z);
    }

    public static int decodeX(long serialized) {
        return (int) serialized;
    }

    public static int decodeZ(long serialized) {
        return (int) (serialized >> 32);
    }

    public static ChunkPos fromLong(long serialized) {
        return new ChunkPos(decodeX(serialized), decodeZ(serialized));
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

    public static long distSq(final int x1, final int z1, final int x2, final int z2) {
        // this could actually overflow int easily
        // world border is 30_000_000/16 chunks, which is about 1.8 million
        // square that? you'll overflow int!
        long dx = x1 - x2;
        long dz = z1 - z2;
        return dx * dx + dz * dz;
    }

    public long distSq(final int x, final int z) {
        return distSq(this.x, this.z, x, z);
    }

    public long distSq(ChunkPos other) {
        return distSq(other.x, other.z);
    }

    public long distSq() {
        return distSq(SPAWN);
    }

    public static long distSqSerialized(long serialized1, long serialized2) {
        return distSq(decodeX(serialized1), decodeZ(serialized1), decodeX(serialized2), decodeZ(serialized2));
    }

    public static long distSqSerialized(long serialized) {
        return SPAWN.distSq(decodeX(serialized), decodeZ(serialized));
    }

    public BlockPos origin() {
        return new BlockPos(getXStart(), 0, getZStart());
    }

    public ChunkPos add(int dx, int dz) {
        return new ChunkPos(this.x + dx, this.z + dz);
    }

    public long serializedAdd(int dx, int dz) {
        return toLong(this.x + dx, this.z + dz);
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