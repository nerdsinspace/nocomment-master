package nocomment.master.util;

public final class BlockPos {
    public final int x;
    public final int y;
    public final int z;
    private final int hash;

    public BlockPos(int x, int y, int z) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.hash = 2873465 * (8734625 * x + y) + z;
    }

    @Override
    public int hashCode() {
        return this.hash;
    }

    private static final int NUM_X_BITS = 26;
    private static final int NUM_Z_BITS = NUM_X_BITS;
    private static final int NUM_Y_BITS = 64 - NUM_X_BITS - NUM_Z_BITS;
    private static final int Y_SHIFT = 0 + NUM_Z_BITS;
    private static final int X_SHIFT = Y_SHIFT + NUM_Y_BITS;
    private static final long X_MASK = (1L << NUM_X_BITS) - 1L;
    private static final long Y_MASK = (1L << NUM_Y_BITS) - 1L;
    private static final long Z_MASK = (1L << NUM_Z_BITS) - 1L;

    public long toLong() {
        return toLong(this.x, this.y, this.z);
    }

    public static long toLong(final int x, final int y, final int z) {
        return ((long) x & X_MASK) << X_SHIFT | ((long) y & Y_MASK) << Y_SHIFT | ((long) z & Z_MASK);
    }

    public static BlockPos fromLong(long serialized) {
        int x = (int) (serialized << (64 - X_SHIFT - NUM_X_BITS) >> (64 - NUM_X_BITS));
        int y = (int) (serialized << (64 - Y_SHIFT - NUM_Y_BITS) >> (64 - NUM_Y_BITS));
        int z = (int) (serialized << (64 - NUM_Z_BITS) >> (64 - NUM_Z_BITS));
        return new BlockPos(x, y, z);
    }

    public static long blockToChunk(long serialized) {
        int x = (int) (serialized << (64 - X_SHIFT - NUM_X_BITS) >> (64 - NUM_X_BITS));
        int z = (int) (serialized << (64 - NUM_Z_BITS) >> (64 - NUM_Z_BITS));
        return ChunkPos.toLong(x >> 4, z >> 4);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof BlockPos && ((BlockPos) o).x == x && ((BlockPos) o).y == y && ((BlockPos) o).z == z);
    }

    public BlockPos add(int dx, int dy, int dz) {
        return new BlockPos(x + dx, y + dy, z + dz);
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + "," + z + ")";
    }
}
