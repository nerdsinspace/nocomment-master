package nocomment.master.util;

public class BlockPos {
    public final int x;
    public final int y;
    public final int z;

    public BlockPos(int x, int y, int z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    @Override
    public int hashCode() {
        int hash = 3241;
        hash = 3457689 * hash + x;
        hash = 8734625 * hash + y;
        hash = 2873465 * hash + z;
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BlockPos && ((BlockPos) o).x == x && ((BlockPos) o).y == y && ((BlockPos) o).z == z;
    }

    public BlockPos add(int dx, int dy, int dz) {
        return new BlockPos(x + dx, y + dy, z + dz);
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + "," + z + ")";
    }
}
