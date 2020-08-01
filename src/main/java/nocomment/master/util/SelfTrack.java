package nocomment.master.util;

public final class SelfTrack {

    private static final int CHUNK_RADIUS = 7;

    private static final ChunkPos LOC_MIN_NETHER = new ChunkPos(37376 >> 4, -43870 >> 4);
    private static final ChunkPos LOC_MAX_NETHER = new ChunkPos(37407 >> 4, -43840 >> 4);

    private static final ChunkPos LOC_MIN_OVERWORLD = new ChunkPos(299242 >> 4, -350978 >> 4);
    private static final ChunkPos LOC_MAX_OVERWORLD = new ChunkPos(299283 >> 4, -350921 >> 4);


    public static boolean tooCloseToCoolLocation(int chunkX, int chunkZ, int dimension) {
        switch (dimension) {
            case 0:
                return tooClose(chunkX, chunkZ, LOC_MIN_OVERWORLD, LOC_MAX_OVERWORLD);
            case -1:
                return tooClose(chunkX, chunkZ, LOC_MIN_NETHER, LOC_MAX_NETHER);
            default:
                return false;
        }
    }

    private static boolean tooClose(int chunkX, int chunkZ, ChunkPos areaMin, ChunkPos areaMax) {
        int dx = chunkX - clamp(chunkX, areaMin.x, areaMax.x);
        int dz = chunkZ - clamp(chunkZ, areaMin.z, areaMax.z);
        return Math.abs(dx) < CHUNK_RADIUS && Math.abs(dz) < CHUNK_RADIUS;
    }

    public static int clamp(int num, int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException();
        }
        return Math.min(Math.max(num, min), max);
    }
}
