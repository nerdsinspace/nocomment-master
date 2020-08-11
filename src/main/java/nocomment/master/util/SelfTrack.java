package nocomment.master.util;

import java.util.ArrayList;
import java.util.List;

public final class SelfTrack {

    private static final int CHUNK_RADIUS = 7;
    private static final List<ChunkPos> MIN_NETHER = new ArrayList<ChunkPos>() {{
        add(new ChunkPos(37376 >> 4, -43870 >> 4));
        add(new ChunkPos(2683712 >> 4, 862480 >> 4));
    }};
    private static final List<ChunkPos> MAX_NETHER = new ArrayList<ChunkPos>() {{
        add(new ChunkPos(37407 >> 4, -43840 >> 4));
        add(new ChunkPos(2683743 >> 4, 862511 >> 4));
    }};
    private static final List<ChunkPos> MIN_OVERWORLD = new ArrayList<ChunkPos>() {{
        add(new ChunkPos(299242 >> 4, -350978 >> 4));
        add(new ChunkPos(21469984 >> 4, 6899984 >> 4));
    }};
    private static final List<ChunkPos> MAX_OVERWORLD = new ArrayList<ChunkPos>() {{
        add(new ChunkPos(299283 >> 4, -350921 >> 4));
        add(new ChunkPos(21470015 >> 4, 6900015 >> 4));
    }};


    public static boolean tooCloseToCoolLocation(int chunkX, int chunkZ, int dimension) {
        switch (dimension) {
            case 0: {
                for (int i = 0; i < MIN_OVERWORLD.size(); i++) {
                    if (tooClose(chunkX, chunkZ, MIN_OVERWORLD.get(i), MAX_OVERWORLD.get(i))) {
                        return true;
                    }
                }
                break;
            }
            case -1: {
                for (int i = 0; i < MIN_NETHER.size(); i++) {
                    if (tooClose(chunkX, chunkZ, MIN_NETHER.get(i), MAX_NETHER.get(i))) {
                        return true;
                    }
                }
                break;
            }
        }
        return false;
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
