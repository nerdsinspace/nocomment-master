package nocomment.master.tracking;

import nocomment.master.util.ChunkPos;

/**
 * A guess of a player's position and velocity
 */
public class Particle {
    private static final double BACKPROJECTION = 1;
    double x;
    double z;
    double dx;
    double dz;

    public void update(double deltaT) {
        x += dx * deltaT;
        z += dz * deltaT;
    }

    public double wouldLoad(ChunkPos pos) {
        if (wouldLoad(x, z, pos, 4)) {
            return 0.95;
        }
        if (wouldLoad(x - dx * BACKPROJECTION, z - dz * BACKPROJECTION, pos, 4)) {
            return 0.25;
        }
        return 0.05;
    }

    public double wouldUnload(ChunkPos pos) {
        if (wouldLoad(x, z, pos, 4)) {
            return 0.1;
        }
        return 0.9;
    }

    public boolean wouldLoadWithTripleBackprojection(ChunkPos pos) {
        for (int i = 0; i < 3; i++) {
            if (wouldLoad(x - i * dx * BACKPROJECTION, z - i * dz * BACKPROJECTION, pos, 7)) {
                return true;
            }
        }
        return false;
    }

    public static boolean wouldLoad(double x, double z, ChunkPos pos, int renderDistance) {
        return Math.abs((int) Math.floor(x) - pos.x) <= renderDistance && Math.abs((int) Math.floor(z) - pos.z) <= renderDistance;
    }

    public ChunkPos toChunkPos() {
        return new ChunkPos((int) Math.floor(x), (int) Math.floor(z));
    }

    public String toString() {
        return roundForStr(x) + "," + roundForStr(z) + " at speed " + roundForStr(dx) + "," + roundForStr(dz);
    }

    private double roundForStr(double d) {
        return Math.round(d * 1000D) / 1000D;
    }
}
