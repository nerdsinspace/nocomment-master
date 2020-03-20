package nocomment.server;

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
        if (wouldLoad(x, z, pos)) {
            return 0.95;
        }
        if (wouldLoad(x - dx * BACKPROJECTION, z - dz * BACKPROJECTION, pos)) {
            return 0.25;
        }
        return 0.05;
    }

    public double wouldUnload(ChunkPos pos) {
        if (wouldLoad(x, z, pos)) {
            return 0.05;
        }
        return 0.95;
    }

    public static boolean wouldLoad(double x, double z, ChunkPos pos) {
        return Math.abs((int) Math.floor(x) - pos.x) <= 4 && Math.abs((int) Math.floor(z) - pos.z) <= 4;
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
