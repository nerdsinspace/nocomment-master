package nocomment.master.tracking;

import io.prometheus.client.Counter;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class MonteCarloParticleFilterMode extends AbstractFilterMode {

    private static final Counter failures = Counter.build()
            .name("monte_carlo_failures_total")
            .help("Steps with no hits nor misses")
            .register();

    private static final Counter timeouts = Counter.build()
            .name("monte_carlo_timeouts_total")
            .help("Filters killed for 120 seconds of no responses")
            .register();

    private static final Counter noGuesses = Counter.build()
            .name("monte_carlo_no_guesses_total")
            .help("Filters killed for no guesses")
            .register();

    private static final Counter missesCtr = Counter.build()
            .name("monte_carlo_misses_total")
            .help("Steps with no hits, just misses")
            .register();

    private static final boolean GUI = false;
    private static final int M = 1000;
    private final Random random = new Random();
    private final Track track;
    private long lastUpdateMS;
    private int iterationsWithoutHits;
    private final ChunkPos start;
    private final JFrame frame;
    private int iterationsWithoutAnything;
    private ParticleList primary = new ParticleList();
    private ParticleList secondary = new ParticleList();

    private class ParticleList {
        private double[] xArr;
        private double[] zArr;
        private double[] dxArr;
        private double[] dzArr;
        private int size;
        private final MutableParticle particle = new MutableParticle();

        private ParticleList() {
            this.xArr = new double[1024];
            this.zArr = new double[1024];
            this.dxArr = new double[1024];
            this.dzArr = new double[1024];
        }

        public void add(double x, double z, double dx, double dz) {
            if (xArr.length <= size + 1) {
                int newLen = xArr.length << 1;
                xArr = Arrays.copyOf(xArr, newLen);
                zArr = Arrays.copyOf(zArr, newLen);
                dxArr = Arrays.copyOf(dxArr, newLen);
                dzArr = Arrays.copyOf(dzArr, newLen);
            }
            set(size, x, z, dx, dz);
            size++;
        }

        public void set(int i) {
            set(i, particle.x, particle.z, particle.dx, particle.dz);
        }

        public MutableParticle get(int i) {
            particle.x = xArr[i];
            particle.z = zArr[i];
            particle.dx = dxArr[i];
            particle.dz = dzArr[i];
            return particle;
        }

        public void set(int i, double x, double z, double dx, double dz) {
            xArr[i] = x;
            zArr[i] = z;
            dxArr[i] = dx;
            dzArr[i] = dz;
        }


        private void swap(double[] arr, int i, int j) {
            double tmp = arr[i];
            arr[i] = arr[j];
            arr[j] = tmp;
        }

        private void swap(int i, int j) {
            swap(xArr, i, j);
            swap(zArr, i, j);
            swap(dxArr, i, j);
            swap(dzArr, i, j);
        }

        public void shuffle() {
            for (int i = size; i > 1; i--) {
                swap(i - 1, random.nextInt(i));
            }
        }

        public void clear() {
            size = 0;
        }

        public int size() {
            return size;
        }
    }

    private static final double BACKPROJECTION = 1;

    public static boolean wouldLoadChk(double x, double z, ChunkPos pos, int renderDistance) {
        return Math.abs((int) Math.floor(x) - pos.x) <= renderDistance && Math.abs((int) Math.floor(z) - pos.z) <= renderDistance;
    }


    private static class MutableParticle {
        double x;
        double z;
        double dx;
        double dz;

        public double wouldUnload(ChunkPos pos) {
            if (wouldLoadChk(x, z, pos, 4)) {
                return 0.1;
            }
            return 0.9;
        }

        public double wouldLoad(ChunkPos pos) {
            if (wouldLoadChk(x, z, pos, 4)) {
                return 0.95;
            }
            if (wouldLoadChk(x - dx * BACKPROJECTION, z - dz * BACKPROJECTION, pos, 4)) {
                return 0.25;
            }
            return 0.05;
        }

        public boolean wouldLoadWithTripleBackprojection(ChunkPos pos) {
            for (int i = 0; i < 3; i++) {
                if (wouldLoadChk(x - i * dx * BACKPROJECTION, z - i * dz * BACKPROJECTION, pos, 7)) {
                    return true;
                }
            }
            return false;
        }

        public long toChunkPos() {
            return ChunkPos.toLong((int) Math.floor(x), (int) Math.floor(z));
        }

        public void update(double deltaT) {
            x += dx * deltaT;
            z += dz * deltaT;
        }
    }

    public MonteCarloParticleFilterMode(ChunkPos start, Track parent) {
        generatePoints(start, M, false);
        this.start = start;
        this.track = parent;
        deltaT();
        if (GUI) {
            frame = setupFrame();
            TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(frame::repaint), 0, 100, TimeUnit.MILLISECONDS);
        } else {
            frame = null;
        }
    }

    private List<ChunkPos> renderHits;
    private List<ChunkPos> renderMisses;

    @Override
    public List<ChunkPos> updateStep(List<ChunkPos> hits, List<ChunkPos> misses) {
        if (hits.isEmpty() && misses.isEmpty()) {
            failures.inc();
            System.out.println("Maybe offline monte :(");
            // maybe we're offline
            if (iterationsWithoutAnything++ > 120) {
                timeouts.inc();
                System.out.println("Offine for 120 seconds, killing track");
                // the bot itself going offline then coming back online will resume the paused filters
                return null;
            }
            return Collections.emptyList();
        }
        iterationsWithoutAnything = 0;

        int numGuesses = 3;
        boolean failed = hits.isEmpty();
        if (failed) {
            ChunkPos mostRecentHit = track.getMostRecentHit();
            for (int dx = -1; dx <= 1; dx++) {
                for (int dz = -1; dz <= 1; dz++) {
                    ChunkPos p = mostRecentHit.add(dx * 7, dz * 7);
                    generatePoints(p, M / 100, true);
                }
            }
            if (iterationsWithoutHits > 1) {
                for (int dx = -2; dx <= 2; dx++) {
                    for (int dz = -2; dz <= 2; dz++) {
                        ChunkPos p = mostRecentHit.add(dx * 7, dz * 7);
                        generatePoints(p, M / 200, true);
                    }
                }
            }
            //System.out.println("Warning: got no hits");
            missesCtr.inc();
            numGuesses += 7;
            iterationsWithoutHits++;
            if (iterationsWithoutHits >= 5) {
                return null;
            }
        } else {
            iterationsWithoutHits = 0;
        }
        //hits.forEach(hit -> generatePoints(hit, 5));
        misses.forEach(miss -> updateFilter(particle -> particle.wouldUnload(miss)));
        hits.forEach(hit -> updateFilter(particle -> particle.wouldLoad(hit)));
        this.renderHits = hits;
        this.renderMisses = misses;
        List<ChunkPos> guesses = guessLocation(numGuesses);
        if (guesses.isEmpty()) {
            noGuesses.inc();
            return null;
        }
        return guesses;
        //System.out.println("Guesses: " + guesses);
        //System.out.println("Best guess: " + guesses.get(0));
        //System.out.println("Avg: " + getAvg());
    }

    private JFrame setupFrame() {
        JFrame frame = new JFrame("no comment");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setSize(690, 690);
        frame.setContentPane(new JComponent() {
            @Override
            public void paintComponent(Graphics g) {
                double d = (System.currentTimeMillis() - lastUpdateMS) / 1000D;
                g.setColor(Color.BLACK);
                for (int i = 0; i < primary.size(); i++) {
                    MutableParticle p = primary.get(i);
                    int[] pos = worldToScreen(p.x + p.dx * d, p.z + p.dz * d);
                    int[] pos2 = worldToScreen(p.x + p.dx, p.z + p.dz);
                    g.drawRect(pos[0], pos[1], 1, 1);
                    //g.drawLine(pos[0], pos[1], pos2[0], pos2[1]);
                }

                for (ChunkPos p : renderHits) {
                    int[] pos = worldToScreen(p.x + 0.5, p.z + 0.5);
                    g.setColor(Color.GREEN);
                    g.fillRect(pos[0] - 2, pos[1] - 2, 5, 5);
                    g.drawString(p.blockPos(), pos[0], pos[1]);

                }
                for (ChunkPos p : renderMisses) {
                    int[] pos = worldToScreen(p.x + 0.5, p.z + 0.5);
                    g.setColor(Color.RED);
                    g.fillRect(pos[0] - 2, pos[1] - 2, 5, 5);
                    g.drawString(p.blockPos(), pos[0], pos[1]);
                }
            }
        });
        frame.setVisible(true);
        return frame;
    }

    private int[] worldToScreen(double x, double z) {
        int xx = (int) Math.round(2 * (x - start.x)) + frame.getWidth() / 2;
        int zz = (int) Math.round(2 * (z - start.z)) + frame.getHeight() / 2;
        return new int[]{xx, zz};
    }

    private List<ChunkPos> guessLocation(int count) {
        Long2LongOpenHashMap candidates = new Long2LongOpenHashMap();
        for (int i = 0; i < primary.size(); i++) {
            candidates.addTo(primary.get(i).toChunkPos(), 1);
        }
        List<ChunkPos> guesses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long highestKey = 0;
            long highestValue = -1;
            ObjectIterator<Long2LongMap.Entry> it = candidates.long2LongEntrySet().fastIterator();
            while (it.hasNext()) {
                Long2LongMap.Entry val = it.next();
                if (val.getLongValue() > highestValue) {
                    highestValue = val.getLongValue();
                    highestKey = val.getLongKey();
                }
            }
            if (highestValue < M / 200) { // this catches highestValue == -1
                break;
            }
            for (int dx = -disallowRadius(); dx <= disallowRadius(); dx++) {
                for (int dz = -disallowRadius(); dz <= disallowRadius(); dz++) {
                    candidates.remove(ChunkPos.add(highestKey, dx, dz));
                }
            }
            guesses.add(ChunkPos.fromLong(highestKey));
        }
        return guesses;
    }

    private int disallowRadius() {
        return iterationsWithoutHits == 0 ? 5 : 6;
    }

    private double deltaT() {
        long then = lastUpdateMS;
        long now = System.currentTimeMillis();

        lastUpdateMS = now;
        return (now - then) / 1000D;
    }

    private void updateFilter(Function<MutableParticle, Double> weighter) {
        primary.shuffle();
        double dt = deltaT();
        int N = primary.size;
        double[] weights = new double[N];
        for (int i = 0; i < N; i++) {
            primary.get(i).update(dt);
            primary.set(i);
            weights[i] = weighter.apply(primary.get(i));
        }
        normalize(weights);
        double inc = 1.0d / M;
        double beta = inc;
        int i = 0;
        ParticleList tmp = secondary;
        tmp.clear();
        while (tmp.size() < M) {
            while (beta > weights[i]) {
                beta -= weights[i];
                i++;
                i %= N;
            }
            beta += inc;
            applyRnd(tmp, dt, primary.get(i));
        }
        secondary = primary;
        primary = tmp;
    }

    private void applyRnd(ParticleList newer, double dt, MutableParticle older) {
        double x = older.x + dt * random.nextGaussian() * older.dx / 15; // also tuned by trial and error
        double z = older.z + dt * random.nextGaussian() * older.dz / 15;
        double dz = older.dz + dt * random.nextGaussian() / 4; // tuned by trial and error
        double dx = older.dx + dt * random.nextGaussian() / 4;
        newer.add(x, z, dx, dz);
    }

    private void generatePoints(ChunkPos center, int count, boolean close) {
        double cx = center.x + 0.5d;
        double cz = center.z + 0.5d;

        double[] velocity;
        if (close) {
            velocity = new double[]{1, 1};
        } else {
            velocity = new double[]{Math.abs(cx), Math.abs(cz)};
        }
        normalize(velocity);
        velocity[0] += 0.2;
        velocity[1] += 0.2;
        normalize(velocity);

        for (int i = 0; i < count; i++) {
            double x = cx + random.nextGaussian() * (close ? 1 : 4);
            double z = cz + random.nextGaussian() * (close ? 1 : 4);
            double dx = velocity[0] * random.nextGaussian() * 36 / 16;
            double dz = velocity[1] * random.nextGaussian() * 36 / 16;
            primary.add(x, z, dx, dz);
        }
    }

    @Override
    public boolean includesBroadly(ChunkPos pos) {
        for (int i = 0; i < primary.size(); i++) {
            if (primary.get(i).wouldLoadWithTripleBackprojection(pos)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void decommission() {
        if (frame != null) {
            frame.dispose();
        }
    }

    private static void normalize(double[] weights) {
        double sum = 0;
        for (double w : weights) {
            sum += w;
        }
        if (sum == 0) {
            return;
        }
        for (int i = 0; i < weights.length; i++) {
            weights[i] /= sum;
        }
    }
}
