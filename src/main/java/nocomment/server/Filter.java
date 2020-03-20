package nocomment.server;

import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Filter {
    private static final int M = 1000;
    private final Random random = new Random();
    public final WorldTrackyTracky context;

    private List<Particle> particles = new ArrayList<>();

    private long lastUpdateMS;
    private ChunkPos mostRecentHit;
    private ScheduledFuture updater;

    private List<ChunkPos> hits = new ArrayList<>();
    private List<ChunkPos> misses = new ArrayList<>();
    private final ChunkPos start;

    private JFrame frame;

    public Filter(ChunkPos hit, WorldTrackyTracky context) {
        this.context = context;
        deltaT();
        generatePoints(hit, M);
        hits.add(hit);
        runCheck(hit);
        mostRecentHit = hit;
        this.start = hit;

        frame = new JFrame("no comment");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setSize(10000, 10000);
        frame.setContentPane(new JComponent() {
            @Override
            public void paintComponent(Graphics g) {
                g.setColor(Color.BLACK);
                for (Particle p : particles) {
                    int[] pos = worldToScreen(p.x, p.z);
                    g.drawRect(pos[0], pos[1], 1, 1);
                }

                for (ChunkPos p : hits) {
                    int[] pos = worldToScreen(p.x + 0.5, p.z + 0.5);
                    g.setColor(Color.GREEN);
                    g.fillRect(pos[0] - 2, pos[1] - 2, 5, 5);
                    g.drawString(p.blockPos(), pos[0], pos[1]);

                }
                for (ChunkPos p : misses) {
                    int[] pos = worldToScreen(p.x + 0.5, p.z + 0.5);
                    g.setColor(Color.RED);
                    g.fillRect(pos[0] - 2, pos[1] - 2, 5, 5);
                    g.drawString(p.blockPos(), pos[0], pos[1]);
                }
            }
        });
        frame.setVisible(true);
    }

    private int[] worldToScreen(double x, double z) {
        int xx = (int) Math.round(2 * (x - start.x)) + frame.getWidth() / 2;
        int zz = (int) Math.round(2 * (z - start.z)) + frame.getHeight() / 2;
        return new int[]{xx, zz};
    }

    public void start() {
        updater = TrackyTrackyManager.scheduler.scheduleAtFixedRate(this::updateStep, 0, 1, TimeUnit.SECONDS);
    }

    private synchronized void updateStep() {
        NoComment.executor.execute(() -> {
            try {
                Thread.sleep(900);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            frame.repaint();
        });
        System.out.println("Update step");
        if (hits.isEmpty() && misses.isEmpty()) {
            System.out.println("Maybe offline");
            // maybe we're offline
            return;
        }
        int numGuesses = 3;
        boolean failed = hits.isEmpty();
        if (failed) {
            for (int dx = -1; dx <= 1; dx++) {
                for (int dz = -1; dz <= 1; dz++) {
                    ChunkPos p = mostRecentHit.add(dx * 7, dz * 7);
                    generatePoints(p, 10);
                }
            }
            for (int dx = -2; dx <= 2; dx++) {
                for (int dz = -2; dz <= 2; dz++) {
                    ChunkPos p = mostRecentHit.add(dx * 7, dz * 7);
                    generatePoints(p, 5);
                }
            }
            System.out.println("Warning: got no hits");
            numGuesses += 7;
        }
        //hits.forEach(hit -> generatePoints(hit, 5));
        hits.forEach(hit -> updateFilter(particle -> particle.wouldLoad(hit)));
        hits.clear();
        misses.forEach(miss -> updateFilter(particle -> particle.wouldUnload(miss)));
        misses.clear();
        List<ChunkPos> guesses = guessLocation(numGuesses);
        System.out.println("Guesses: " + guesses);
        System.out.println("Best guess: " + guesses.get(0));
        System.out.println("Avg: " + getAvg());
        guesses.forEach(this::runCheck);
    }


    private List<ChunkPos> guessLocation(int count) {
        Map<ChunkPos, Long> candidates = particles.stream()
                .map(Particle::toChunkPos)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<ChunkPos> guesses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Optional<ChunkPos> posOpt = candidates.entrySet().stream()
                    .max(Comparator.comparingLong(Map.Entry::getValue))
                    .map(Map.Entry::getKey);
            if (!posOpt.isPresent()) {
                return guesses;
            }
            ChunkPos pos = posOpt.get();
            for (int dx = -6; dx <= 6; dx++) {
                for (int dz = -6; dz <= 6; dz++) {
                    candidates.remove(pos.add(dx, dz));
                }
            }
            guesses.add(pos);
        }
        return guesses;
    }

    private double deltaT() {
        long then = lastUpdateMS;
        long now = System.currentTimeMillis();

        lastUpdateMS = now;
        return (now - then) / 1000D;
    }

    private synchronized void updateFilter(Function<Particle, Double> weighter) {
        Collections.shuffle(particles);
        double dt = deltaT();
        int N = particles.size();
        double[] weights = new double[N];
        for (int i = 0; i < N; i++) {
            particles.get(i).update(dt);
            weights[i] = weighter.apply(particles.get(i));
        }
        normalize(weights);
        List<Particle> results = new ArrayList<>();
        double inc = 1.0d / M;
        double beta = inc;
        int i = 0;
        while (results.size() < M) {
            while (beta > weights[i]) {
                beta -= weights[i];
                i++;
                i %= N;
            }
            beta += inc;
            results.add(applyRnd(dt, particles.get(i)));
        }
        particles = results;
    }

    private Particle applyRnd(double dt, Particle older) {
        Particle newer = new Particle();
        newer.x = older.x + dt * random.nextGaussian() * older.dx / 15; // also tuned by trial and error
        newer.z = older.z + dt * random.nextGaussian() * older.dz / 15;
        newer.dz = older.dz + dt * random.nextGaussian() / 4; // tuned by trial and error
        newer.dx = older.dx + dt * random.nextGaussian() / 4;
        return newer;
    }

    private void generatePoints(ChunkPos center, int count) {
        double cx = center.x + 0.5d;
        double cz = center.z + 0.5d;

        double[] velocity = new double[]{Math.abs(cx), Math.abs(cz)};
        normalize(velocity);
        velocity[0] += 0.2;
        velocity[1] += 0.2;
        normalize(velocity);

        for (int i = 0; i < count; i++) {
            Particle p = new Particle();
            p.x = cx + random.nextGaussian() * 4;
            p.z = cz + random.nextGaussian() * 4;
            p.dx = velocity[0] * random.nextGaussian() * 36 / 16;
            p.dz = velocity[1] * random.nextGaussian() * 36 / 16;
            particles.add(p);
        }
    }

    private Particle getAvg() {
        Particle avg = new Particle();
        particles.forEach(particle -> {
            avg.x += particle.x / particles.size();
            avg.z += particle.z / particles.size();
            avg.dx += particle.dx / particles.size();
            avg.dz += particle.dz / particles.size();
        });
        return avg;
    }

    private void runCheck(ChunkPos pos) {
        context.world.submitTask(new SingleChunkTask(0, pos, () -> {
            synchronized (Filter.this) {
                hits.add(pos);
                mostRecentHit = pos;
            }
        }, () -> {
            synchronized (Filter.this) {
                misses.add(pos);
            }
        }));
    }

    private static void normalize(double[] weights) {
        double sum = 0;
        for (double w : weights) {
            sum += w;
        }
        if (sum == 0) {
            throw new IllegalStateException();
        }
        for (int i = 0; i < weights.length; i++) {
            weights[i] /= sum;
        }
    }
}
