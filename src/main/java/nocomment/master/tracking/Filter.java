package nocomment.master.tracking;

import nocomment.master.NoComment;
import nocomment.master.db.Database;
import nocomment.master.db.Hit;
import nocomment.master.task.SingleChunkTask;
import nocomment.master.util.ChunkPos;
import nocomment.master.util.LoggingExecutor;

import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Filter {
    private static final boolean GUI = false;
    private static final int M = 1000;
    private final Random random = new Random();
    private final WorldTrackyTracky context;
    private final long trackID;

    private List<Particle> particles = new ArrayList<>();

    private long lastUpdateMS;
    private ChunkPos mostRecentHit;
    private ScheduledFuture<?> updater;

    private int iterationsWithoutHits;

    private List<ChunkPos> hits = new ArrayList<>();
    private List<ChunkPos> misses = new ArrayList<>();
    private final ChunkPos start;

    private final JFrame frame;

    public Filter(Hit hit, WorldTrackyTracky context, OptionalLong prevTrackID) {
        this.context = context;
        deltaT();
        generatePoints(new ChunkPos(0, 0), hit.pos, M, false);
        this.trackID = Database.createTrack(hit, prevTrackID);
        insertHit(hit);
        runCheck(hit.pos);
        this.start = hit.pos;
        if (!GUI) {
            frame = null;
            return;
        }
        frame = new JFrame("no comment");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setSize(690, 690);
        frame.setContentPane(new JComponent() {
            @Override
            public void paintComponent(Graphics g) {
                double d = (System.currentTimeMillis() - lastUpdateMS) / 1000D;
                g.setColor(Color.BLACK);
                for (Particle p : particles) {
                    int[] pos = worldToScreen(p.x + p.dx * d, p.z + p.dz * d);
                    int[] pos2 = worldToScreen(p.x + p.dx, p.z + p.dz);
                    g.drawRect(pos[0], pos[1], 1, 1);
                    //g.drawLine(pos[0], pos[1], pos2[0], pos2[1]);
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
        TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(frame::repaint), 0, 100, TimeUnit.MILLISECONDS);
    }

    private int[] worldToScreen(double x, double z) {
        int xx = (int) Math.round(2 * (x - start.x)) + frame.getWidth() / 2;
        int zz = (int) Math.round(2 * (z - start.z)) + frame.getHeight() / 2;
        return new int[]{xx, zz};
    }

    public void start() {
        updater = TrackyTrackyManager.scheduler.scheduleAtFixedRate(LoggingExecutor.wrap(this::updateStep), 0, 1, TimeUnit.SECONDS);
    }

    private synchronized void updateStep() {
        //System.out.println("Update step");
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
                    generatePoints(mostRecentHit, p, M / 100, true);
                }
            }
            if (iterationsWithoutHits > 1) {
                for (int dx = -2; dx <= 2; dx++) {
                    for (int dz = -2; dz <= 2; dz++) {
                        ChunkPos p = mostRecentHit.add(dx * 7, dz * 7);
                        generatePoints(mostRecentHit, p, M / 200, true);
                    }
                }
            }
            //System.out.println("Warning: got no hits");
            numGuesses += 7;
            iterationsWithoutHits++;
            if (iterationsWithoutHits >= 5) {
                failed();
                return;
            }
        } else {
            iterationsWithoutHits = 0;
        }
        //hits.forEach(hit -> generatePoints(hit, 5));
        misses.forEach(miss -> updateFilter(particle -> particle.wouldUnload(miss)));
        misses.clear();
        hits.forEach(hit -> updateFilter(particle -> particle.wouldLoad(hit)));
        hits.clear();
        List<ChunkPos> guesses = guessLocation(numGuesses);
        if (guesses.isEmpty()) {
            failed();
            return;
        }
        //System.out.println("Guesses: " + guesses);
        //System.out.println("Best guess: " + guesses.get(0));
        //System.out.println("Avg: " + getAvg());
        guesses.forEach(this::runCheck);
    }

    public void failed() {
        System.out.println("Filter " + trackID + " has FAILED");
        updater.cancel(false);
        NoComment.executor.execute(() -> context.filterFailure(this));
        if (frame != null) {
            frame.dispose();
        }
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
                break;
            }
            ChunkPos pos = posOpt.get();
            long val = candidates.get(pos);
            if (val < M / 200) {
                break;
            }
            for (int dx = -disallowRadius(); dx <= disallowRadius(); dx++) {
                for (int dz = -disallowRadius(); dz <= disallowRadius(); dz++) {
                    candidates.remove(pos.add(dx, dz));
                }
            }
            guesses.add(pos);
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

    private void generatePoints(ChunkPos from, ChunkPos center, int count, boolean close) {
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
            Particle p = new Particle();
            p.x = cx + random.nextGaussian() * (close ? 1 : 4);
            p.z = cz + random.nextGaussian() * (close ? 1 : 4);
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

    public synchronized void insertHit(Hit hit) {
        hits.add(hit.pos);
        mostRecentHit = hit.pos;
        NoComment.executor.execute(() -> Database.addHitToTrack(hit, trackID));
    }

    public ChunkPos getMostRecentHit() {
        return mostRecentHit;
    }

    public long getTrackID() {
        return trackID;
    }

    public synchronized boolean includes(ChunkPos pos) {
        for (Particle p : particles) {
            if (Particle.wouldLoad(p.x, p.z, pos)) {
                return true;
            }
        }
        return false;
    }

    private void runCheck(ChunkPos pos) {
        NoComment.executor.execute(() ->
                context.world.submitTask(new SingleChunkTask(0, pos, this::insertHit, () -> {
                    synchronized (Filter.this) {
                        misses.add(pos);
                    }
                }))
        );
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
