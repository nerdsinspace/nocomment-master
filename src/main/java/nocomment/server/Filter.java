package nocomment.server;

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

    public Filter(ChunkPos hit, WorldTrackyTracky context) {
        this.context = context;
        deltaT();
        generatePoints(hit, M);
        hits.add(hit);
    }

    public void start() {
        updater = TrackyTrackyManager.scheduler.scheduleAtFixedRate(this::updateStep, 0, 2, TimeUnit.SECONDS);
    }

    private synchronized void updateStep() {
        if (hits.isEmpty() && misses.isEmpty()) {
            System.out.println("Maybe offline");
            // maybe we're offline
            return;
        }
        hits.forEach(hit -> generatePoints(hit, 5));
        hits.forEach(hit -> updateFilter(particle -> particle.wouldLoad(hit)));
        hits.clear();
        misses.forEach(miss -> updateFilter(particle -> particle.wouldUnload(miss)));
        misses.clear();
        int numGuesses = 5;
        if (hits.isEmpty()) {
            System.out.println("Warning: got no hits");
            numGuesses += 5;
        }
        List<ChunkPos> guesses = guessLocation(numGuesses);
        System.out.println("Guesses: " + guesses);
        System.out.println("Best guess: " + guesses.get(0));
        System.out.println("Avg: " + getAvg());
        guesses.forEach(this::runCheck);
    }


    private List<ChunkPos> guessLocation(int count) {
        Map<ChunkPos, Long> candidates = particles.stream().map(Particle::toChunkPos).collect(Collectors.groupingBy(x -> x, Collectors.counting()));
        List<ChunkPos> guesses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Optional<ChunkPos> posOpt = candidates.entrySet().stream().max(Comparator.comparingLong(Map.Entry::getValue)).map(Map.Entry::getKey);
            if (!posOpt.isPresent()) {
                return guesses;
            }
            ChunkPos pos = posOpt.get();
            for (int dx = -3; dx <= 3; dx++) {
                for (int dz = -3; dz <= 3; dz++) {
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
        newer.x = older.x + dt * random.nextGaussian() * older.dx / 10;
        newer.z = older.z + dt * random.nextGaussian() * older.dz / 10;
        newer.dz = older.dz + dt * random.nextGaussian() / 5;
        newer.dx = older.dx + dt * random.nextGaussian() / 5;
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
            p.dx = velocity[0] * random.nextGaussian() * 36 / 16 / 1.5;
            p.dx = velocity[1] * random.nextGaussian() * 36 / 16 / 1.5;
            particles.add(p);
        }
    }

    private Particle getAvg() {
        Particle avg = new Particle();
        particles.forEach(particle -> {
            avg.x += particle.x / particles.size();
            avg.z += particle.z / particles.size();
            avg.dz += particle.dx / particles.size();
            avg.dz += particle.dz / particles.size();
        });
        return avg;
    }

    private void runCheck(ChunkPos pos) {
        context.world.submitTask(new SingleChunkTask(0, pos, () -> {
            synchronized (Filter.this) {
                hits.add(pos);
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
