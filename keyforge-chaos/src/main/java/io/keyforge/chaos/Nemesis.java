package io.keyforge.chaos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Injects faults into the cluster while the workload runs: a calm pause, a fault drawn by the {@link FaultPlanner},
 * held for a while, healed, and again, one fault at a time. Every action goes into the history as an invocation when
 * it begins and an {@code info} when it is done. Stopping heals the current fault, then everything else.
 */
final class Nemesis {

    private static final Logger log = LoggerFactory.getLogger(Nemesis.class);

    private static final Duration WIPE_ELECTION_PATIENCE = Duration.ofSeconds(30);

    private final ClusterControl control;
    private final Leadership leadership;
    private final FaultPlanner planner;
    private final History history;
    private final List<String> nodes;
    private final Duration faultDuration;
    private final Duration calmDuration;
    private final RandomGenerator random;
    private final CountDownLatch stopRequested = new CountDownLatch(1);
    private Thread thread;
    private int faults;

    Nemesis(ClusterControl control, Leadership leadership, FaultPlanner planner, History history, List<String> nodes,
            Duration faultDuration, Duration calmDuration, RandomGenerator random) {
        this.control = control;
        this.leadership = leadership;
        this.planner = planner;
        this.history = history;
        this.nodes = List.copyOf(nodes);
        this.faultDuration = faultDuration;
        this.calmDuration = calmDuration;
        this.random = random;
    }

    void start() {
        thread = Thread.ofPlatform().name("nemesis").start(this::loop);
    }

    /** Stops injecting, heals the current fault and everything else, and waits until that is done. */
    void stop() throws InterruptedException {
        stopRequested.countDown();
        if (thread != null) {
            thread.join();
        }
    }

    /** How many faults were injected. */
    synchronized int faults() {
        return faults;
    }

    private void loop() {
        try {
            while (!pause(draw(calmDuration))) {
                Fault fault = planner.next(leadership.leader());
                inject(fault);
                pause(draw(faultDuration));
                heal(fault);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            log.info("Nemesis stopped; healing everything");
            control.healEverything(nodes);
        }
    }

    private void inject(Fault fault) throws InterruptedException {
        synchronized (this) {
            faults++;
        }
        log.info("Injecting {}: {}", fault.kind().label(), fault.description());
        history.nemesis(EventType.INVOKE, "start", fault.kind(), fault.description());
        String outcome = fault.description();
        try {
            apply(fault);
        } catch (RuntimeException e) {
            log.warn("Could not inject {}: {}", fault.description(), e.getMessage());
            outcome += " (failed: " + e.getMessage() + ")";
        }
        history.nemesis(EventType.INFO, "start", fault.kind(), outcome);
    }

    private void heal(Fault fault) {
        log.info("Healing {}: {}", fault.kind().label(), fault.description());
        history.nemesis(EventType.INVOKE, "stop", fault.kind(), fault.description());
        String outcome = fault.description();
        try {
            switch (fault) {
                case Fault.Partition _, Fault.Latency _, Fault.Loss _ -> control.healNetwork();
                case Fault.Kill kill -> control.start(kill.nodes());
                case Fault.Pause pause -> control.resume(pause.nodes());
                case Fault.Wipe _ -> control.start(nodes);
            }
        } catch (RuntimeException e) {
            log.warn("Could not heal {}: {}", fault.description(), e.getMessage());
            outcome += " (failed: " + e.getMessage() + ")";
        }
        history.nemesis(EventType.INFO, "stop", fault.kind(), outcome);
    }

    private void apply(Fault fault) throws InterruptedException {
        switch (fault) {
            case Fault.Partition partition -> control.partition(partition.grudges());
            case Fault.Latency latency -> control.delay(latency.nodes(), latency.delayMillis(),
                    latency.jitterMillis());
            case Fault.Loss loss -> control.drop(loss.nodes(), loss.percent());
            case Fault.Kill kill -> control.kill(kill.nodes());
            case Fault.Pause pause -> control.pause(pause.nodes());
            case Fault.Wipe wipe -> {
                control.kill(nodes);
                control.wipe(wipe.wiped());
                control.start(wipe.wiped());
                // Only the wiped majority runs until it has elected a leader of its own. That leader serves without
                // the writes acknowledged before, which is exactly what the checker must notice.
                if (!leadership.awaitLeader(wipe.wiped(), WIPE_ELECTION_PATIENCE)) {
                    log.warn("The wiped nodes {} elected no leader within {} s", wipe.wiped(),
                            WIPE_ELECTION_PATIENCE.toSeconds());
                }
                List<String> rest = new ArrayList<>(nodes);
                rest.removeAll(wipe.wiped());
                control.start(rest);
            }
        }
    }

    /** Waits for {@code duration} or until stopped; returns whether a stop was requested. */
    private boolean pause(Duration duration) throws InterruptedException {
        return stopRequested.await(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** A duration drawn uniformly between half and one and a half times {@code mean}. */
    private Duration draw(Duration mean) {
        long nanos = mean.toNanos();
        return Duration.ofNanos(nanos / 2 + random.nextLong(Math.max(1, nanos)));
    }
}
