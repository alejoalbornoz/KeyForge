package io.keyforge.server.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.keyforge.raft.RaftStatus;
import io.keyforge.raft.Role;
import io.keyforge.server.command.ClientSession;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.CommandObserver;
import io.keyforge.server.command.CommandSpec;
import io.keyforge.server.command.CommandTable;
import io.keyforge.server.command.StandardCommands;
import io.keyforge.storage.InMemoryKeyValueStore;
import io.keyforge.storage.lsm.LsmStats;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class MetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(
            new InMemoryKeyValueStore.Options(16, Duration.ZERO, InstantSource.system()));
    private final CommandTable commands = StandardCommands.create(store, InstantSource.system(), "test", Map.of());

    @AfterEach
    void close() {
        store.close();
    }

    @Test
    void everyExecutedCommandIsTimedUnderItsNameAndErrorRepliesAreCounted() {
        CommandDispatcher dispatcher = new CommandDispatcher(commands, null, new CommandMetrics(commands, registry));
        ClientSession session = new ClientSession();

        dispatch(dispatcher, session, "SET", "k", "1");
        dispatch(dispatcher, session, "get", "k");
        dispatch(dispatcher, session, "GET", "k");
        dispatch(dispatcher, session, "INCR", "k");
        dispatch(dispatcher, session, "SET", "k", "not a number");
        dispatch(dispatcher, session, "INCR", "k"); // ERR value is not an integer
        dispatch(dispatcher, session, "NOSUCHCOMMAND");
        dispatch(dispatcher, session, "GET"); // wrong number of arguments

        assertThat(timer("get").count()).isEqualTo(2);
        assertThat(timer("set").count()).isEqualTo(2);
        assertThat(timer("incr").count()).isEqualTo(2);
        assertThat(registry.get("keyforge.command.errors").tag("command", "incr").counter().count()).isEqualTo(1);
        assertThat(registry.get("keyforge.command.errors").tag("command", "get").counter().count()).isZero();
        assertThat(registry.find("keyforge.commands").tag("command", "nosuchcommand").timer()).isNull();
        assertThat(registry.get("keyforge.commands").timers()).as("one timer per command, created up front")
                .hasSize(commands.size());
    }

    @Test
    void commandTimersRecordTheObservedDurations() {
        CommandMetrics metrics = new CommandMetrics(commands, registry);
        CommandSpec get = commands.commands().stream().filter(spec -> spec.name().equals("get")).findFirst()
                .orElseThrow();

        metrics.executed(get, TimeUnit.MICROSECONDS.toNanos(50), false);
        metrics.executed(get, TimeUnit.MILLISECONDS.toNanos(20), false);

        assertThat(timer("get").count()).isEqualTo(2);
        assertThat(timer("get").max(TimeUnit.MILLISECONDS)).isEqualTo(20);
        assertThat(timer("get").totalTime(TimeUnit.MICROSECONDS)).isEqualTo(20_050);
    }

    @Test
    void theDispatcherReadsNoClockWithoutAnObserver() {
        List<CommandSpec> seen = new ArrayList<>();
        CommandObserver recording = (spec, nanos, error) -> seen.add(spec);
        ClientSession session = new ClientSession();

        dispatch(new CommandDispatcher(commands, null, recording), session, "PING");
        dispatch(new CommandDispatcher(commands), session, "PING");

        assertThat(seen).extracting(CommandSpec::name).containsExactly("ping");
    }

    @Test
    void raftMetricsFollowTheLatestStatus() {
        AtomicReference<RaftStatus> status = new AtomicReference<>(
                new RaftStatus("node-1", Role.FOLLOWER, 3, "node-2", 100, 90, 120, 3, 50));
        AtomicLong applied = new AtomicLong(90);
        NodeMetrics.bindRaft(registry, status::get, applied::get);

        assertThat(gauge("keyforge.raft.leader")).isZero();
        assertThat(registry.get("keyforge.raft.role").tag("role", "follower").gauge().value()).isEqualTo(1);
        assertThat(registry.get("keyforge.raft.role").tag("role", "leader").gauge().value()).isZero();
        assertThat(gauge("keyforge.raft.term")).isEqualTo(3);
        assertThat(gauge("keyforge.raft.commit.index")).isEqualTo(100);
        assertThat(gauge("keyforge.raft.applied.index")).isEqualTo(90);
        assertThat(gauge("keyforge.raft.last.log.index")).isEqualTo(120);
        assertThat(gauge("keyforge.raft.snapshot.index")).isEqualTo(50);
        assertThat(gauge("keyforge.raft.apply.lag")).isEqualTo(10);

        status.set(new RaftStatus("node-1", Role.LEADER, 4, "node-1", 130, 130, 130, 4, 50));
        applied.set(130);

        assertThat(gauge("keyforge.raft.leader")).isEqualTo(1);
        assertThat(registry.get("keyforge.raft.role").tag("role", "leader").gauge().value()).isEqualTo(1);
        assertThat(registry.get("keyforge.raft.role").tag("role", "follower").gauge().value()).isZero();
        assertThat(gauge("keyforge.raft.term")).isEqualTo(4);
        assertThat(gauge("keyforge.raft.apply.lag")).isZero();
    }

    @Test
    void lsmMetricsReadTheEngineStatsWhenScraped() {
        AtomicReference<LsmStats> stats = new AtomicReference<>(new LsmStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        NodeMetrics.bindLsm(registry, stats::get);
        stats.set(new LsmStats(3, 3_000, 2, 2_500, 1_800, 1_500_000_000L, 2, 2_000, 512, 1));

        assertThat(registry.get("keyforge.lsm.flushes").functionCounter().count()).isEqualTo(3);
        assertThat(registry.get("keyforge.lsm.flushed.bytes").functionCounter().count()).isEqualTo(3_000);
        assertThat(registry.get("keyforge.lsm.compactions").functionCounter().count()).isEqualTo(2);
        assertThat(registry.get("keyforge.lsm.compaction.read.bytes").functionCounter().count()).isEqualTo(2_500);
        assertThat(registry.get("keyforge.lsm.compaction.written.bytes").functionCounter().count())
                .isEqualTo(1_800);
        assertThat(registry.get("keyforge.lsm.compaction.seconds").functionCounter().count()).isEqualTo(1.5);
        assertThat(gauge("keyforge.lsm.sstables")).isEqualTo(2);
        assertThat(gauge("keyforge.lsm.sstable.bytes")).isEqualTo(2_000);
        assertThat(gauge("keyforge.lsm.memtable.bytes")).isEqualTo(512);
        assertThat(gauge("keyforge.lsm.immutable.memtables")).isEqualTo(1);
    }

    @Test
    void metersKeepReadingTheirSourcesAfterAGarbageCollection() throws InterruptedException {
        LsmStats stats = new LsmStats(4, 400, 3, 300, 200, 2_000_000_000L, 2, 200, 64, 0);
        RaftStatus status = new RaftStatus("node-1", Role.LEADER, 7, "node-1", 50, 50, 50, 7, 0);
        // Fresh lambdas, referenced by nothing but the registry: Micrometer would hold them weakly by default.
        NodeMetrics.bindLsm(registry, () -> stats);
        NodeMetrics.bindRaft(registry, () -> status, () -> 50);

        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(20);
        }

        assertThat(registry.get("keyforge.lsm.flushes").functionCounter().count()).isEqualTo(4);
        assertThat(registry.get("keyforge.lsm.compaction.seconds").functionCounter().count()).isEqualTo(2);
        assertThat(gauge("keyforge.lsm.sstables")).isEqualTo(2);
        assertThat(gauge("keyforge.raft.term")).isEqualTo(7);
        assertThat(gauge("keyforge.raft.applied.index")).isEqualTo(50);
    }

    private Timer timer(String command) {
        return registry.get("keyforge.commands").tag("command", command).timer();
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    private static void dispatch(CommandDispatcher dispatcher, ClientSession session, String... arguments) {
        dispatcher.dispatch(session, Arrays.stream(arguments).map(a -> a.getBytes(StandardCharsets.UTF_8))
                .toArray(byte[][]::new));
    }
}
