package io.keyforge.server.metrics;

import java.util.Locale;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import io.keyforge.raft.RaftStatus;
import io.keyforge.raft.Role;
import io.keyforge.storage.lsm.LsmStats;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

/**
 * Gauges and counters over a node's Raft state and LSM engine, read when the registry is scraped; nothing is recorded
 * on the data path. In Prometheus, with a registry's {@code node} common tag:
 *
 * <ul>
 *   <li>{@code keyforge_raft_leader} (1 on the leader, else 0), {@code keyforge_raft_role} (one series per role, 1 for
 *       the current one), {@code keyforge_raft_term};</li>
 *   <li>{@code keyforge_raft_commit_index}, {@code keyforge_raft_applied_index}, {@code keyforge_raft_last_log_index},
 *       {@code keyforge_raft_snapshot_index}, and {@code keyforge_raft_apply_lag}: committed entries not applied
 *       yet;</li>
 *   <li>{@code keyforge_lsm_flushes_total}, {@code keyforge_lsm_flushed_bytes_total},
 *       {@code keyforge_lsm_compactions_total}, {@code keyforge_lsm_compaction_read_bytes_total},
 *       {@code keyforge_lsm_compaction_written_bytes_total}, {@code keyforge_lsm_compaction_seconds_total};</li>
 *   <li>{@code keyforge_lsm_sstables}, {@code keyforge_lsm_sstable_bytes}, {@code keyforge_lsm_memtable_bytes},
 *       {@code keyforge_lsm_immutable_memtables}.</li>
 * </ul>
 */
public final class NodeMetrics {

    private NodeMetrics() {
    }

    /**
     * Registers the Raft meters.
     *
     * @param status  the Raft core's latest status
     * @param applied the index of the last entry applied to the state machine
     */
    public static void bindRaft(MeterRegistry registry, Supplier<RaftStatus> status, LongSupplier applied) {
        gauge(registry, "keyforge.raft.leader", "1 if this node is the leader, else 0",
                status, s -> s.get().role() == Role.LEADER ? 1 : 0);
        for (Role role : Role.values()) {
            String name = role.name().toLowerCase(Locale.ROOT);
            Gauge.builder("keyforge.raft.role", status, s -> s.get().role() == role ? 1 : 0)
                    .description("1 for this node's current Raft role, 0 for the others")
                    .tag("role", name)
                    .strongReference(true)
                    .register(registry);
        }
        gauge(registry, "keyforge.raft.term", "The current term", status, s -> s.get().term());
        gauge(registry, "keyforge.raft.commit.index", "Index of the highest entry known to be committed",
                status, s -> s.get().commitIndex());
        gauge(registry, "keyforge.raft.applied.index", "Index of the last entry applied to the state machine",
                applied, LongSupplier::getAsLong);
        gauge(registry, "keyforge.raft.last.log.index", "Index of the last entry in the log",
                status, s -> s.get().lastLogIndex());
        gauge(registry, "keyforge.raft.snapshot.index", "Index of the last entry covered by the snapshot",
                status, s -> s.get().snapshotIndex());
        gauge(registry, "keyforge.raft.apply.lag", "Committed entries not yet applied to the state machine",
                status, s -> Math.max(0, s.get().commitIndex() - applied.getAsLong()));
    }

    /** Registers the LSM engine's meters over its {@link LsmStats}. */
    public static void bindLsm(MeterRegistry registry, Supplier<LsmStats> stats) {
        counter(registry, "keyforge.lsm.flushes", "Memtables flushed to SSTables", stats, LsmStats::flushes);
        counter(registry, "keyforge.lsm.flushed.bytes", "Bytes of SSTables written by flushes",
                stats, LsmStats::flushedBytes);
        counter(registry, "keyforge.lsm.compactions", "Compactions completed", stats, LsmStats::compactions);
        counter(registry, "keyforge.lsm.compaction.read.bytes", "Bytes of SSTables read by compactions",
                stats, LsmStats::compactionInputBytes);
        counter(registry, "keyforge.lsm.compaction.written.bytes", "Bytes of SSTables written by compactions",
                stats, LsmStats::compactionOutputBytes);
        FunctionCounter.builder("keyforge.lsm.compaction.seconds", stats, s -> s.get().compactionNanos() / 1e9)
                .description("Time spent compacting")
                .baseUnit("seconds")
                .register(registry);
        gauge(registry, "keyforge.lsm.sstables", "SSTables in the current version", stats, s -> s.get().tables());
        Gauge.builder("keyforge.lsm.sstable.bytes", stats, s -> s.get().tableBytes())
                .description("Size of the current SSTables on disk")
                .baseUnit("bytes")
                .strongReference(true)
                .register(registry);
        Gauge.builder("keyforge.lsm.memtable.bytes", stats, s -> s.get().memtableBytes())
                .description("Approximate bytes in the active and immutable memtables")
                .baseUnit("bytes")
                .strongReference(true)
                .register(registry);
        gauge(registry, "keyforge.lsm.immutable.memtables", "Memtables waiting to be flushed",
                stats, s -> s.get().immutableMemtables());
    }

    /**
     * Registers the JVM's meters: heap and other memory pools ({@code jvm_memory_*}), GC pauses
     * ({@code jvm_gc_pause_seconds}), threads, class loading, CPU and uptime.
     *
     * @return closes the GC listener the GC meters need
     */
    public static AutoCloseable bindJvm(MeterRegistry registry) {
        new JvmMemoryMetrics().bindTo(registry);
        JvmGcMetrics gc = new JvmGcMetrics();
        gc.bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ClassLoaderMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
        return gc;
    }

    private static <T> void gauge(MeterRegistry registry, String name, String description, T source,
                                  ToDoubleFunction<T> value) {
        Gauge.builder(name, source, value).description(description).strongReference(true).register(registry);
    }

    /**
     * Micrometer holds a function counter's state weakly, and nothing else may refer to {@code stats} (a method
     * reference, say): the LSM gauges, registered with strong references to the same supplier, keep it alive.
     */
    private static void counter(MeterRegistry registry, String name, String description, Supplier<LsmStats> stats,
                                ToLongFunction<LsmStats> value) {
        FunctionCounter.builder(name, stats, s -> value.applyAsLong(s.get()))
                .description(description)
                .register(registry);
    }
}
