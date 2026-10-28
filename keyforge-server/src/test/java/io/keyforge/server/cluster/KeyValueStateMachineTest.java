package io.keyforge.server.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.raft.LogEntry;
import io.keyforge.storage.KeyValueStore;
import io.keyforge.storage.WriteCondition;
import io.keyforge.storage.lsm.LsmKeyValueStore;

class KeyValueStateMachineTest {

    @TempDir
    Path dir;

    private final List<LsmKeyValueStore> stores = new ArrayList<>();

    @AfterEach
    void close() {
        stores.forEach(LsmKeyValueStore::close);
    }

    /** A replica whose wall clock reads {@code wallMillis}; replicated writes must not care. */
    private LsmKeyValueStore store(String name, long wallMillis) {
        InstantSource clock = new InstantSource() {
            @Override
            public Instant instant() {
                return Instant.ofEpochMilli(wallMillis);
            }

            @Override
            public long millis() {
                return wallMillis;
            }
        };
        LsmKeyValueStore store = LsmKeyValueStore.open(LsmKeyValueStore.Options.defaults(dir.resolve(name))
                .withClock(clock).withReplicated(true));
        stores.add(store);
        return store;
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static List<LogEntry> log() {
        List<KvCommand> commands = List.of(
                new KvCommand.Set(1_000, b("session"), b("alice"), 1_500, WriteCondition.ALWAYS),
                new KvCommand.IncrementBy(1_100, b("counter"), 5),
                new KvCommand.Set(1_200, b("session"), b("bob"), KeyValueStore.NO_EXPIRY, WriteCondition.IF_ABSENT),
                // A leader whose clock lags: log time stays at 1,200, so the session is still live here.
                new KvCommand.Delete(900, List.of(b("missing"))),
                new KvCommand.Set(2_000, b("session"), b("carol"), KeyValueStore.NO_EXPIRY,
                        WriteCondition.IF_ABSENT),
                new KvCommand.Set(2_100, b("text"), b("abc"), KeyValueStore.NO_EXPIRY, WriteCondition.ALWAYS),
                new KvCommand.IncrementBy(2_200, b("text"), 1),
                new KvCommand.SetAll(2_300, List.of(b("a"), b("b")), List.of(b("1"), b("2"))),
                new KvCommand.Delete(2_400, List.of(b("a"), b("b"), b("nope"))));
        List<LogEntry> entries = new ArrayList<>();
        entries.add(LogEntry.noop(1, 1));
        for (int i = 0; i < commands.size(); i++) {
            entries.add(LogEntry.command(i + 2, 1, KvCommand.encode(commands.get(i))));
        }
        return entries;
    }

    private static List<Object> applyAll(KeyValueStateMachine machine, List<LogEntry> entries) {
        List<Object> results = new ArrayList<>();
        for (LogEntry entry : entries) {
            Object result = machine.apply(entry);
            results.add(result instanceof KvCommand.Failure failure ? failure.message() : result);
        }
        return results;
    }

    @Test
    void replicasWithDifferentClocksReachTheSameResultsAndState() {
        LsmKeyValueStore early = store("early", 0);
        LsmKeyValueStore late = store("late", 10_000_000);
        List<Object> first = applyAll(new KeyValueStateMachine(early), log());
        List<Object> second = applyAll(new KeyValueStateMachine(late), log());

        assertThat(first).isEqualTo(second);
        assertThat(first).containsExactly(null, true, 5L, false, 0, true, true,
                "ERR value is not an integer or out of range", null, 2);
        assertThat(dump(early)).isEqualTo(dump(late));
    }

    @Test
    void theLogTimeNeverGoesBackwards() {
        KeyValueStateMachine machine = new KeyValueStateMachine(store("s", 0));
        List<LogEntry> entries = log();
        for (LogEntry entry : entries.subList(0, 5)) {
            machine.apply(entry);
        }
        // Entry 5 was stamped 900 by a lagging leader, after an entry stamped 1,200.
        assertThat(machine.logTime()).isEqualTo(1_200);
    }

    @Test
    void positionsAreDurableAndASnapshotRestoresTheSameState() throws IOException {
        LsmKeyValueStore source = store("source", 0);
        KeyValueStateMachine machine = new KeyValueStateMachine(source);
        applyAll(machine, log());
        source.flush();
        assertThat(machine.durableAppliedIndex()).isEqualTo(10);

        ByteArrayOutputStream snapshot = new ByteArrayOutputStream();
        machine.writeSnapshot(snapshot);
        LsmKeyValueStore target = store("target", 99_999_999);
        KeyValueStateMachine restored = new KeyValueStateMachine(target);
        restored.restoreSnapshot(10, 1, new ByteArrayInputStream(snapshot.toByteArray()));

        assertThat(restored.durableAppliedIndex()).isEqualTo(10);
        assertThat(restored.logTime()).isEqualTo(machine.logTime());
        assertThat(dump(target)).isEqualTo(dump(source));
        // Both continue identically.
        LogEntry next = LogEntry.command(11, 2, KvCommand.encode(new KvCommand.IncrementBy(2_500, b("counter"), 1)));
        assertThat(restored.apply(next)).isEqualTo(machine.apply(next)).isEqualTo(6L);
    }

    private static List<String> dump(LsmKeyValueStore store) {
        List<String> entries = new ArrayList<>();
        store.export(entry -> entries.add(new String(entry.key(), StandardCharsets.UTF_8) + "="
                + new String(entry.value(), StandardCharsets.UTF_8) + "@" + entry.expireAtMillis()));
        return entries;
    }
}
