package io.keyforge.raft.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.raft.LogEntry;
import io.keyforge.raft.PersistentState;
import io.keyforge.raft.RaftAction;
import io.keyforge.raft.SnapshotPayload;

class FileRaftStorageTest {

    @TempDir
    Path directory;

    private static LogEntry entry(long index, long term) {
        return LogEntry.command(index, term, ("entry-" + index + "-" + term).getBytes(StandardCharsets.UTF_8));
    }

    private static List<LogEntry> entries(long from, long to, long term) {
        return LongStream.rangeClosed(from, to).mapToObj(index -> entry(index, term)).toList();
    }

    private void append(FileRaftStorage storage, List<LogEntry> entries) throws IOException {
        storage.apply(new RaftAction.AppendLog(entries));
        storage.sync();
    }

    @Test
    void anEmptyDirectoryIsAnEmptyLog() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered()).isEqualTo(PersistentState.EMPTY);
            assertThat(storage.snapshots().latest()).isEmpty();
        }
    }

    @Test
    void termVoteAndEntriesSurviveARestart() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            storage.apply(new RaftAction.PersistState(3, "n2"));
            append(storage, entries(1, 5, 2));
            storage.apply(new RaftAction.AppendLog(List.of(LogEntry.noop(6, 3))));
            storage.sync();
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            PersistentState state = storage.recovered();
            assertThat(state.term()).isEqualTo(3);
            assertThat(state.votedFor()).isEqualTo("n2");
            assertThat(state.entries()).hasSize(6).startsWith(entry(1, 2)).endsWith(LogEntry.noop(6, 3));
            storage.apply(new RaftAction.PersistState(4, null));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered().votedFor()).isNull();
            assertThat(storage.recovered().term()).isEqualTo(4);
        }
    }

    @Test
    void truncationAcrossSegmentsKeepsThePrefix() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory, 200)) {
            append(storage, entries(1, 40, 1));
            assertThat(segments()).hasSizeGreaterThan(3);
            storage.apply(new RaftAction.TruncateLog(12));
            append(storage, entries(12, 15, 2));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory, 200)) {
            List<LogEntry> expected = new ArrayList<>(entries(1, 11, 1));
            expected.addAll(entries(12, 15, 2));
            assertThat(storage.recovered().entries()).isEqualTo(expected);
        }
    }

    @Test
    void compactionDropsCoveredSegmentsAndRecoveryStartsAfterTheSnapshot() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory, 200)) {
            append(storage, entries(1, 40, 1));
            int before = segments().size();
            storage.snapshots().write(30, 1, out -> out.write("state@30".getBytes(StandardCharsets.UTF_8)));
            storage.apply(new RaftAction.CompactLog(30, 1));
            storage.sync();
            assertThat(segments()).hasSizeLessThan(before);
            append(storage, entries(41, 42, 2));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory, 200)) {
            PersistentState state = storage.recovered();
            assertThat(state.snapshotIndex()).isEqualTo(30);
            assertThat(state.snapshotTerm()).isEqualTo(1);
            assertThat(state.entries().getFirst().index()).isEqualTo(31);
            assertThat(state.lastIndex()).isEqualTo(42);
            try (InputStream in = SnapshotStore.openBody(storage.snapshots().latest().orElseThrow())) {
                assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("state@30");
            }
        }
    }

    @Test
    void compactingThroughTheLastEntryLeavesAnEmptyLogAfterTheSnapshot() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            append(storage, entries(1, 10, 1));
            storage.snapshots().write(10, 1, out -> out.write(1));
            storage.apply(new RaftAction.CompactLog(10, 1));
            append(storage, entries(11, 12, 1));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered().entries()).isEqualTo(entries(11, 12, 1));
        }
    }

    @Test
    void anInstalledSnapshotReplacesTheLog() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            append(storage, entries(1, 5, 1));
            storage.apply(new RaftAction.RestoreSnapshot(100, 7, SnapshotPayload.of(new byte[] {1, 2, 3})));
            append(storage, entries(101, 102, 7));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            PersistentState state = storage.recovered();
            assertThat(state.snapshotIndex()).isEqualTo(100);
            assertThat(state.entries()).isEqualTo(entries(101, 102, 7));
            assertThat(storage.snapshots().latest().orElseThrow().bodySize()).isEqualTo(3);
        }
    }

    @Test
    void recoveryFinishesAnInstallThatCrashedBeforeTheLogWasReset() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            append(storage, entries(1, 5, 1));
            // The snapshot landed, the reset of the log did not.
            storage.snapshots().install(100, 7, SnapshotPayload.of(new byte[] {1}));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered().snapshotIndex()).isEqualTo(100);
            assertThat(storage.recovered().entries()).isEmpty();
            append(storage, entries(101, 101, 7));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered().entries()).isEqualTo(entries(101, 101, 7));
        }
    }

    @Test
    void aTornLastRecordIsTruncated() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            append(storage, entries(1, 3, 1));
        }
        Path segment = segments().getLast();
        long size = Files.size(segment);
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
            channel.truncate(size - 5);
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered().entries()).isEqualTo(entries(1, 2, 1));
            append(storage, entries(3, 4, 2));
        }
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered().lastIndex()).isEqualTo(4);
        }
    }

    @Test
    void aZeroFilledTailIsTruncated() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            append(storage, entries(1, 3, 1));
        }
        Files.write(segments().getLast(), new byte[64], StandardOpenOption.APPEND);
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            assertThat(storage.recovered().entries()).isEqualTo(entries(1, 3, 1));
        }
    }

    @Test
    void corruptionInTheMiddleIsFatal() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            append(storage, entries(1, 3, 1));
        }
        Path segment = segments().getLast();
        byte[] bytes = Files.readAllBytes(segment);
        bytes[12] ^= 0x40; // inside the first record's payload
        Files.write(segment, bytes);
        assertThatThrownBy(() -> FileRaftStorage.open(directory)).isInstanceOf(StorageCorruptionException.class);
    }

    @Test
    void aDamagedSnapshotIsFatal() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            storage.snapshots().write(5, 1, out -> out.write(new byte[100]));
        }
        Path snapshot = directory.resolve("snapshot-5-1.snap");
        byte[] bytes = Files.readAllBytes(snapshot);
        bytes[40] = 1;
        Files.write(snapshot, bytes);
        assertThatThrownBy(() -> FileRaftStorage.open(directory)).isInstanceOf(StorageCorruptionException.class);
    }

    @Test
    void onlyTheNewestSnapshotIsKept() throws IOException {
        try (FileRaftStorage storage = FileRaftStorage.open(directory)) {
            storage.snapshots().write(5, 1, out -> out.write(5));
            storage.snapshots().write(9, 2, out -> out.write(9));
            // A snapshot older than the newest is superseded at once.
            storage.snapshots().write(7, 2, out -> out.write(7));
            assertThat(storage.snapshots().latest().orElseThrow().index()).isEqualTo(9);
        }
        try (var files = Files.list(directory)) {
            assertThat(files.map(path -> path.getFileName().toString()).filter(name -> name.startsWith("snapshot")))
                    .containsExactly("snapshot-9-2.snap");
        }
    }

    @Test
    void aDirectoryCanBeOpenedOnlyOnce() throws IOException {
        try (FileRaftStorage ignored = FileRaftStorage.open(directory)) {
            assertThatThrownBy(() -> FileRaftStorage.open(directory)).isInstanceOf(IllegalStateException.class);
        }
    }

    private List<Path> segments() throws IOException {
        try (var files = Files.list(directory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".seg")).sorted().toList();
        }
    }
}
