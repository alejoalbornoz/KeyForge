package io.keyforge.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

/**
 * Cutting a WAL at any byte, as a crash during an append would, must recover exactly the records that were complete
 * before the cut: never fewer, never a damaged one, never an error.
 */
class WalTornTailPropertyTest {

    private Path dir;

    @BeforeProperty
    void createDirectory() throws IOException {
        dir = Files.createTempDirectory("wal-torn-tail");
    }

    @AfterProperty
    void deleteDirectory() throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    @Property(tries = 200)
    void recoversExactlyTheCompleteRecordsBeforeACut(@ForAll @IntRange(min = 1, max = 20) int recordCount,
                                                     @ForAll @IntRange(min = 0, max = 10_000) int cutSeed)
            throws IOException {
        List<WalRecord> records = WalTest.records(recordCount);
        List<Integer> ends = new ArrayList<>();
        ByteWriter all = new ByteWriter(256);
        for (WalRecord record : records) {
            WalRecord.encode(all, record.seq(), record.entries());
            ends.add(all.size());
        }
        int cut = cutSeed % (all.size() + 1);
        Path file = dir.resolve("cut-" + recordCount + "-" + cut + ".wal");
        Files.write(file, Arrays.copyOf(all.array(), cut));

        List<WalRecord> recovered = new ArrayList<>();
        WalReader.replay(file, true, recovered::add);

        int complete = (int) ends.stream().filter(end -> end <= cut).count();
        assertThat(recovered).isEqualTo(records.subList(0, complete));
        assertThat(Files.size(file)).isEqualTo(complete == 0 ? 0 : ends.get(complete - 1));
    }
}
