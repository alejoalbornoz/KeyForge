package io.keyforge.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataDirectoryTest {

    @TempDir
    Path dir;

    @Test
    void manifestRoundTrips() throws IOException {
        Manifest manifest = new Manifest(42, 1_000, 40, List.of(
                new Manifest.TableEntry(39, 4096, 501, 1_000),
                new Manifest.TableEntry(12, 99_999, 1, 500)));
        try (DataDirectory directory = DataDirectory.open(dir)) {
            assertThat(directory.readManifest()).as("not initialised yet").isNull();

            directory.writeManifest(manifest);

            assertThat(directory.readManifest()).isEqualTo(manifest);
        }
    }

    @Test
    void writingTheManifestReplacesItAndLeavesNoTemporaryFile() throws IOException {
        try (DataDirectory directory = DataDirectory.open(dir)) {
            directory.writeManifest(new Manifest(1, 0, 1, List.of()));
            directory.writeManifest(new Manifest(2, 5, 1, List.of(new Manifest.TableEntry(1, 10, 1, 5))));

            assertThat(directory.readManifest().nextFileNumber()).isEqualTo(2);
            assertThat(dir.resolve(DataDirectory.MANIFEST_TEMP)).doesNotExist();
        }
    }

    @Test
    void aDamagedManifestIsCorruption() throws IOException {
        try (DataDirectory directory = DataDirectory.open(dir)) {
            directory.writeManifest(new Manifest(1, 0, 1, List.of(new Manifest.TableEntry(1, 10, 1, 5))));
            byte[] bytes = Files.readAllBytes(dir.resolve(DataDirectory.MANIFEST));
            bytes[12] ^= 1;
            Files.write(dir.resolve(DataDirectory.MANIFEST), bytes);

            assertThatThrownBy(directory::readManifest)
                    .isInstanceOf(CorruptionException.class)
                    .hasMessageContaining("CRC");
        }
    }

    @Test
    void aManifestWithTheWrongMagicIsRejected() throws IOException {
        Files.write(dir.resolve(DataDirectory.MANIFEST), new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        try (DataDirectory directory = DataDirectory.open(dir)) {
            assertThatThrownBy(directory::readManifest)
                    .isInstanceOf(CorruptionException.class)
                    .hasMessageContaining("magic");
        }
    }

    @Test
    void onlyOneStoreCanOpenADirectory() throws IOException {
        try (DataDirectory _ = DataDirectory.open(dir)) {
            assertThatThrownBy(() -> DataDirectory.open(dir))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already in use");
        }
        DataDirectory.open(dir).close();
    }

    @Test
    void listsWalAndTableNumbers() throws IOException {
        try (DataDirectory directory = DataDirectory.open(dir)) {
            Files.createFile(directory.walPath(3));
            Files.createFile(directory.walPath(10));
            Files.createFile(directory.tablePath(7));
            Files.createFile(dir.resolve("notes.txt"));

            assertThat(directory.walNumbers()).containsExactly(3L, 10L);
            assertThat(directory.tableNumbers()).containsExactly(7L);
            assertThat(directory.walPath(3).getFileName().toString()).isEqualTo("000003.wal");
        }
    }
}
