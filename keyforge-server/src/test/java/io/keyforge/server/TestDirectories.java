package io.keyforge.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/** Temporary data directories for tests that start nodes outside JUnit's {@code @TempDir} lifecycle. */
public final class TestDirectories {

    private TestDirectories() {
    }

    public static void deleteRecursively(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> files = Files.walk(directory)) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
