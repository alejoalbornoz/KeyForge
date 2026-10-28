package io.keyforge.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.storage.lsm.FsyncPolicy;

class KeyForgeConfigTest {

    @TempDir
    Path tempDir;

    @Test
    void defaults() {
        KeyForgeConfig config = KeyForgeConfig.defaults();

        assertThat(config.port()).isEqualTo(6379);
        assertThat(config.bindAddress()).isEqualTo("0.0.0.0");
        assertThat(config.serverType()).isEqualTo(ServerType.NETTY);
        assertThat(config.ioThreads()).isZero();
        assertThat(config.maxBulkLength()).isEqualTo(512 * 1024 * 1024);
        assertThat(config.storeSegments()).isEqualTo(256);
        assertThat(config.activeExpireInterval()).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void fileThenEnvironmentThenOverrides() throws IOException {
        Path file = tempDir.resolve("keyforge.properties");
        Files.writeString(file, """
                server.port = 7000
                server.io-threads = 2
                store.segments = 64
                """);
        Map<String, String> environment = Map.of(
                "KEYFORGE_SERVER_IO_THREADS", "4",
                "KEYFORGE_STORE_SEGMENTS", "128",
                "UNRELATED", "ignored");

        KeyForgeConfig config = KeyForgeConfig.load(file, environment, Map.of("store.segments", "32"));

        assertThat(config.port()).as("from file").isEqualTo(7000);
        assertThat(config.ioThreads()).as("environment beats file").isEqualTo(4);
        assertThat(config.storeSegments()).as("override beats environment").isEqualTo(32);
    }

    @Test
    void parsesCommandLineOptions() throws IOException {
        Path file = tempDir.resolve("node.properties");
        Files.writeString(file, "node.id=n7\n");

        KeyForgeConfig config = KeyForgeConfig.fromCommandLine(
                new String[] {"--config", file.toString(), "--server.port=6400", "--server.bind", "127.0.0.1"},
                Map.of());

        assertThat(config.nodeId()).isEqualTo("n7");
        assertThat(config.port()).isEqualTo(6400);
        assertThat(config.bindAddress()).isEqualTo("127.0.0.1");
    }

    @Test
    void serverTypeComesFromAnySource() throws IOException {
        Path file = tempDir.resolve("vt.properties");
        Files.writeString(file, "server.type = virtual-threads");

        assertThat(KeyForgeConfig.load(file, Map.of(), Map.of()).serverType()).isEqualTo(ServerType.VIRTUAL_THREADS);
        assertThat(KeyForgeConfig.load(null, Map.of("KEYFORGE_SERVER_TYPE", "Virtual-Threads"), Map.of()).serverType())
                .as("case-insensitive, from the environment")
                .isEqualTo(ServerType.VIRTUAL_THREADS);
        assertThat(KeyForgeConfig.fromCommandLine(new String[] {"--server.type=netty"},
                Map.of("KEYFORGE_SERVER_TYPE", "virtual-threads")).serverType())
                .as("command line beats environment")
                .isEqualTo(ServerType.NETTY);
    }

    @Test
    void rejectsUnknownServerTypes() {
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("server.type", "epoll")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'server.type' must be one of [netty, virtual-threads], got 'epoll'");
    }

    @Test
    void environmentVariableNames() {
        assertThat(KeyForgeConfig.environmentVariable("server.io-threads")).isEqualTo("KEYFORGE_SERVER_IO_THREADS");
        assertThat(KeyForgeConfig.environmentVariable("store.active-expire-interval-ms"))
                .isEqualTo("KEYFORGE_STORE_ACTIVE_EXPIRE_INTERVAL_MS");
    }

    @Test
    void rejectsUnknownProperties() {
        assertThatThrownBy(() -> KeyForgeConfig.fromCommandLine(new String[] {"--server.prot=1"}, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown property 'server.prot'");
    }

    @Test
    void rejectsInvalidValues() {
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("server.port", "70000")))
                .hasMessage("Property 'server.port' must be between 0 and 65535");
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of("KEYFORGE_SERVER_PORT", "abc"), Map.of()))
                .hasMessage("Property 'server.port' must be an integer, got 'abc'");
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("store.segments", "100")))
                .hasMessageContaining("power of two");
        assertThatThrownBy(() -> KeyForgeConfig.fromCommandLine(new String[] {"--server.port"}, Map.of()))
                .hasMessage("Missing value for '--server.port'");
    }

    @Test
    void propertiesRoundTrip() {
        KeyForgeConfig config = KeyForgeConfig.defaults().withPort(1234).withServerType(ServerType.VIRTUAL_THREADS)
                .withStoreEngine(StoreEngine.MEMORY).withDataDirectory(Path.of("some", "dir"));

        assertThat(KeyForgeConfig.load(null, Map.of(), config.toProperties())).isEqualTo(config);
    }

    @Test
    void storageDefaultsToTheDurableLsmEngine() {
        KeyForgeConfig config = KeyForgeConfig.defaults();

        assertThat(config.storeEngine()).isEqualTo(StoreEngine.LSM);
        assertThat(config.dataDirectory()).isEqualTo(Path.of("data"));
        assertThat(config.fsyncPolicy()).isEqualTo(FsyncPolicy.GROUP_COMMIT);
        assertThat(config.groupCommitInterval()).isEqualTo(Duration.ofMillis(10));
        assertThat(config.groupCommitBytes()).isEqualTo(1L << 20);
        assertThat(config.memtableSizeBytes()).isEqualTo(64L << 20);
    }

    @Test
    void storagePropertiesComeFromAnySource() throws IOException {
        Path file = tempDir.resolve("storage.properties");
        Files.writeString(file, """
                store.data-dir = /var/lib/keyforge
                store.memtable-size-bytes = 8388608
                """);
        Map<String, String> environment = Map.of(
                "KEYFORGE_STORE_ENGINE", "Memory",
                "KEYFORGE_STORE_WAL_FSYNC_POLICY", "always");

        KeyForgeConfig config = KeyForgeConfig.load(file, environment, Map.of(
                "store.wal.group-commit-interval-ms", "25",
                "store.wal.group-commit-bytes", "4096"));

        assertThat(config.dataDirectory()).isEqualTo(Path.of("/var/lib/keyforge"));
        assertThat(config.memtableSizeBytes()).isEqualTo(8L << 20);
        assertThat(config.storeEngine()).isEqualTo(StoreEngine.MEMORY);
        assertThat(config.fsyncPolicy()).isEqualTo(FsyncPolicy.ALWAYS);
        assertThat(config.groupCommitInterval()).isEqualTo(Duration.ofMillis(25));
        assertThat(config.groupCommitBytes()).isEqualTo(4096);
        assertThat(KeyForgeConfig.environmentVariable("store.wal.fsync-policy"))
                .isEqualTo("KEYFORGE_STORE_WAL_FSYNC_POLICY");
    }

    @Test
    void rejectsInvalidStorageValues() {
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("store.engine", "rocksdb")))
                .hasMessage("Property 'store.engine' must be one of [lsm, memory], got 'rocksdb'");
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("store.wal.fsync-policy", "everysec")))
                .hasMessage("Property 'store.wal.fsync-policy' must be one of [always, group-commit], got 'everysec'");
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("store.memtable-size-bytes", "0")))
                .hasMessage("Property 'store.memtable-size-bytes' must be positive");
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("store.data-dir", " ")))
                .hasMessage("Property 'store.data-dir' must not be empty");
        assertThatThrownBy(() -> KeyForgeConfig.load(null, Map.of(), Map.of("store.wal.group-commit-interval-ms",
                "-1"))).hasMessage("Property 'store.wal.group-commit-interval-ms' must not be negative");
    }
}
