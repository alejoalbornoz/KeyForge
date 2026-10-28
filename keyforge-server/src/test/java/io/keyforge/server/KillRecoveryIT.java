package io.keyforge.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.keyforge.storage.lsm.FsyncPolicy;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.SetParams;

/**
 * Runs the real server ({@link KeyForgeMain}) in a child JVM, writes to it over the network, kills it without warning
 * ({@link Process#destroyForcibly()}: SIGKILL, i.e. {@code kill -9}, on Linux; TerminateProcess on Windows) while
 * clients are writing, restarts it on the same data directory and checks that every write a client saw acknowledged is
 * still there.
 */
@Timeout(180)
class KillRecoveryIT {

    private static final int WRITERS = 4;

    @TempDir
    Path dataDirectory;

    @TempDir
    Path logs;

    static Stream<Arguments> configurations() {
        return Stream.of(
                Arguments.of(ServerType.NETTY, FsyncPolicy.GROUP_COMMIT),
                Arguments.of(ServerType.VIRTUAL_THREADS, FsyncPolicy.ALWAYS));
    }

    @ParameterizedTest(name = "{0} server, {1}")
    @MethodSource("configurations")
    void acknowledgedWritesSurviveKillDashNine(ServerType serverType, FsyncPolicy policy) throws Exception {
        long[] acknowledgedSets = new long[WRITERS];
        AtomicLong acknowledgedIncrements = new AtomicLong();

        Process first = startServer(serverType, policy, "first");
        try {
            int port = awaitReady(first, "first");
            try (Jedis jedis = new Jedis("127.0.0.1", port)) {
                assertThat(jedis.configGet("appendonly")).containsEntry("appendonly", "yes");
                jedis.set("plain", "1");
                jedis.mset("m1", "a", "m2", "b");
                for (int i = 0; i < 5; i++) {
                    jedis.incr("counter");
                }
                jedis.set("gone", "x");
                jedis.del("gone");
                jedis.set("expiring", "v", SetParams.setParams().px(Duration.ofMinutes(10).toMillis()));
            }
            writeUntilKilled(first, port, acknowledgedSets, acknowledgedIncrements);
        } finally {
            first.destroyForcibly();
            first.waitFor(30, TimeUnit.SECONDS);
        }

        Process second = startServer(serverType, policy, "second");
        try {
            int port = awaitReady(second, "second");
            try (Jedis jedis = new Jedis("127.0.0.1", port)) {
                assertThat(jedis.get("plain")).isEqualTo("1");
                assertThat(jedis.mget("m1", "m2")).containsExactly("a", "b");
                assertThat(jedis.get("counter")).isEqualTo("5");
                assertThat(jedis.get("gone")).isNull();
                assertThat(jedis.get("expiring")).isEqualTo("v");
                for (int writer = 0; writer < WRITERS; writer++) {
                    assertThat(acknowledgedSets[writer]).as("writer %d made progress", writer).isPositive();
                    for (long i = 0; i <= acknowledgedSets[writer]; i++) {
                        assertThat(jedis.get(key(writer, i))).as("acknowledged %s", key(writer, i))
                                .isEqualTo(Long.toString(i));
                    }
                }
                long hits = Long.parseLong(jedis.get("hits"));
                assertThat(hits).as("each writer has at most one INCR in flight")
                        .isBetween(acknowledgedIncrements.get(), acknowledgedIncrements.get() + WRITERS);
            }
        } finally {
            second.destroyForcibly();
            second.waitFor(30, TimeUnit.SECONDS);
        }
    }

    /** Writers set fresh keys and increment a counter until the server dies under them, 1-2 seconds in. */
    private static void writeUntilKilled(Process server, int port, long[] acknowledgedSets,
                                         AtomicLong acknowledgedIncrements) throws Exception {
        AtomicBoolean killed = new AtomicBoolean();
        ExecutorService executor = Executors.newFixedThreadPool(WRITERS);
        try {
            List<Future<?>> writers = new ArrayList<>();
            for (int w = 0; w < WRITERS; w++) {
                int writer = w;
                acknowledgedSets[writer] = -1;
                writers.add(executor.submit(() -> {
                    try (Jedis jedis = new Jedis("127.0.0.1", port)) {
                        for (long i = 0; ; i++) {
                            jedis.set(key(writer, i), Long.toString(i));
                            acknowledgedSets[writer] = i;
                            jedis.incr("hits");
                            acknowledgedIncrements.incrementAndGet();
                        }
                    } catch (JedisException e) {
                        if (!killed.get()) {
                            throw e;
                        }
                    }
                    return null;
                }));
            }
            Thread.sleep(1_000 + (long) (Math.random() * 1_000));
            killed.set(true);
            server.destroyForcibly();
            server.waitFor(30, TimeUnit.SECONDS);
            for (Future<?> writer : writers) {
                writer.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private Process startServer(ServerType serverType, FsyncPolicy policy, String label) throws IOException {
        int port = freePort();
        Path log = logs.resolve(label + ".log");
        Files.writeString(logs.resolve(label + ".port"), Integer.toString(port));
        return new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx512m",
                "-cp", System.getProperty("java.class.path"),
                KeyForgeMain.class.getName(),
                "--server.bind=127.0.0.1",
                "--server.port=" + port,
                "--server.type=" + serverType.configName(),
                "--store.data-dir=" + dataDirectory,
                "--store.wal.fsync-policy=" + policy.configName(),
                // Small memtables, so that flushes and compactions run while the clients write.
                "--store.memtable-size-bytes=262144")
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
    }

    private int awaitReady(Process server, String label) throws Exception {
        int port = Integer.parseInt(Files.readString(logs.resolve(label + ".port")));
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (!server.isAlive()) {
                throw new AssertionError("The server exited with " + server.exitValue() + ": "
                        + Files.readString(logs.resolve(label + ".log")));
            }
            try (Jedis jedis = new Jedis("127.0.0.1", port)) {
                if ("PONG".equals(jedis.ping())) {
                    return port;
                }
            } catch (JedisException e) {
                Thread.sleep(50);
            }
        }
        throw new AssertionError("The server did not start: " + Files.readString(logs.resolve(label + ".log")));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String key(int writer, long i) {
        return "w" + writer + ":" + i;
    }
}
