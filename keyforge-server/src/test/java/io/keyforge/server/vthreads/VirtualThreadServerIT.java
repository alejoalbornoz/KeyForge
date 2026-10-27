package io.keyforge.server.vthreads;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.keyforge.server.KeyForgeConfig;
import io.keyforge.server.ServerType;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.StandardCommands;
import io.keyforge.storage.InMemoryKeyValueStore;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordingStream;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Behaviour specific to the virtual-thread server. The protocol behaviour it shares with the Netty server is covered by
 * {@code KeyForgeServerIT}.
 */
@Timeout(60)
class VirtualThreadServerIT {

    private static final String PINNED = "jdk.VirtualThreadPinned";

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(
            InMemoryKeyValueStore.Options.defaults().withActiveExpireInterval(Duration.ZERO));
    private final VirtualThreadServer server = new VirtualThreadServer(
            KeyForgeConfig.load(null, Map.of(), Map.of(
                    KeyForgeConfig.SERVER_BIND, "127.0.0.1",
                    KeyForgeConfig.SERVER_PORT, "0",
                    KeyForgeConfig.SERVER_TYPE, ServerType.VIRTUAL_THREADS.configName())),
            new CommandDispatcher(StandardCommands.create(store, InstantSource.system(), "test", Map.of())));

    @AfterEach
    void tearDown() {
        server.close();
        store.close();
    }

    @Test
    void thePortIsKnownOnlyOnceStarted() {
        assertThatThrownBy(server::port).isInstanceOf(IllegalStateException.class);

        server.start();

        assertThat(server.port()).isPositive();
        assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void connectionsDoNotCostAPlatformThreadEach() throws IOException {
        server.start();
        int platformThreadsBefore = ManagementFactory.getThreadMXBean().getThreadCount();
        List<Socket> clients = new ArrayList<>();
        try {
            for (int i = 0; i < 1_000; i++) {
                Socket client = connect();
                clients.add(client);
                client.getOutputStream().write(bytes("PING\r\n"));
            }
            for (Socket client : clients) {
                assertThat(readLine(client.getInputStream())).isEqualTo("+PONG");
            }

            int platformThreadsAfter = ManagementFactory.getThreadMXBean().getThreadCount();
            // 1,000 connections are being served; the JDK may add a few carrier and poller threads, nothing more.
            assertThat(platformThreadsAfter - platformThreadsBefore).isLessThan(64);
        } finally {
            for (Socket client : clients) {
                client.close();
            }
        }
    }

    @Test
    void closingTheServerClosesOpenConnections() throws IOException {
        server.start();
        try (Socket client = connect()) {
            client.getOutputStream().write(bytes("PING\r\n"));
            assertThat(readLine(client.getInputStream())).isEqualTo("+PONG");

            server.close();

            assertThat(isClosedByServer(client)).isTrue();
        }
        assertThatThrownBy(this::connect).isInstanceOf(IOException.class);
    }

    /**
     * Since JDK 24 (JEP 491) blocking inside {@code synchronized} no longer pins a virtual thread to its carrier; what
     * still pins is blocking with a native frame or a class initializer on the stack. This test records the JFR event
     * the JDK emits whenever a virtual thread parks while pinned, and expects none while the server does real work.
     * See {@code docs/adr/0005-virtual-thread-server-and-pinning.md}.
     */
    @Test
    void servingRequestsNeverPinsAVirtualThread() throws Exception {
        server.start();
        // Run every code path once, on one connection, before recording: the first use of a class runs its
        // initializer, and a virtual thread that waits for another thread's class initialization is pinned.
        workload(1, 50);

        List<RecordedEvent> pinned = new CopyOnWriteArrayList<>();
        try (RecordingStream recording = new RecordingStream()) {
            recording.enable(PINNED).withThreshold(Duration.ZERO).withStackTrace();
            recording.onEvent(PINNED, pinned::add);
            recording.startAsync();

            // Control: the recording does see pinning, so an empty result below is meaningful.
            Thread.ofVirtual().start(SlowClassInitializer::touch).join();
            workload(16, 500);

            recording.stop();
        }

        assertThat(pinned).as("pin events").isNotEmpty().allSatisfy(event ->
                assertThat(describe(event)).contains(SlowClassInitializer.class.getSimpleName()));
    }

    /** Concurrent clients doing single-key, multi-key, pipelined and large-value commands. */
    private void workload(int clients, int iterations) throws Exception {
        byte[] largeValue = new byte[256 * 1024];
        try (ExecutorService executor = Executors.newFixedThreadPool(clients)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                String prefix = "client" + c + ":";
                futures.add(executor.submit(() -> {
                    try (Jedis jedis = new Jedis("127.0.0.1", server.port())) {
                        for (int i = 0; i < iterations; i++) {
                            jedis.set(prefix + i, "v" + i);
                            jedis.get(prefix + i);
                            jedis.incr(prefix + "counter");
                            jedis.mset(prefix + "a", "1", prefix + "b", "2");
                            jedis.mget(prefix + "a", prefix + "b");
                        }
                        Pipeline pipeline = jedis.pipelined();
                        for (int i = 0; i < iterations; i++) {
                            pipeline.incr(prefix + "pipelined");
                        }
                        pipeline.sync();
                        byte[] key = bytes(prefix + "large");
                        jedis.set(key, largeValue);
                        jedis.get(key);
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
    }

    private static String describe(RecordedEvent event) {
        StringBuilder text = new StringBuilder(event.toString());
        if (event.getStackTrace() != null) {
            for (RecordedFrame frame : event.getStackTrace().getFrames()) {
                text.append('\n').append(frame.getMethod().getType().getName()).append('.')
                        .append(frame.getMethod().getName());
            }
        }
        return text.toString();
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 5_000);
        socket.setSoTimeout(10_000);
        return socket;
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        int b;
        while ((b = in.read()) != '\n') {
            if (b < 0) {
                throw new IOException("Connection closed in the middle of a line");
            }
            if (b != '\r') {
                line.append((char) b);
            }
        }
        return line.toString();
    }

    private static boolean isClosedByServer(Socket client) throws IOException {
        try {
            return client.getInputStream().read() < 0;
        } catch (SocketException e) {
            return true; // reset instead of an orderly close
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** Parks inside its static initializer, which pins the virtual thread that triggers it. */
    private static final class SlowClassInitializer {
        static {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        static void touch() {
        }
    }
}
