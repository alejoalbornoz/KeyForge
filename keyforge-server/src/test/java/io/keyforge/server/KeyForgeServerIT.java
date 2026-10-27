package io.keyforge.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespDecoder;
import io.keyforge.protocol.RespEncoder;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespNull;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

/**
 * End-to-end tests over real sockets, with a real Redis client (Jedis) and with hand-written raw RESP. Every test runs
 * once against each server implementation, which must behave identically.
 */
@Timeout(60)
@ParameterizedClass(name = "{0}")
@EnumSource(ServerType.class)
class KeyForgeServerIT {

    private static KeyForgeNode node;

    @Parameter
    ServerType serverType;

    private Jedis jedis;

    @BeforeParameterizedClassInvocation
    static void startNode(ServerType serverType) throws InterruptedException {
        KeyForgeConfig config = KeyForgeConfig.load(null, Map.of(), Map.of(
                KeyForgeConfig.SERVER_BIND, "127.0.0.1",
                KeyForgeConfig.SERVER_PORT, "0",
                KeyForgeConfig.SERVER_TYPE, serverType.configName(),
                KeyForgeConfig.SERVER_IO_THREADS, "4",
                KeyForgeConfig.STORE_ACTIVE_EXPIRE_INTERVAL_MS, "20"));
        node = KeyForgeNode.start(config);
    }

    @AfterParameterizedClassInvocation
    static void stopNode() {
        node.close();
    }

    @BeforeEach
    void connect() {
        jedis = new Jedis("127.0.0.1", node.port());
        jedis.flushDB();
    }

    @AfterEach
    void disconnect() {
        jedis.close();
    }

    @Test
    void basicCommandsThroughARealClient() {
        assertThat(jedis.ping()).isEqualTo("PONG");
        assertThat(jedis.echo("hi")).isEqualTo("hi");
        assertThat(jedis.set("k", "v")).isEqualTo("OK");
        assertThat(jedis.get("k")).isEqualTo("v");
        assertThat(jedis.set("k", "other", SetParams.setParams().nx())).isNull();
        assertThat(jedis.set("k", "new", SetParams.setParams().xx())).isEqualTo("OK");
        assertThat(jedis.incr("n")).isEqualTo(1);
        assertThat(jedis.decr("n")).isEqualTo(0);
        assertThat(jedis.mset("a", "1", "b", "2")).isEqualTo("OK");
        assertThat(jedis.mget("a", "missing", "b")).containsExactly("1", null, "2");
        assertThat(jedis.exists("a", "b", "missing")).isEqualTo(2);
        assertThat(jedis.del("a", "b")).isEqualTo(2);
        assertThat(jedis.dbSize()).isEqualTo(2);
        assertThat(jedis.flushDB()).isEqualTo("OK");
        assertThat(jedis.dbSize()).isZero();
    }

    @Test
    void errorsReachTheClientWithRedisWording() {
        jedis.set("text", "abc");

        assertThatThrownBy(() -> jedis.incr("text"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR value is not an integer or out of range");
        assertThatThrownBy(() -> jedis.sendCommand(command("FOO"), "x"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR unknown command 'FOO', with args beginning with: 'x' ");
        assertThatThrownBy(() -> jedis.sendCommand(command("HELLO"), "3"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("NOPROTO unsupported protocol version");
        assertThat(jedis.ping()).as("the connection survives command errors").isEqualTo("PONG");
    }

    @Test
    void configGetAnswersWhatRedisBenchmarkAsks() {
        assertThat(jedis.configGet("save")).isEqualTo(Map.of("save", ""));
        assertThat(jedis.configGet("appendonly")).isEqualTo(Map.of("appendonly", "no"));
    }

    @Test
    void keysExpire() throws InterruptedException {
        jedis.set("short", "v", SetParams.setParams().px(100));
        jedis.set("long", "v", SetParams.setParams().ex(60));

        Thread.sleep(250);

        assertThat(jedis.get("short")).isNull();
        assertThat(jedis.get("long")).isEqualTo("v");
    }

    @Test
    void activeExpiryReclaimsKeysNobodyReads() throws InterruptedException {
        Pipeline pipeline = jedis.pipelined();
        for (int i = 0; i < 500; i++) {
            pipeline.set("volatile:" + i, "v", SetParams.setParams().px(300));
        }
        pipeline.sync();
        assertThat(jedis.dbSize()).isPositive();

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (jedis.dbSize() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertThat(jedis.dbSize()).isZero();
    }

    @Test
    void pipelinedCommandsAreAnsweredInOrder() {
        Pipeline pipeline = jedis.pipelined();
        List<Response<Long>> responses = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            responses.add(pipeline.incr("counter"));
        }
        pipeline.sync();

        for (int i = 0; i < responses.size(); i++) {
            assertThat(responses.get(i).get()).isEqualTo(i + 1);
        }
    }

    @Test
    void concurrentClientsDoNotLoseIncrements() throws Exception {
        int clients = 8;
        int incrementsPerClient = 2_000;
        ExecutorService executor = Executors.newFixedThreadPool(clients);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                futures.add(executor.submit(() -> {
                    try (Jedis client = new Jedis("127.0.0.1", node.port())) {
                        for (int i = 0; i < incrementsPerClient; i++) {
                            client.incr("shared");
                        }
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(jedis.get("shared")).isEqualTo(Integer.toString(clients * incrementsPerClient));
    }

    @Test
    void largeValuesRoundTrip() {
        byte[] value = new byte[10 * 1024 * 1024];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (i * 31);
        }

        jedis.set("big".getBytes(StandardCharsets.UTF_8), value);

        assertThat(jedis.get("big".getBytes(StandardCharsets.UTF_8))).isEqualTo(value);
    }

    @Test
    void inlineCommandsWorkLikeTelnet() throws IOException {
        try (RawClient client = new RawClient()) {
            client.send("SET greeting \"hello world\"\r\nGET greeting\r\nPING\r\n");

            assertThat(client.read()).isEqualTo(RespSimpleString.OK);
            assertThat(client.read()).isEqualTo(RespBulkString.of("hello world"));
            assertThat(client.read()).isEqualTo(RespSimpleString.PONG);
        }
    }

    @Test
    void requestsWrittenOneByteAtATimeAreReassembled() throws IOException {
        try (RawClient client = new RawClient()) {
            byte[] request = RespEncoder.encodeCommand("SET", "slow", "value");
            for (byte b : request) {
                client.sendBytes(new byte[] {b});
            }
            client.send("*2\r\n$3\r\nGET\r\n$4\r\nslow\r\n");

            assertThat(client.read()).isEqualTo(RespSimpleString.OK);
            assertThat(client.read()).isEqualTo(RespBulkString.of("value"));
        }
    }

    @Test
    void aProtocolErrorGetsAnErrorReplyAndTheConnectionIsClosed() throws IOException {
        try (RawClient client = new RawClient()) {
            client.send("PING\r\n*1\r\n$-5\r\n");

            assertThat(client.read()).isEqualTo(RespSimpleString.PONG);
            assertThat(client.read()).isEqualTo(RespError.err("Protocol error: invalid bulk length"));
            assertThat(client.isClosedByServer()).isTrue();
        }
    }

    @Test
    void quitClosesTheConnection() throws IOException {
        try (RawClient client = new RawClient()) {
            client.send("QUIT\r\n");

            assertThat(client.read()).isEqualTo(RespSimpleString.OK);
            assertThat(client.isClosedByServer()).isTrue();
        }
    }

    @Test
    void aHugePipelineFromAClientThatReadsLateIsFullyAnswered() throws IOException {
        // The client writes ~6 MB of requests and only starts reading later. Once the replies back up, the server
        // pauses reading (backpressure) and the writer blocks; reading must then unblock everything.
        int requests = 50_000;
        byte[] request = RespEncoder.encodeCommand("ECHO", "x".repeat(100));
        try (RawClient client = new RawClient()) {
            Thread writer = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < requests; i++) {
                        client.sendBytes(request);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(300);
            for (int i = 0; i < requests; i++) {
                assertThat(client.read()).isEqualTo(RespBulkString.of("x".repeat(100)));
            }
            writer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void binaryKeysAndValuesAreSafe() throws IOException {
        byte[] key = {0, '\r', '\n', (byte) 0xFF};
        byte[] value = {'$', '*', '\r', '\n', 0};
        try (RawClient client = new RawClient()) {
            client.sendBytes(RespEncoder.encodeCommand(List.of(bytes("SET"), key, value)));
            client.sendBytes(RespEncoder.encodeCommand(List.of(bytes("GET"), key)));

            assertThat(client.read()).isEqualTo(RespSimpleString.OK);
            assertThat(client.read()).isEqualTo(new RespBulkString(value));
            client.sendBytes(RespEncoder.encodeCommand(List.of(bytes("EXISTS"), Arrays.copyOf(key, 3))));
            assertThat(client.read()).isEqualTo(RespInteger.ZERO);
            client.sendBytes(RespEncoder.encodeCommand(List.of(bytes("GET"), bytes("absent"))));
            assertThat(client.read()).isEqualTo(RespNull.BULK_STRING);
        }
    }

    private static ProtocolCommand command(String name) {
        return () -> bytes(name);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** A minimal RESP client over a plain socket, reading replies with KeyForge's own decoder. */
    private static final class RawClient implements AutoCloseable {
        private final Socket socket = new Socket();
        private final OutputStream out;
        private final InputStream in;
        private final RespDecoder decoder = new RespDecoder();
        private final ByteBuffer buffer = ByteBuffer.allocate(64 * 1024).flip();

        RawClient() throws IOException {
            socket.connect(new InetSocketAddress("127.0.0.1", node.port()), 5_000);
            socket.setSoTimeout(10_000);
            socket.setTcpNoDelay(true);
            out = socket.getOutputStream();
            in = socket.getInputStream();
        }

        void send(String text) throws IOException {
            sendBytes(text.getBytes(StandardCharsets.UTF_8));
        }

        void sendBytes(byte[] bytes) throws IOException {
            out.write(bytes);
            out.flush();
        }

        RespValue read() throws IOException {
            while (true) {
                RespValue value = decoder.decode(buffer);
                if (value != null) {
                    return value;
                }
                if (!fill()) {
                    throw new IOException("Connection closed before a full reply arrived");
                }
            }
        }

        /** True if the server closed the connection and sent nothing more. */
        boolean isClosedByServer() throws IOException {
            try {
                return !buffer.hasRemaining() && !fill();
            } catch (SocketException e) {
                return true; // reset instead of an orderly close
            }
        }

        private boolean fill() throws IOException {
            buffer.compact();
            try {
                int read = in.read(buffer.array(), buffer.position(), buffer.remaining());
                if (read < 0) {
                    return false;
                }
                buffer.position(buffer.position() + read);
                return true;
            } finally {
                buffer.flip();
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
