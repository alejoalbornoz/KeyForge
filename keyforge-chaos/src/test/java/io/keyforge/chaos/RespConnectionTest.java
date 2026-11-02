package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespSimpleString;

@Timeout(30)
class RespConnectionTest {

    private ServerSocket server;
    private final CountDownLatch release = new CountDownLatch(1);

    @AfterEach
    void close() throws IOException {
        release.countDown();
        if (server != null) {
            server.close();
        }
    }

    /** A server that reads one request per reply in {@code replies} and answers it, split into single bytes. */
    private InetSocketAddress serve(String... replies) throws IOException {
        server = new ServerSocket(0);
        Thread.ofVirtual().start(() -> {
            try (Socket socket = server.accept()) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                for (String reply : replies) {
                    if (in.read(new byte[1024]) < 0) {
                        return;
                    }
                    for (byte b : reply.getBytes(StandardCharsets.UTF_8)) {
                        out.write(b);
                        out.flush();
                    }
                }
                release.await();
            } catch (IOException | InterruptedException e) {
                // The test is over.
            }
        });
        return new InetSocketAddress("127.0.0.1", server.getLocalPort());
    }

    @Test
    void sendsCommandsAndDecodesRepliesThatArriveInPieces() throws IOException {
        InetSocketAddress address = serve("+OK\r\n", "$4\r\n2000\r\n", ":7\r\n", "-CLUSTERDOWN no leader\r\n");

        try (RespConnection connection = RespConnection.open(address, Duration.ofSeconds(1), Duration.ofSeconds(5))) {
            assertThat(connection.call("SET", "k", "2000")).isEqualTo(RespSimpleString.OK);
            assertThat(connection.call("GET", "k")).isInstanceOfSatisfying(RespBulkString.class,
                    bulk -> assertThat(new String(bulk.value(), StandardCharsets.UTF_8)).isEqualTo("2000"));
            assertThat(connection.call("INCR", "k")).isEqualTo(new RespInteger(7));
            assertThat(connection.call("GET", "k")).isEqualTo(new RespError("CLUSTERDOWN no leader"));
        }
    }

    @Test
    void aMissingReplyTimesOut() throws IOException {
        InetSocketAddress address = serve();

        try (RespConnection connection = RespConnection.open(address, Duration.ofSeconds(1),
                Duration.ofMillis(200))) {
            assertThatThrownBy(() -> connection.call("GET", "k")).isInstanceOf(SocketTimeoutException.class);
        }
    }

    @Test
    void aClosedConnectionIsReported() throws IOException {
        server = new ServerSocket(0);
        Thread.ofVirtual().start(() -> {
            try (Socket socket = server.accept()) {
                socket.getInputStream().read(new byte[1024]);
            } catch (IOException e) {
                // The test is over.
            }
        });
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", server.getLocalPort());

        try (RespConnection connection = RespConnection.open(address, Duration.ofSeconds(1), Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> connection.call("GET", "k")).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void openingFailsWhenNobodyListens() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        assertThatThrownBy(() -> RespConnection.open(InetSocketAddress.createUnresolved("127.0.0.1", port),
                Duration.ofSeconds(1), Duration.ofSeconds(1))).isInstanceOf(IOException.class);
    }
}
