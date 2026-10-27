package io.keyforge.server.vthreads;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespDecoder;
import io.keyforge.protocol.RespEncoder;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespRequestDecoder;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.StandardCommands;
import io.keyforge.server.vthreads.ReceiveBufferTest.ChunkedInputStream;
import io.keyforge.storage.InMemoryKeyValueStore;

/** The virtual-thread connection loop driven through in-memory streams, without sockets. */
class ConnectionTest {

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(
            InMemoryKeyValueStore.Options.defaults().withActiveExpireInterval(Duration.ZERO));
    private final CommandDispatcher dispatcher = new CommandDispatcher(
            StandardCommands.create(store, InstantSource.system(), "test", Map.of()));
    private final RecordingOutputStream out = new RecordingOutputStream();

    @AfterEach
    void tearDown() {
        store.close();
    }

    @Test
    void pipelinedRequestsInOneReadAreAnsweredInOrderWithOneWrite() throws IOException {
        serve("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\n");

        assertThat(replies()).containsExactly(RespSimpleString.OK, RespBulkString.of("v"), RespSimpleString.PONG);
        assertThat(out.writes).hasSize(1);
    }

    @Test
    void writesOncePerRead() throws IOException {
        serve("PING\r\nPING\r\n", "PING\r\n", "ECHO a\r\nECHO b\r\nECHO c\r\n");

        assertThat(replies()).hasSize(6);
        assertThat(out.writes).hasSize(3);
    }

    @Test
    void requestsSplitAcrossReadsAreReassembled() throws IOException {
        byte[] request = RespEncoder.encodeCommand("ECHO", "hello world");
        byte[][] chunks = new byte[request.length][];
        for (int i = 0; i < request.length; i++) {
            chunks[i] = new byte[] {request[i]};
        }

        serve(new ChunkedInputStream(chunks));

        assertThat(replies()).containsExactly(RespBulkString.of("hello world"));
        assertThat(out.writes).as("nothing is written for reads that complete no request").hasSize(1);
    }

    @Test
    void aBatchWithLargeRepliesIsWrittenInBoundedPieces() throws IOException {
        String value = "x".repeat(1000);
        int requests = 200;
        String batch = ("ECHO " + value + "\r\n").repeat(requests);

        // A receive buffer large enough for the whole ~200 KB batch to arrive in a single read.
        new Connection(dispatcher, new RespRequestDecoder(), 1 << 20).serve(new ChunkedInputStream(batch), out);

        assertThat(replies()).hasSize(requests).allMatch(RespBulkString.of(value)::equals);
        int reply = RespEncoder.encode(RespBulkString.of(value)).length;
        int repliesPerWrite = (Connection.FLUSH_THRESHOLD + reply - 1) / reply;
        assertThat(out.writes).hasSize((requests + repliesPerWrite - 1) / repliesPerWrite);
        assertThat(out.writes).allSatisfy(write ->
                assertThat(write.length).isLessThan(Connection.FLUSH_THRESHOLD + reply));
    }

    @Test
    void valuesLargerThanTheReceiveBufferRoundTrip() throws IOException {
        byte[] value = new byte[100_000];
        Arrays.fill(value, (byte) 'v');
        byte[] set = RespEncoder.encodeCommand(List.of(bytes("SET"), bytes("big"), value));
        byte[][] chunks = new byte[set.length / 1000 + 2][];
        for (int i = 0; i * 1000 < set.length; i++) {
            chunks[i] = Arrays.copyOfRange(set, i * 1000, Math.min(set.length, (i + 1) * 1000));
        }
        chunks[chunks.length - 1] = RespEncoder.encodeCommand("GET", "big");

        serve(new ChunkedInputStream(Arrays.stream(chunks).filter(c -> c != null).toArray(byte[][]::new)));

        assertThat(replies()).containsExactly(RespSimpleString.OK, new RespBulkString(value));
    }

    @Test
    void aProtocolErrorIsAnsweredAfterThePrecedingRequestsAndEndsTheConnection() throws IOException {
        serve("PING\r\n*1\r\n$x\r\nSET k v\r\n", "SET k2 v\r\n");

        assertThat(replies()).containsExactly(
                RespSimpleString.PONG, RespError.err("Protocol error: invalid bulk length"));
        assertThat(store.get(bytes("k2"))).isNull();
    }

    @Test
    void quitIsAnsweredAndTheRestIsIgnored() throws IOException {
        serve("QUIT\r\nSET k v\r\n", "SET k2 v\r\n");

        assertThat(replies()).containsExactly(RespSimpleString.OK);
        assertThat(store.get(bytes("k"))).isNull();
        assertThat(store.get(bytes("k2"))).isNull();
    }

    @Test
    void commandErrorsDoNotEndTheConnection() throws IOException {
        serve("FOO\r\n", "PING\r\n");

        assertThat(replies()).containsExactly(
                RespError.err("unknown command 'FOO', with args beginning with: "), RespSimpleString.PONG);
    }

    private void serve(String... chunks) throws IOException {
        serve(new ChunkedInputStream(chunks));
    }

    private void serve(ChunkedInputStream in) throws IOException {
        new Connection(dispatcher, new RespRequestDecoder(), 64).serve(in, out);
    }

    private List<RespValue> replies() {
        RespDecoder decoder = new RespDecoder();
        ByteBuffer input = ByteBuffer.wrap(out.content.toByteArray());
        List<RespValue> replies = new ArrayList<>();
        RespValue reply;
        while ((reply = decoder.decode(input)) != null) {
            replies.add(reply);
        }
        assertThat(input.hasRemaining()).as("partial reply").isFalse();
        return replies;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** Remembers every {@code write} call separately, to check how replies are batched. */
    private static final class RecordingOutputStream extends OutputStream {
        final List<byte[]> writes = new ArrayList<>();
        final ByteArrayOutputStream content = new ByteArrayOutputStream();

        @Override
        public void write(int b) {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] source, int offset, int length) {
            writes.add(Arrays.copyOfRange(source, offset, offset + length));
            content.write(source, offset, length);
        }
    }
}
