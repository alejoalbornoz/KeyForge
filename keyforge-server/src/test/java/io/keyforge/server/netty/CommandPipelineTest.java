package io.keyforge.server.netty;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
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
import io.keyforge.storage.InMemoryKeyValueStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;

/** The Netty pipeline (decoder, handler, encoder) driven through an {@link EmbeddedChannel}, without sockets. */
class CommandPipelineTest {

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(
            InMemoryKeyValueStore.Options.defaults().withActiveExpireInterval(Duration.ZERO));
    private final CommandDispatcher dispatcher = new CommandDispatcher(
            StandardCommands.create(store, InstantSource.system(), "test", Map.of()));
    private final EmbeddedChannel channel = new EmbeddedChannel(
            new RespRequestFrameDecoder(new RespRequestDecoder()),
            new RespReplyEncoder(),
            new CommandChannelHandler(dispatcher));

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
        store.close();
    }

    @Test
    void pipelinedRequestsInOneReadAreAnsweredInOrder() {
        channel.writeInbound(buffer(
                "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\n"));

        assertThat(replies()).containsExactly(RespSimpleString.OK, RespBulkString.of("v"), RespSimpleString.PONG);
    }

    @Test
    void requestsSplitAcrossReadsAreReassembled() {
        byte[] request = RespEncoder.encodeCommand("ECHO", "hello world");
        for (byte b : request) {
            channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {b}));
        }

        assertThat(replies()).containsExactly(RespBulkString.of("hello world"));
    }

    @Test
    void aProtocolErrorIsAnsweredAfterThePrecedingRequestsAndClosesTheConnection() {
        channel.writeInbound(buffer("PING\r\n*1\r\n$x\r\nPING\r\n"));
        channel.runPendingTasks();

        assertThat(replies()).containsExactly(
                RespSimpleString.PONG, RespError.err("Protocol error: invalid bulk length"));
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void quitClosesTheConnectionAfterItsReplyAndIgnoresTheRest() {
        channel.writeInbound(buffer("QUIT\r\nPING\r\n"));
        channel.runPendingTasks();

        assertThat(replies()).containsExactly(RespSimpleString.OK);
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void stopsReadingWhileTheClientIsNotDrainingReplies() {
        channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(8, 16));
        channel.writeInbound(buffer("SET k " + "x".repeat(40) + "\r\n"));
        replies();
        assertThat(channel.config().isAutoRead()).isTrue();

        // A 47-byte reply sits unflushed in the outbound buffer, above the 16-byte high water mark.
        channel.pipeline().fireChannelRead(new byte[][] {bytes("GET"), bytes("k")});
        assertThat(channel.isWritable()).isFalse();
        assertThat(channel.config().isAutoRead()).as("reading paused").isFalse();

        // Flushing drains it; the channel turns writable and reading resumes.
        channel.pipeline().fireChannelReadComplete();
        assertThat(channel.isWritable()).isTrue();
        assertThat(channel.config().isAutoRead()).as("reading resumed").isTrue();
        assertThat(replies()).containsExactly(RespBulkString.of("x".repeat(40)));
    }

    private List<RespValue> replies() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ByteBuf buffer;
        while ((buffer = channel.readOutbound()) != null) {
            bytes.writeBytes(ByteBufUtil.getBytes(buffer));
            buffer.release();
        }
        RespDecoder decoder = new RespDecoder();
        ByteBuffer input = ByteBuffer.wrap(bytes.toByteArray());
        List<RespValue> replies = new ArrayList<>();
        RespValue reply;
        while ((reply = decoder.decode(input)) != null) {
            replies.add(reply);
        }
        assertThat(input.hasRemaining()).as("partial reply").isFalse();
        return replies;
    }

    private static ByteBuf buffer(String text) {
        return Unpooled.wrappedBuffer(bytes(text));
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
