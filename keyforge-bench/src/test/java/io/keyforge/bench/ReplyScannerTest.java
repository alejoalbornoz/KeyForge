package io.keyforge.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

class ReplyScannerTest {

    @Test
    void skipsEachReplyTypeGetAndSetProduce() {
        ByteBuf in = buffer("+OK\r\n$-1\r\n$5\r\nhello\r\n:42\r\n-ERR boom\r\n$0\r\n\r\n");

        assertThat(ReplyScanner.skipReply(in)).isEqualTo(ReplyScanner.OK);
        assertThat(ReplyScanner.skipReply(in)).as("null bulk string").isEqualTo(ReplyScanner.OK);
        assertThat(ReplyScanner.skipReply(in)).isEqualTo(ReplyScanner.OK);
        assertThat(ReplyScanner.skipReply(in)).isEqualTo(ReplyScanner.OK);
        assertThat(ReplyScanner.skipReply(in)).isEqualTo(ReplyScanner.ERROR);
        assertThat(ReplyScanner.skipReply(in)).as("empty bulk string").isEqualTo(ReplyScanner.OK);
        assertThat(ReplyScanner.skipReply(in)).isEqualTo(ReplyScanner.INCOMPLETE);
        assertThat(in.isReadable()).isFalse();
    }

    @Test
    void consumesNothingUntilTheWholeReplyHasArrived() {
        String reply = "$11\r\nhello world\r\n";
        for (int split = 0; split < reply.length(); split++) {
            ByteBuf in = buffer(reply.substring(0, split));

            assertThat(ReplyScanner.skipReply(in)).as("after %d bytes", split).isEqualTo(ReplyScanner.INCOMPLETE);
            assertThat(in.readerIndex()).isZero();
        }
    }

    @Test
    void bulkContentMayContainLineBreaks() {
        ByteBuf in = buffer("$4\r\n\r\n\r\n\r\n+OK\r\n");

        assertThat(ReplyScanner.skipReply(in)).isEqualTo(ReplyScanner.OK);
        assertThat(in.toString(StandardCharsets.US_ASCII)).isEqualTo("+OK\r\n");
    }

    @Test
    void rejectsRepliesTheLoadGeneratorNeverExpects() {
        assertThatThrownBy(() -> ReplyScanner.skipReply(buffer("*1\r\n$1\r\na\r\n")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ReplyScanner.skipReply(buffer("$x\r\n")))
                .isInstanceOf(IllegalStateException.class);
    }

    private static ByteBuf buffer(String text) {
        return Unpooled.copiedBuffer(text, StandardCharsets.US_ASCII);
    }
}
