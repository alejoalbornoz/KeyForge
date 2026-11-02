package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.EOFException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.keyforge.protocol.RespArray;
import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespNull;
import io.keyforge.protocol.RespSimpleString;

class CompletionTest {

    @Test
    void properRepliesAreOk() {
        assertThat(Completion.of(OpFunction.GET, new RespBulkString("3000".getBytes(StandardCharsets.UTF_8))))
                .isEqualTo(Completion.ok(3000L));
        assertThat(Completion.of(OpFunction.GET, RespNull.BULK_STRING)).isEqualTo(Completion.ok(null));
        assertThat(Completion.of(OpFunction.SET, RespSimpleString.OK)).isEqualTo(Completion.ok(null));
        assertThat(Completion.of(OpFunction.INCR, new RespInteger(12))).isEqualTo(Completion.ok(12L));
    }

    @Test
    void onlyClusterDownIsADefiniteFailure() {
        assertThat(Completion.of(OpFunction.SET, new RespError("CLUSTERDOWN no Raft leader is available")))
                .isEqualTo(Completion.fail("CLUSTERDOWN no Raft leader is available"));
    }

    @Test
    void errorsThatLeaveTheOutcomeOpenAreIndeterminate() {
        for (String message : new String[] {
                "ERR timed out after 5000 ms committing the write (it may still be applied)",
                "ERR forwarding to the leader node-2 failed (DEADLINE_EXCEEDED); the command may or may not have been"
                        + " applied",
                "ERR The Raft node shut down before the proposal was applied; the write may or may not be applied",
                "ERR internal error while executing 'incr'"}) {
            assertThat(Completion.of(OpFunction.INCR, new RespError(message))).isEqualTo(Completion.info(message));
        }
    }

    @Test
    void timeoutsAreNeverFailures() {
        Completion timedOut = Completion.noReply(new SocketTimeoutException("Read timed out"), Duration.ofSeconds(10));

        assertThat(timedOut.type()).isEqualTo(EventType.INFO);
        assertThat(timedOut.error()).isEqualTo("timed out after 10000 ms");
        assertThat(Completion.noReply(new EOFException("the server closed the connection"), Duration.ofSeconds(1)))
                .isEqualTo(Completion.info("connection lost: EOFException: the server closed the connection"));
    }

    @Test
    void anOperationThatWasNeverSentFailed() {
        assertThat(Completion.notSent(new ConnectException("Connection refused")))
                .isEqualTo(Completion.fail("not sent: ConnectException: Connection refused"));
    }

    @Test
    void unexpectedRepliesAreIndeterminate() {
        assertThat(Completion.of(OpFunction.GET, new RespBulkString("abc".getBytes(StandardCharsets.UTF_8))))
                .isEqualTo(Completion.info("unexpected value 'abc'"));
        assertThat(Completion.of(OpFunction.SET, RespNull.BULK_STRING).type()).isEqualTo(EventType.INFO);
        assertThat(Completion.of(OpFunction.INCR, RespArray.EMPTY).type()).isEqualTo(EventType.INFO);
    }
}
