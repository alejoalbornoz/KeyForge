package io.keyforge.server.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;
import io.keyforge.storage.WriteCondition;

/** Forwarding of redirected commands, and INFO. */
class RoutingAndInfoTest {

    /** A keyspace on a node that is not the leader: every data operation redirects to {@code node-2}. */
    private static final class FollowerKeyspace implements Keyspace {
        @Override
        public byte[] get(byte[] key) {
            throw new RedirectException("node-2");
        }

        @Override
        public List<byte[]> getAll(List<byte[]> keys) {
            throw new RedirectException("node-2");
        }

        @Override
        public int countExisting(List<byte[]> keys) {
            throw new RedirectException("node-2");
        }

        @Override
        public long size() {
            throw new RedirectException("node-2");
        }

        @Override
        public boolean set(byte[] key, byte[] value, long expireAtMillis, WriteCondition condition) {
            throw new RedirectException("node-2");
        }

        @Override
        public void setAll(List<byte[]> keys, List<byte[]> values) {
            throw new RedirectException("node-2");
        }

        @Override
        public long incrementBy(byte[] key, long delta) {
            throw new RedirectException("node-2");
        }

        @Override
        public int delete(List<byte[]> keys) {
            throw new RedirectException("node-2");
        }

        @Override
        public void clear() {
            throw new RedirectException("node-2");
        }
    }

    private final List<String> forwarded = new ArrayList<>();
    private final CommandTable table = StandardCommands.create(new FollowerKeyspace(), InstantSource.system(), "9.9",
            Map.of(), () -> List.of(
                    new InfoSection("server", Map.of("keyforge_version", "9.9")),
                    new InfoSection("raft", Map.of("role", "follower"))));
    private final CommandDispatcher dispatcher = new CommandDispatcher(table, (arguments, target) -> {
        forwarded.add(target + ":" + new String(arguments[0], StandardCharsets.UTF_8) + " "
                + new String(arguments[1], StandardCharsets.UTF_8));
        return RespSimpleString.OK;
    });

    private static byte[][] request(String... words) {
        byte[][] arguments = new byte[words.length][];
        for (int i = 0; i < words.length; i++) {
            arguments[i] = words[i].getBytes(StandardCharsets.UTF_8);
        }
        return arguments;
    }

    @Test
    void redirectedCommandsAreForwardedAsTheClientSentThem() {
        RespValue reply = dispatcher.dispatch(new ClientSession(), request("SET", "k", "v", "EX", "10"));
        assertThat(reply).isEqualTo(RespSimpleString.OK);
        assertThat(forwarded).containsExactly("node-2:SET k");
    }

    @Test
    void forwardedCommandsAreNeverForwardedAgain() {
        assertThatThrownBy(() -> dispatcher.dispatchLocally(new ClientSession(), request("GET", "k")))
                .isInstanceOf(RedirectException.class);
        assertThat(forwarded).isEmpty();
    }

    @Test
    void localCommandsStayLocal() {
        assertThat(dispatcher.dispatch(new ClientSession(), request("PING"))).isEqualTo(RespSimpleString.PONG);
        assertThat(forwarded).isEmpty();
    }

    @Test
    void withoutARouterARedirectIsAnError() {
        RespValue reply = new CommandDispatcher(table).dispatch(new ClientSession(), request("GET", "k"));
        assertThat(reply).isInstanceOf(RespError.class);
    }

    @Test
    void infoListsSectionsLikeRedis() {
        RespBulkString all = (RespBulkString) dispatcher.dispatch(new ClientSession(), request("INFO"));
        assertThat(all.utf8()).isEqualTo("# Server\r\nkeyforge_version:9.9\r\n\r\n# Raft\r\nrole:follower\r\n");
        RespBulkString raft = (RespBulkString) dispatcher.dispatch(new ClientSession(), request("info", "RAFT"));
        assertThat(raft.utf8()).isEqualTo("# Raft\r\nrole:follower\r\n");
        RespBulkString none = (RespBulkString) dispatcher.dispatch(new ClientSession(), request("INFO", "memory"));
        assertThat(none.utf8()).isEmpty();
    }
}
