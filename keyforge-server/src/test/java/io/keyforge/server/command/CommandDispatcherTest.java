package io.keyforge.server.command;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.keyforge.protocol.RespArray;
import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespNull;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;
import io.keyforge.storage.InMemoryKeyValueStore;

/** Command semantics and error wording, checked against what Redis 7 replies. */
class CommandDispatcherTest {

    private static final long START = 1_700_000_000_000L;

    private final AtomicLong now = new AtomicLong(START);
    private final InstantSource clock = new InstantSource() {
        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(now.get());
        }

        @Override
        public long millis() {
            return now.get();
        }
    };
    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore(new InMemoryKeyValueStore.Options(
            16, Duration.ZERO, clock));
    private final CommandDispatcher dispatcher = new CommandDispatcher(StandardCommands.create(
            store, clock, "1.2.3", Map.of("save", "", "appendonly", "no", "port", "6379")));
    private final ClientSession session = new ClientSession();

    @AfterEach
    void closeStore() {
        store.close();
    }

    @Nested
    class Dispatch {

        @Test
        void commandNamesAreCaseInsensitive() {
            assertThat(exec("pInG")).isEqualTo(RespSimpleString.PONG);
        }

        @Test
        void unknownCommandsAreReportedLikeRedis() {
            assertThat(exec("FOO")).isEqualTo(error("ERR unknown command 'FOO', with args beginning with: "));
            assertThat(exec("foo", "a", "b"))
                    .isEqualTo(error("ERR unknown command 'foo', with args beginning with: 'a' 'b' "));
        }

        @Test
        void unknownCommandErrorsQuoteAtMost128BytesOfArguments() {
            String longArgument = "x".repeat(200);
            RespError reply = (RespError) exec("foo", longArgument, "never-shown");

            assertThat(reply.message())
                    .isEqualTo("ERR unknown command 'foo', with args beginning with: '" + "x".repeat(128) + "' ");
        }

        @Test
        void unknownCommandErrorsCannotInjectLineBreaks() {
            RespError reply = (RespError) exec("evil\r\n+OK");

            assertThat(reply.message()).doesNotContain("\r", "\n");
        }

        @Test
        void wrongArgumentCountsAreReportedLikeRedis() {
            assertThat(exec("GET")).isEqualTo(error("ERR wrong number of arguments for 'get' command"));
            assertThat(exec("get", "a", "b")).isEqualTo(error("ERR wrong number of arguments for 'get' command"));
            assertThat(exec("SET", "k")).isEqualTo(error("ERR wrong number of arguments for 'set' command"));
            assertThat(exec("PING", "a", "b")).isEqualTo(error("ERR wrong number of arguments for 'ping' command"));
        }
    }

    @Nested
    class Strings {

        @Test
        void setThenGet() {
            assertThat(exec("SET", "k", "v")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("GET", "k")).isEqualTo(bulk("v"));
            assertThat(exec("GET", "missing")).isEqualTo(RespNull.BULK_STRING);
        }

        @Test
        void setNxOnlySetsMissingKeys() {
            assertThat(exec("SET", "k", "1", "NX")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("SET", "k", "2", "nx")).isEqualTo(RespNull.BULK_STRING);
            assertThat(exec("GET", "k")).isEqualTo(bulk("1"));
        }

        @Test
        void setXxOnlySetsExistingKeys() {
            assertThat(exec("SET", "k", "1", "XX")).isEqualTo(RespNull.BULK_STRING);
            exec("SET", "k", "1");
            assertThat(exec("SET", "k", "2", "XX")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("GET", "k")).isEqualTo(bulk("2"));
        }

        @Test
        void setExExpiresAfterSeconds() {
            exec("SET", "k", "v", "EX", "10");

            now.addAndGet(9_999);
            assertThat(exec("GET", "k")).isEqualTo(bulk("v"));
            now.addAndGet(1);
            assertThat(exec("GET", "k")).isEqualTo(RespNull.BULK_STRING);
        }

        @Test
        void setPxExpiresAfterMilliseconds() {
            exec("SET", "k", "v", "PX", "250", "NX");

            now.addAndGet(249);
            assertThat(exec("EXISTS", "k")).isEqualTo(RespInteger.of(1));
            now.addAndGet(1);
            assertThat(exec("EXISTS", "k")).isEqualTo(RespInteger.of(0));
        }

        @Test
        void setWithoutExpiryClearsAnExistingOne() {
            exec("SET", "k", "v", "PX", "100");
            exec("SET", "k", "v2");

            now.addAndGet(1_000);
            assertThat(exec("GET", "k")).isEqualTo(bulk("v2"));
        }

        @Test
        void setRejectsConflictingOrUnknownOptions() {
            RespError syntax = error("ERR syntax error");
            assertThat(exec("SET", "k", "v", "NX", "XX")).isEqualTo(syntax);
            assertThat(exec("SET", "k", "v", "EX", "1", "PX", "1")).isEqualTo(syntax);
            assertThat(exec("SET", "k", "v", "EX")).isEqualTo(syntax);
            assertThat(exec("SET", "k", "v", "KEEPTTL")).isEqualTo(syntax);
            assertThat(exec("SET", "k", "v", "EX", "abc", "NX", "XX")).as("syntax wins over numbers").isEqualTo(syntax);
            assertThat(exec("GET", "k")).isEqualTo(RespNull.BULK_STRING);
        }

        @Test
        void setRejectsInvalidExpireTimes() {
            RespError invalid = error("ERR invalid expire time in 'set' command");
            assertThat(exec("SET", "k", "v", "EX", "0")).isEqualTo(invalid);
            assertThat(exec("SET", "k", "v", "PX", "-5")).isEqualTo(invalid);
            assertThat(exec("SET", "k", "v", "EX", "9223372036854775")).isEqualTo(invalid);
            assertThat(exec("SET", "k", "v", "PX", "9223372036854775807")).isEqualTo(invalid);
            assertThat(exec("SET", "k", "v", "EX", "ten"))
                    .isEqualTo(error("ERR value is not an integer or out of range"));
        }

        @Test
        void incrAndDecrTreatMissingKeysAsZero() {
            assertThat(exec("INCR", "n")).isEqualTo(RespInteger.of(1));
            assertThat(exec("INCR", "n")).isEqualTo(RespInteger.of(2));
            assertThat(exec("DECR", "n")).isEqualTo(RespInteger.of(1));
            assertThat(exec("DECR", "other")).isEqualTo(RespInteger.of(-1));
            assertThat(exec("GET", "n")).isEqualTo(bulk("1"));
        }

        @Test
        void incrRejectsValuesThatAreNotCanonicalIntegers() {
            RespError notAnInteger = error("ERR value is not an integer or out of range");
            for (String value : new String[] {"abc", "1.5", " 1", "01", "+1", ""}) {
                exec("SET", "k", value);
                assertThat(exec("INCR", "k")).as("INCR on '%s'", value).isEqualTo(notAnInteger);
                assertThat(exec("GET", "k")).isEqualTo(bulk(value));
            }
        }

        @Test
        void incrAndDecrDetectOverflow() {
            exec("SET", "max", Long.toString(Long.MAX_VALUE));
            exec("SET", "min", Long.toString(Long.MIN_VALUE));

            assertThat(exec("INCR", "max")).isEqualTo(error("ERR increment or decrement would overflow"));
            assertThat(exec("DECR", "min")).isEqualTo(error("ERR increment or decrement would overflow"));
            assertThat(exec("GET", "max")).isEqualTo(bulk(Long.toString(Long.MAX_VALUE)));
        }

        @Test
        void incrKeepsTheExpiry() {
            exec("SET", "n", "5", "PX", "100");
            exec("INCR", "n");

            now.addAndGet(100);
            assertThat(exec("GET", "n")).isEqualTo(RespNull.BULK_STRING);
        }

        @Test
        void msetAndMget() {
            assertThat(exec("MSET", "a", "1", "b", "2")).isEqualTo(RespSimpleString.OK);

            assertThat(exec("MGET", "a", "missing", "b"))
                    .isEqualTo(RespArray.of(bulk("1"), RespNull.BULK_STRING, bulk("2")));
        }

        @Test
        void msetNeedsKeyValuePairs() {
            assertThat(exec("MSET", "a", "1", "b")).isEqualTo(error("ERR wrong number of arguments for 'mset' command"));
            assertThat(exec("MSET", "a")).isEqualTo(error("ERR wrong number of arguments for 'mset' command"));
            assertThat(exec("GET", "a")).isEqualTo(RespNull.BULK_STRING);
        }

        @Test
        void valuesAreBinarySafe() {
            byte[] value = {0, '\r', '\n', (byte) 0xFF};
            dispatcher.dispatch(session, new byte[][] {b("SET"), b("bin"), value});

            assertThat(exec("GET", "bin")).isEqualTo(new RespBulkString(value));
        }
    }

    @Nested
    class Keyspace {

        @Test
        void delCountsRemovedKeys() {
            exec("MSET", "a", "1", "b", "2");

            assertThat(exec("DEL", "a", "b", "c", "a")).isEqualTo(RespInteger.of(2));
            assertThat(exec("DEL", "a")).isEqualTo(RespInteger.of(0));
        }

        @Test
        void existsCountsDuplicates() {
            exec("SET", "a", "1");

            assertThat(exec("EXISTS", "a", "a", "b")).isEqualTo(RespInteger.of(2));
        }

        @Test
        void dbsizeAndFlushdb() {
            exec("MSET", "a", "1", "b", "2", "c", "3");
            assertThat(exec("DBSIZE")).isEqualTo(RespInteger.of(3));

            assertThat(exec("FLUSHDB")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("DBSIZE")).isEqualTo(RespInteger.of(0));

            exec("SET", "a", "1");
            assertThat(exec("FLUSHDB", "ASYNC")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("FLUSHDB", "sync")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("FLUSHDB", "LATER")).isEqualTo(error("ERR syntax error"));
        }
    }

    @Nested
    class Connection {

        @Test
        void pingAndEcho() {
            assertThat(exec("PING")).isEqualTo(RespSimpleString.PONG);
            assertThat(exec("PING", "hi")).isEqualTo(bulk("hi"));
            assertThat(exec("ECHO", "hello")).isEqualTo(bulk("hello"));
        }

        @Test
        void quitRepliesOkAndAsksForTheConnectionToClose() {
            assertThat(exec("QUIT")).isEqualTo(RespSimpleString.OK);
            assertThat(session.isCloseRequested()).isTrue();
        }

        @Test
        void hello3IsRefusedBecauseOnlyResp2IsSpoken() {
            assertThat(exec("HELLO", "3")).isEqualTo(error("NOPROTO unsupported protocol version"));
            assertThat(exec("HELLO", "x")).isEqualTo(error("ERR Protocol version is not an integer or out of range"));
        }

        @Test
        void hello2DescribesTheServer() {
            RespArray reply = (RespArray) exec("HELLO", "2", "SETNAME", "app");

            assertThat(reply.elements()).contains(bulk("server"), bulk("keyforge"), bulk("version"), bulk("1.2.3"),
                    bulk("proto"), RespInteger.of(2), bulk("id"), RespInteger.of(session.id()));
            assertThat(session.name()).isEqualTo("app");
            assertThat(exec("HELLO", "2", "BOGUS")).isEqualTo(error("ERR Syntax error in HELLO option 'BOGUS'"));
        }

        @Test
        void clientNamesAndInfo() {
            assertThat(exec("CLIENT", "GETNAME")).isEqualTo(RespNull.BULK_STRING);
            assertThat(exec("CLIENT", "SETNAME", "worker-1")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("CLIENT", "GETNAME")).isEqualTo(bulk("worker-1"));
            assertThat(exec("CLIENT", "SETNAME", "has space"))
                    .isEqualTo(error("ERR Client names cannot contain spaces, newlines or special characters."));
            assertThat(exec("CLIENT", "ID")).isEqualTo(RespInteger.of(session.id()));
            assertThat(exec("CLIENT", "SETINFO", "LIB-NAME", "jedis")).isEqualTo(RespSimpleString.OK);
            assertThat(exec("CLIENT", "SETINFO", "COLOR", "blue")).isEqualTo(error("ERR Unrecognized option 'COLOR'"));
            assertThat(exec("CLIENT", "KILL")).isEqualTo(error("ERR unknown subcommand 'KILL'. Try CLIENT HELP."));
            assertThat(exec("CLIENT", "ID", "extra"))
                    .isEqualTo(error("ERR wrong number of arguments for 'client|id' command"));
        }
    }

    @Nested
    class Server {

        @Test
        void configGetReturnsNameValuePairs() {
            assertThat(exec("CONFIG", "GET", "save")).isEqualTo(RespArray.of(bulk("save"), bulk("")));
            assertThat(exec("CONFIG", "GET", "APPENDONLY")).isEqualTo(RespArray.of(bulk("appendonly"), bulk("no")));
            assertThat(exec("CONFIG", "GET", "nonexistent")).isEqualTo(RespArray.EMPTY);
        }

        @Test
        void configGetSupportsGlobsAndSeveralParameters() {
            assertThat(exec("CONFIG", "GET", "*")).isEqualTo(RespArray.of(
                    bulk("appendonly"), bulk("no"), bulk("port"), bulk("6379"), bulk("save"), bulk("")));
            assertThat(exec("CONFIG", "GET", "sa?e", "port")).isEqualTo(RespArray.of(
                    bulk("save"), bulk(""), bulk("port"), bulk("6379")));
        }

        @Test
        void configOnlySupportsGet() {
            assertThat(exec("CONFIG", "SET", "save", "")).isEqualTo(error("ERR unknown subcommand 'SET'. Try CONFIG HELP."));
            assertThat(exec("CONFIG", "GET")).isEqualTo(error("ERR wrong number of arguments for 'config|get' command"));
        }

        @Test
        void commandStubs() {
            assertThat(exec("COMMAND")).isEqualTo(RespArray.EMPTY);
            assertThat(exec("COMMAND", "DOCS")).isEqualTo(RespArray.EMPTY);
            assertThat(exec("COMMAND", "DOCS", "get")).isEqualTo(RespArray.EMPTY);
            assertThat(exec("COMMAND", "COUNT")).isEqualTo(RespInteger.of(dispatcher.commands().size()));
            assertThat(((RespArray) exec("COMMAND", "LIST")).elements()).contains(bulk("get"), bulk("set"), bulk("command"));
            assertThat(exec("COMMAND", "GETKEYS")).isEqualTo(error("ERR unknown subcommand 'GETKEYS'. Try COMMAND HELP."));
        }
    }

    @Test
    void globMatching() {
        assertThat(ServerCommands.globMatches("*", "anything")).isTrue();
        assertThat(ServerCommands.globMatches("max*", "maxmemory")).isTrue();
        assertThat(ServerCommands.globMatches("*memory", "maxmemory")).isTrue();
        assertThat(ServerCommands.globMatches("m?x*y", "maxmemory")).isTrue();
        assertThat(ServerCommands.globMatches("save", "saved")).isFalse();
        assertThat(ServerCommands.globMatches("*x", "maxmemory")).isFalse();
    }

    private RespValue exec(String... arguments) {
        return dispatcher.dispatch(session, Arrays.stream(arguments).map(CommandDispatcherTest::b).toArray(byte[][]::new));
    }

    private static RespBulkString bulk(String text) {
        return RespBulkString.of(text);
    }

    private static RespError error(String message) {
        return new RespError(message);
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
