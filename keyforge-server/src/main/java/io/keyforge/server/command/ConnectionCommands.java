package io.keyforge.server.command;

import java.util.List;

import io.keyforge.protocol.RespArray;
import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespNull;
import io.keyforge.protocol.RespNumbers;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;

/** PING, ECHO, QUIT, HELLO and CLIENT. */
final class ConnectionCommands {

    private final String serverVersion;

    ConnectionCommands(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    List<CommandSpec> specs() {
        return List.of(
                new CommandSpec("ping", -1, (session, args) -> ping(args)),
                new CommandSpec("echo", 2, (session, args) -> new RespBulkString(args[1])),
                new CommandSpec("quit", -1, (session, args) -> quit(session)),
                new CommandSpec("hello", -1, this::hello),
                new CommandSpec("client", -2, ConnectionCommands::client));
    }

    private static RespValue ping(byte[][] args) {
        return switch (args.length) {
            case 1 -> RespSimpleString.PONG;
            case 2 -> new RespBulkString(args[1]);
            default -> throw Arguments.wrongArity("ping");
        };
    }

    private static RespValue quit(ClientSession session) {
        session.requestClose();
        return RespSimpleString.OK;
    }

    /**
     * {@code HELLO [protover [AUTH username password] [SETNAME name]]}. Only RESP2 is spoken: {@code HELLO 3} fails with
     * {@code NOPROTO}, which tells clients such as Lettuce to fall back to RESP2.
     */
    private RespValue hello(ClientSession session, byte[][] args) {
        int i = 1;
        if (args.length > 1) {
            long version;
            try {
                version = RespNumbers.parseLong(args[1]);
            } catch (NumberFormatException e) {
                throw new CommandException("ERR Protocol version is not an integer or out of range");
            }
            if (version != 2) {
                throw new CommandException("NOPROTO unsupported protocol version");
            }
            i = 2;
        }
        for (; i < args.length; i++) {
            if (Arguments.is(args[i], "AUTH") && i + 2 < args.length) {
                i += 2; // No authentication is configured; like Redis' default user, any credentials are accepted.
            } else if (Arguments.is(args[i], "SETNAME") && i + 1 < args.length) {
                setName(session, args[++i]);
            } else {
                throw new CommandException(
                        "ERR Syntax error in HELLO option '" + Arguments.truncate(args[i], 128) + "'");
            }
        }
        return RespArray.of(
                RespBulkString.of("server"), RespBulkString.of("keyforge"),
                RespBulkString.of("version"), RespBulkString.of(serverVersion),
                RespBulkString.of("proto"), RespInteger.of(2),
                RespBulkString.of("id"), RespInteger.of(session.id()),
                RespBulkString.of("mode"), RespBulkString.of("standalone"),
                RespBulkString.of("role"), RespBulkString.of("master"),
                RespBulkString.of("modules"), RespArray.EMPTY);
    }

    /** {@code CLIENT SETNAME | GETNAME | ID | SETINFO}: what client libraries send while connecting. */
    private static RespValue client(ClientSession session, byte[][] args) {
        byte[] subcommand = args[1];
        if (Arguments.is(subcommand, "SETNAME")) {
            requireCount(args, 3, "client|setname");
            setName(session, args[2]);
            return RespSimpleString.OK;
        }
        if (Arguments.is(subcommand, "GETNAME")) {
            requireCount(args, 2, "client|getname");
            return session.name() == null ? RespNull.BULK_STRING : RespBulkString.of(session.name());
        }
        if (Arguments.is(subcommand, "ID")) {
            requireCount(args, 2, "client|id");
            return RespInteger.of(session.id());
        }
        if (Arguments.is(subcommand, "SETINFO")) {
            requireCount(args, 4, "client|setinfo");
            if (!Arguments.is(args[2], "LIB-NAME") && !Arguments.is(args[2], "LIB-VER")) {
                throw new CommandException("ERR Unrecognized option '" + Arguments.truncate(args[2], 128) + "'");
            }
            return RespSimpleString.OK;
        }
        throw Arguments.unknownSubcommand(subcommand, "CLIENT");
    }

    /** Redis accepts names made of printable ASCII without spaces; an empty name clears it. */
    private static void setName(ClientSession session, byte[] name) {
        for (byte b : name) {
            if (b < '!' || b > '~') {
                throw new CommandException(
                        "ERR Client names cannot contain spaces, newlines or special characters.");
            }
        }
        session.setName(name.length == 0 ? null : Arguments.utf8(name));
    }

    private static void requireCount(byte[][] args, int expected, String fullName) {
        if (args.length != expected) {
            throw Arguments.wrongArity(fullName);
        }
    }
}
