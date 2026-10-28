package io.keyforge.server.command;

import java.util.List;

import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;

/** DEL, EXISTS, DBSIZE and FLUSHDB. */
final class KeyspaceCommands {

    private final Keyspace keyspace;

    KeyspaceCommands(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    List<CommandSpec> specs() {
        return List.of(
                new CommandSpec("del", -2,
                        (session, args) -> RespInteger.of(keyspace.delete(Arguments.tail(args, 1)))),
                new CommandSpec("exists", -2,
                        (session, args) -> RespInteger.of(keyspace.countExisting(Arguments.tail(args, 1)))),
                new CommandSpec("dbsize", 1, (session, args) -> RespInteger.of(keyspace.size())),
                new CommandSpec("flushdb", -1, (session, args) -> flushdb(args)));
    }

    /** {@code FLUSHDB [ASYNC | SYNC]}. Both modes flush synchronously; the in-memory clear is fast. */
    private RespValue flushdb(byte[][] args) {
        if (args.length > 2 || (args.length == 2 && !Arguments.is(args[1], "ASYNC") && !Arguments.is(args[1], "SYNC"))) {
            throw new CommandException(Arguments.SYNTAX_ERROR);
        }
        keyspace.clear();
        return RespSimpleString.OK;
    }
}
