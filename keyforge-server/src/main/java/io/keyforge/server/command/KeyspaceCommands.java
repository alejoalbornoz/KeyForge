package io.keyforge.server.command;

import java.util.List;

import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;
import io.keyforge.storage.KeyValueStore;

/** DEL, EXISTS, DBSIZE and FLUSHDB. */
final class KeyspaceCommands {

    private final KeyValueStore store;

    KeyspaceCommands(KeyValueStore store) {
        this.store = store;
    }

    List<CommandSpec> specs() {
        return List.of(
                new CommandSpec("del", -2, (session, args) -> RespInteger.of(store.delete(Arguments.tail(args, 1)))),
                new CommandSpec("exists", -2,
                        (session, args) -> RespInteger.of(store.countExisting(Arguments.tail(args, 1)))),
                new CommandSpec("dbsize", 1, (session, args) -> RespInteger.of(store.size())),
                new CommandSpec("flushdb", -1, (session, args) -> flushdb(args)));
    }

    /** {@code FLUSHDB [ASYNC | SYNC]}. Both modes flush synchronously; the in-memory clear is fast. */
    private RespValue flushdb(byte[][] args) {
        if (args.length > 2 || (args.length == 2 && !Arguments.is(args[1], "ASYNC") && !Arguments.is(args[1], "SYNC"))) {
            throw new CommandException(Arguments.SYNTAX_ERROR);
        }
        store.clear();
        return RespSimpleString.OK;
    }
}
