package io.keyforge.server.command;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.keyforge.storage.KeyValueStore;

/** Builds the command table every KeyForge server exposes. */
public final class StandardCommands {

    private StandardCommands() {
    }

    /**
     * @param store            where data commands read and write
     * @param clock            wall clock used to turn {@code EX}/{@code PX} into absolute expiry instants
     * @param serverVersion    reported by {@code HELLO}
     * @param configParameters reported by {@code CONFIG GET}, keyed by lowercase Redis parameter name
     */
    public static CommandTable create(KeyValueStore store, InstantSource clock, String serverVersion,
                                      Map<String, String> configParameters) {
        AtomicReference<CommandTable> table = new AtomicReference<>();
        List<CommandSpec> specs = new ArrayList<>();
        specs.addAll(new ConnectionCommands(serverVersion).specs());
        specs.addAll(new StringCommands(store, clock).specs());
        specs.addAll(new KeyspaceCommands(store).specs());
        specs.addAll(new ServerCommands(configParameters, table::get).specs());
        table.set(new CommandTable(specs));
        return table.get();
    }
}
