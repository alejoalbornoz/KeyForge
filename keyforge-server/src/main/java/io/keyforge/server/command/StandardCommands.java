package io.keyforge.server.command;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.keyforge.storage.KeyValueStore;

/** Builds the command table every KeyForge server exposes. */
public final class StandardCommands {

    private StandardCommands() {
    }

    /**
     * The commands of a standalone node over {@code store}.
     *
     * @param store            where data commands read and write
     * @param clock            wall clock used to turn {@code EX}/{@code PX} into absolute expiry instants
     * @param serverVersion    reported by {@code HELLO} and {@code INFO}
     * @param configParameters reported by {@code CONFIG GET}, keyed by lowercase Redis parameter name
     */
    public static CommandTable create(KeyValueStore store, InstantSource clock, String serverVersion,
                                      Map<String, String> configParameters) {
        Map<String, String> server = new LinkedHashMap<>();
        server.put("keyforge_version", serverVersion);
        server.put("keyforge_mode", "standalone");
        List<InfoSection> info = List.of(new InfoSection("server", server));
        return create(new LocalKeyspace(store), clock, serverVersion, configParameters, () -> info);
    }

    /**
     * @param keyspace         where data commands read and write
     * @param clock            wall clock used to turn {@code EX}/{@code PX} into absolute expiry instants
     * @param serverVersion    reported by {@code HELLO}
     * @param configParameters reported by {@code CONFIG GET}, keyed by lowercase Redis parameter name
     * @param info             the sections {@code INFO} reports, computed on each call
     */
    public static CommandTable create(Keyspace keyspace, InstantSource clock, String serverVersion,
                                      Map<String, String> configParameters, Supplier<List<InfoSection>> info) {
        AtomicReference<CommandTable> table = new AtomicReference<>();
        List<CommandSpec> specs = new ArrayList<>();
        specs.addAll(new ConnectionCommands(serverVersion).specs());
        specs.addAll(new StringCommands(keyspace, clock).specs());
        specs.addAll(new KeyspaceCommands(keyspace).specs());
        specs.addAll(new ServerCommands(configParameters, table::get, info).specs());
        table.set(new CommandTable(specs));
        return table.get();
    }
}
