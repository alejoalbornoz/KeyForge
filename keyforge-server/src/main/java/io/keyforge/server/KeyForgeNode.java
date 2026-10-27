package io.keyforge.server;

import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.Map;

import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.CommandTable;
import io.keyforge.server.command.StandardCommands;
import io.keyforge.server.netty.NettyServer;
import io.keyforge.server.vthreads.VirtualThreadServer;
import io.keyforge.storage.InMemoryKeyValueStore;
import io.keyforge.storage.KeyValueStore;

/** Composition root: wires the store, the command table and the network server ({@code server.type}) of one node. */
public final class KeyForgeNode implements AutoCloseable {

    private final KeyValueStore store;
    private final KeyForgeServer server;

    private KeyForgeNode(KeyValueStore store, KeyForgeServer server) {
        this.store = store;
        this.server = server;
    }

    /** Builds a node from {@code config} and starts serving; returns once the port is bound. */
    public static KeyForgeNode start(KeyForgeConfig config) throws InterruptedException {
        InstantSource clock = InstantSource.system();
        KeyValueStore store = new InMemoryKeyValueStore(new InMemoryKeyValueStore.Options(
                config.storeSegments(), config.activeExpireInterval(), clock));
        CommandTable commands = StandardCommands.create(store, clock, version(), redisConfigParameters(config));
        CommandDispatcher dispatcher = new CommandDispatcher(commands);
        KeyForgeServer server = switch (config.serverType()) {
            case NETTY -> new NettyServer(config, dispatcher);
            case VIRTUAL_THREADS -> new VirtualThreadServer(config, dispatcher);
        };
        try {
            server.start();
        } catch (InterruptedException | RuntimeException e) {
            store.close();
            throw e;
        }
        return new KeyForgeNode(store, server);
    }

    public int port() {
        return server.port();
    }

    @Override
    public void close() {
        server.close();
        store.close();
    }

    /** The version from the jar manifest, or a placeholder when running from classes. */
    public static String version() {
        String version = KeyForgeNode.class.getPackage().getImplementationVersion();
        return version != null ? version : "0.0.0-dev";
    }

    /**
     * What {@code CONFIG GET} reports, under Redis' parameter names. Persistence is reported as off, which is true
     * of the in-memory engine.
     */
    static Map<String, String> redisConfigParameters(KeyForgeConfig config) {
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("bind", config.bindAddress());
        parameters.put("port", Integer.toString(config.port()));
        parameters.put("databases", "1");
        parameters.put("save", "");
        parameters.put("appendonly", "no");
        parameters.put("maxmemory", "0");
        parameters.put("proto-max-bulk-len", Integer.toString(config.maxBulkLength()));
        parameters.put("io-threads", Integer.toString(config.ioThreads()));
        return parameters;
    }
}
