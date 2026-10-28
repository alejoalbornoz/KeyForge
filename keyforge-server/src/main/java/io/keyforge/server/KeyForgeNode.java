package io.keyforge.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.keyforge.raft.Clock;
import io.keyforge.raft.RaftConfig;
import io.keyforge.raft.RaftStatus;
import io.keyforge.raft.grpc.GrpcTransport;
import io.keyforge.raft.grpc.PeerAddress;
import io.keyforge.raft.runtime.RaftRuntime;
import io.keyforge.raft.storage.FileRaftStorage;
import io.keyforge.server.cluster.KeyValueStateMachine;
import io.keyforge.server.cluster.LeaderForwarder;
import io.keyforge.server.cluster.ReplicatedKeyspace;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.CommandTable;
import io.keyforge.server.command.InfoSection;
import io.keyforge.server.command.StandardCommands;
import io.keyforge.server.netty.NettyServer;
import io.keyforge.server.vthreads.VirtualThreadServer;
import io.keyforge.storage.InMemoryKeyValueStore;
import io.keyforge.storage.KeyValueStore;
import io.keyforge.storage.lsm.LsmKeyValueStore;

/**
 * Composition root: wires the store ({@code store.engine}), the command table and the network server
 * ({@code server.type}) of one node. With {@code cluster.members} set, the node also runs Raft: the LSM store becomes
 * the replicated state machine (without its own WAL), writes go through the log, reads through ReadIndex, and data
 * commands that reach a follower are forwarded to the leader.
 */
public final class KeyForgeNode implements AutoCloseable {

    private final KeyValueStore store;
    private final KeyForgeServer server;
    /** {@code null} on a standalone node. */
    private final RaftRuntime raft;

    private KeyForgeNode(KeyValueStore store, KeyForgeServer server, RaftRuntime raft) {
        this.store = store;
        this.server = server;
        this.raft = raft;
    }

    /** Builds a node from {@code config} and starts serving; returns once the port is bound. */
    public static KeyForgeNode start(KeyForgeConfig config) throws InterruptedException {
        return config.clustered() ? startClustered(config) : startStandalone(config);
    }

    private static KeyForgeNode startStandalone(KeyForgeConfig config) throws InterruptedException {
        InstantSource clock = InstantSource.system();
        KeyValueStore store = openStore(config, clock);
        CommandTable commands = StandardCommands.create(store, clock, version(), redisConfigParameters(config));
        KeyForgeServer server = startServer(config, new CommandDispatcher(commands), store);
        return new KeyForgeNode(store, server, null);
    }

    private static KeyForgeNode startClustered(KeyForgeConfig config) throws InterruptedException {
        InstantSource clock = InstantSource.system();
        LsmKeyValueStore store = LsmKeyValueStore.open(lsmOptions(config, clock).withReplicated(true));
        RaftRuntime raft;
        GrpcTransport transport;
        try {
            Map<String, PeerAddress> peers = new LinkedHashMap<>(config.clusterMembers());
            PeerAddress self = peers.remove(config.nodeId());
            transport = new GrpcTransport(config.nodeId(), config.raftBind(), self.port(), peers,
                    config.raftDataDirectory().resolve("spool"));
            raft = RaftRuntime.start(raftConfig(config), Clock.system(), transport,
                    FileRaftStorage.open(config.raftDataDirectory()), new KeyValueStateMachine(store),
                    new RaftRuntime.Options(RaftRuntime.Options.DEFAULT_TICK_MILLIS, config.snapshotThreshold()));
        } catch (IOException e) {
            store.close();
            throw new UncheckedIOException("Cannot open the Raft data in " + config.raftDataDirectory(), e);
        } catch (RuntimeException e) {
            store.close();
            throw e;
        }
        try {
            ReplicatedKeyspace keyspace = new ReplicatedKeyspace(raft, store, clock, config.requestTimeout());
            AtomicReference<CommandDispatcher> dispatcher = new AtomicReference<>();
            LeaderForwarder forwarder = new LeaderForwarder(raft, transport, dispatcher::get, config.requestTimeout(),
                    config.maxBulkLength(), config.maxArrayLength());
            CommandTable commands = StandardCommands.create(keyspace, clock, version(),
                    redisConfigParameters(config), () -> infoSections(config, raft));
            dispatcher.set(new CommandDispatcher(commands, forwarder));
            transport.setForwardHandler(forwarder);
            KeyForgeServer server = startServer(config, dispatcher.get(), null);
            return new KeyForgeNode(store, server, raft);
        } catch (InterruptedException | RuntimeException e) {
            raft.close();
            store.close();
            throw e;
        }
    }

    private static KeyForgeServer startServer(KeyForgeConfig config, CommandDispatcher dispatcher,
                                              KeyValueStore storeToCloseOnFailure) throws InterruptedException {
        KeyForgeServer server = switch (config.serverType()) {
            case NETTY -> new NettyServer(config, dispatcher);
            case VIRTUAL_THREADS -> new VirtualThreadServer(config, dispatcher);
        };
        try {
            server.start();
        } catch (InterruptedException | RuntimeException e) {
            if (storeToCloseOnFailure != null) {
                storeToCloseOnFailure.close();
            }
            throw e;
        }
        return server;
    }

    public int port() {
        return server.port();
    }

    /** The Raft node, or {@code null} on a standalone node. */
    public RaftRuntime raft() {
        return raft;
    }

    /** Opens the engine {@code store.engine} selects; the LSM engine recovers whatever is in the data directory. */
    static KeyValueStore openStore(KeyForgeConfig config, InstantSource clock) {
        return switch (config.storeEngine()) {
            case LSM -> LsmKeyValueStore.open(lsmOptions(config, clock));
            case MEMORY -> new InMemoryKeyValueStore(new InMemoryKeyValueStore.Options(
                    config.storeSegments(), config.activeExpireInterval(), clock));
        };
    }

    private static LsmKeyValueStore.Options lsmOptions(KeyForgeConfig config, InstantSource clock) {
        return new LsmKeyValueStore.Options(
                config.dataDirectory(),
                config.fsyncPolicy(),
                config.groupCommitInterval(),
                config.groupCommitBytes(),
                config.memtableSizeBytes(),
                LsmKeyValueStore.Options.DEFAULT_COMPACTION_MIN_THRESHOLD,
                LsmKeyValueStore.Options.DEFAULT_COMPACTION_SMALL_TABLE_BYTES,
                clock);
    }

    static RaftConfig raftConfig(KeyForgeConfig config) {
        long electionTimeout = config.electionTimeout().toMillis();
        return RaftConfig.of(config.nodeId(), new ArrayList<>(config.clusterMembers().keySet()))
                .withTimeouts(electionTimeout, 2 * electionTimeout, config.heartbeatInterval().toMillis());
    }

    @Override
    public void close() {
        server.close();
        if (raft != null) {
            raft.close();
        }
        store.close();
    }

    /** The version from the jar manifest, or a placeholder when running from classes. */
    public static String version() {
        String version = KeyForgeNode.class.getPackage().getImplementationVersion();
        return version != null ? version : "0.0.0-dev";
    }

    /**
     * What {@code CONFIG GET} reports, under Redis' parameter names. {@code appendonly} says whether writes are logged
     * before they are acknowledged: yes for the LSM engine and its write-ahead log (or, in a cluster, the Raft log),
     * no for the in-memory engine. Snapshots ({@code save}) do not exist.
     */
    static Map<String, String> redisConfigParameters(KeyForgeConfig config) {
        boolean durable = config.storeEngine() == StoreEngine.LSM;
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("bind", config.bindAddress());
        parameters.put("port", Integer.toString(config.port()));
        parameters.put("databases", "1");
        parameters.put("save", "");
        parameters.put("appendonly", durable ? "yes" : "no");
        if (durable) {
            parameters.put("dir", config.dataDirectory().toAbsolutePath().toString());
        }
        parameters.put("maxmemory", "0");
        parameters.put("proto-max-bulk-len", Integer.toString(config.maxBulkLength()));
        parameters.put("io-threads", Integer.toString(config.ioThreads()));
        return parameters;
    }

    /** {@code INFO server} and {@code INFO raft} of a cluster node. */
    static List<InfoSection> infoSections(KeyForgeConfig config, RaftRuntime raft) {
        Map<String, String> server = new LinkedHashMap<>();
        server.put("keyforge_version", version());
        server.put("keyforge_mode", "raft");
        server.put("node_id", config.nodeId());
        RaftStatus status = raft.status();
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("role", status.role().name().toLowerCase(java.util.Locale.ROOT));
        fields.put("term", Long.toString(status.term()));
        fields.put("leader_id", status.leaderId() == null ? "" : status.leaderId());
        fields.put("commit_index", Long.toString(status.commitIndex()));
        fields.put("last_applied", Long.toString(raft.appliedIndex()));
        fields.put("last_log_index", Long.toString(status.lastLogIndex()));
        fields.put("last_log_term", Long.toString(status.lastLogTerm()));
        fields.put("snapshot_index", Long.toString(status.snapshotIndex()));
        fields.put("members", String.join(",", config.clusterMembers().keySet()));
        return List.of(new InfoSection("server", server), new InfoSection("raft", fields));
    }
}
