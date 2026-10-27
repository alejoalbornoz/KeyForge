package io.keyforge.server;

import java.util.Arrays;

/** The network front end a node runs, chosen with the {@code server.type} property. */
public enum ServerType {

    /** Netty event loops over NIO; see {@link io.keyforge.server.netty.NettyServer}. */
    NETTY("netty"),

    /**
     * One virtual thread per connection, blocking socket I/O; see
     * {@link io.keyforge.server.vthreads.VirtualThreadServer}.
     */
    VIRTUAL_THREADS("virtual-threads");

    private final String configName;

    ServerType(String configName) {
        this.configName = configName;
    }

    /** The value of {@code server.type} that selects this front end. */
    public String configName() {
        return configName;
    }

    /**
     * Parses a {@code server.type} value; case-insensitive.
     *
     * @throws IllegalArgumentException if no front end has that name
     */
    public static ServerType fromConfigName(String name) {
        for (ServerType type : values()) {
            if (type.configName.equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Property '" + KeyForgeConfig.SERVER_TYPE + "' must be one of "
                + Arrays.stream(values()).map(ServerType::configName).toList() + ", got '" + name + "'");
    }
}
