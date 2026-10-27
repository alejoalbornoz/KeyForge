package io.keyforge.server;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * The typed configuration of a node, and the only place configuration is read.
 *
 * <p>Values are resolved from, in increasing order of precedence: built-in defaults, a properties file, environment
 * variables, and command-line overrides. A property such as {@code server.io-threads} is overridden by the environment
 * variable {@code KEYFORGE_SERVER_IO_THREADS} and by the command-line option {@code --server.io-threads=4}. Unknown
 * property names in the file or on the command line are rejected, so typos fail loudly.
 *
 * @param nodeId               identifies the node in logs (MDC key {@code nodeId})
 * @param bindAddress          address the server listens on
 * @param port                 TCP port; 0 picks a free one
 * @param serverType           network front end: Netty event loops or one virtual thread per connection
 * @param ioThreads            Netty event-loop threads; 0 means Netty's default of twice the CPU count. Ignored by the
 *                             virtual-thread server, whose carrier threads are the JDK's default scheduler
 * @param maxBulkLength        largest accepted bulk string, like Redis' {@code proto-max-bulk-len}
 * @param maxArrayLength       most arguments accepted in one request
 * @param storeSegments        lock segments of the in-memory store; a power of two
 * @param activeExpireInterval period of the active-expiry task; 0 disables it
 */
public record KeyForgeConfig(
        String nodeId,
        String bindAddress,
        int port,
        ServerType serverType,
        int ioThreads,
        int maxBulkLength,
        int maxArrayLength,
        int storeSegments,
        Duration activeExpireInterval) {

    public static final String NODE_ID = "node.id";
    public static final String SERVER_BIND = "server.bind";
    public static final String SERVER_PORT = "server.port";
    public static final String SERVER_TYPE = "server.type";
    public static final String SERVER_IO_THREADS = "server.io-threads";
    public static final String PROTOCOL_MAX_BULK_LENGTH = "protocol.max-bulk-length";
    public static final String PROTOCOL_MAX_ARRAY_LENGTH = "protocol.max-array-length";
    public static final String STORE_SEGMENTS = "store.segments";
    public static final String STORE_ACTIVE_EXPIRE_INTERVAL_MS = "store.active-expire-interval-ms";

    private static final String ENV_PREFIX = "KEYFORGE_";

    private static final Map<String, String> DEFAULTS = defaultValues();

    public KeyForgeConfig {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(bindAddress, "bindAddress");
        Objects.requireNonNull(serverType, "serverType");
        Objects.requireNonNull(activeExpireInterval, "activeExpireInterval");
        require(port >= 0 && port <= 65535, SERVER_PORT, "must be between 0 and 65535");
        require(ioThreads >= 0, SERVER_IO_THREADS, "must not be negative");
        require(maxBulkLength >= 1, PROTOCOL_MAX_BULK_LENGTH, "must be positive");
        require(maxArrayLength >= 1, PROTOCOL_MAX_ARRAY_LENGTH, "must be positive");
        require(storeSegments >= 1 && storeSegments <= 65536 && Integer.bitCount(storeSegments) == 1,
                STORE_SEGMENTS, "must be a power of two between 1 and 65536");
        require(!activeExpireInterval.isNegative(), STORE_ACTIVE_EXPIRE_INTERVAL_MS, "must not be negative");
    }

    /** The built-in defaults. */
    public static KeyForgeConfig defaults() {
        return fromMap(DEFAULTS);
    }

    /**
     * Resolves the configuration.
     *
     * @param file        optional properties file
     * @param environment usually {@link System#getenv()}
     * @param overrides   highest-precedence values, keyed by property name
     * @throws IllegalArgumentException for unknown properties or invalid values
     * @throws UncheckedIOException     if the file cannot be read
     */
    public static KeyForgeConfig load(Path file, Map<String, String> environment, Map<String, String> overrides) {
        Map<String, String> values = new LinkedHashMap<>(DEFAULTS);
        if (file != null) {
            values.putAll(checkKnown(readProperties(file), "in " + file));
        }
        for (String key : DEFAULTS.keySet()) {
            String fromEnvironment = environment.get(environmentVariable(key));
            if (fromEnvironment != null) {
                values.put(key, fromEnvironment);
            }
        }
        values.putAll(checkKnown(overrides, "on the command line"));
        return fromMap(values);
    }

    /**
     * Resolves the configuration from command-line arguments: {@code --config <file>} plus any number of
     * {@code --<property>=<value>} or {@code --<property> <value>} overrides.
     */
    public static KeyForgeConfig fromCommandLine(String[] args, Map<String, String> environment) {
        Path file = null;
        Map<String, String> overrides = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--") || arg.length() == 2) {
                throw new IllegalArgumentException("Unexpected argument '" + arg + "'; expected --<property>=<value>");
            }
            String name;
            String value;
            int equals = arg.indexOf('=');
            if (equals > 0) {
                name = arg.substring(2, equals);
                value = arg.substring(equals + 1);
            } else if (i + 1 < args.length) {
                name = arg.substring(2);
                value = args[++i];
            } else {
                throw new IllegalArgumentException("Missing value for '" + arg + "'");
            }
            if (name.equals("config")) {
                file = Path.of(value);
            } else {
                overrides.put(name, value);
            }
        }
        return load(file, environment, overrides);
    }

    /** The environment variable that overrides {@code property}: {@code server.io-threads -> KEYFORGE_SERVER_IO_THREADS}. */
    public static String environmentVariable(String property) {
        return ENV_PREFIX + property.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_');
    }

    /** The configuration as property name/value pairs, in a stable order. */
    public Map<String, String> toProperties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(NODE_ID, nodeId);
        properties.put(SERVER_BIND, bindAddress);
        properties.put(SERVER_PORT, Integer.toString(port));
        properties.put(SERVER_TYPE, serverType.configName());
        properties.put(SERVER_IO_THREADS, Integer.toString(ioThreads));
        properties.put(PROTOCOL_MAX_BULK_LENGTH, Integer.toString(maxBulkLength));
        properties.put(PROTOCOL_MAX_ARRAY_LENGTH, Integer.toString(maxArrayLength));
        properties.put(STORE_SEGMENTS, Integer.toString(storeSegments));
        properties.put(STORE_ACTIVE_EXPIRE_INTERVAL_MS, Long.toString(activeExpireInterval.toMillis()));
        return properties;
    }

    public KeyForgeConfig withPort(int newPort) {
        return new KeyForgeConfig(nodeId, bindAddress, newPort, serverType, ioThreads, maxBulkLength, maxArrayLength,
                storeSegments, activeExpireInterval);
    }

    public KeyForgeConfig withServerType(ServerType newServerType) {
        return new KeyForgeConfig(nodeId, bindAddress, port, newServerType, ioThreads, maxBulkLength, maxArrayLength,
                storeSegments, activeExpireInterval);
    }

    private static Map<String, String> defaultValues() {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put(NODE_ID, "node-1");
        defaults.put(SERVER_BIND, "0.0.0.0");
        defaults.put(SERVER_PORT, "6379");
        defaults.put(SERVER_TYPE, ServerType.NETTY.configName());
        defaults.put(SERVER_IO_THREADS, "0");
        defaults.put(PROTOCOL_MAX_BULK_LENGTH, Integer.toString(512 * 1024 * 1024));
        defaults.put(PROTOCOL_MAX_ARRAY_LENGTH, Integer.toString(1024 * 1024));
        defaults.put(STORE_SEGMENTS, "256");
        defaults.put(STORE_ACTIVE_EXPIRE_INTERVAL_MS, "100");
        return Collections.unmodifiableMap(defaults);
    }

    private static KeyForgeConfig fromMap(Map<String, String> values) {
        return new KeyForgeConfig(
                values.get(NODE_ID).strip(),
                values.get(SERVER_BIND).strip(),
                parseInt(values, SERVER_PORT),
                ServerType.fromConfigName(values.get(SERVER_TYPE).strip()),
                parseInt(values, SERVER_IO_THREADS),
                parseInt(values, PROTOCOL_MAX_BULK_LENGTH),
                parseInt(values, PROTOCOL_MAX_ARRAY_LENGTH),
                parseInt(values, STORE_SEGMENTS),
                Duration.ofMillis(parseInt(values, STORE_ACTIVE_EXPIRE_INTERVAL_MS)));
    }

    private static Map<String, String> readProperties(Path file) {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read config file " + file, e);
        }
        Map<String, String> values = new LinkedHashMap<>();
        properties.stringPropertyNames().forEach(name -> values.put(name, properties.getProperty(name)));
        return values;
    }

    private static Map<String, String> checkKnown(Map<String, String> values, String where) {
        for (String key : values.keySet()) {
            if (!DEFAULTS.containsKey(key)) {
                throw new IllegalArgumentException("Unknown property '" + key + "' " + where
                        + "; known properties: " + DEFAULTS.keySet().stream().sorted().toList());
            }
        }
        return values;
    }

    private static int parseInt(Map<String, String> values, String key) {
        String value = values.get(key).strip();
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Property '" + key + "' must be an integer, got '" + value + "'");
        }
    }

    private static void require(boolean condition, String key, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Property '" + key + "' " + message);
        }
    }
}
