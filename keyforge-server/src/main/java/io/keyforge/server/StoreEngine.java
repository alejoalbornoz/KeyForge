package io.keyforge.server;

import java.util.Arrays;

/** The storage engine a node runs, chosen with the {@code store.engine} property. */
public enum StoreEngine {

    /** The durable log-structured merge tree; see {@link io.keyforge.storage.lsm.LsmKeyValueStore}. */
    LSM("lsm"),

    /** The segmented in-memory hash table, which loses everything on restart; see
     * {@link io.keyforge.storage.InMemoryKeyValueStore}. */
    MEMORY("memory");

    private final String configName;

    StoreEngine(String configName) {
        this.configName = configName;
    }

    /** The value of {@code store.engine} that selects this engine. */
    public String configName() {
        return configName;
    }

    /**
     * Parses a {@code store.engine} value; case-insensitive.
     *
     * @throws IllegalArgumentException if no engine has that name
     */
    public static StoreEngine fromConfigName(String name) {
        for (StoreEngine engine : values()) {
            if (engine.configName.equalsIgnoreCase(name)) {
                return engine;
            }
        }
        throw new IllegalArgumentException("Property '" + KeyForgeConfig.STORE_ENGINE + "' must be one of "
                + Arrays.stream(values()).map(StoreEngine::configName).toList() + ", got '" + name + "'");
    }
}
