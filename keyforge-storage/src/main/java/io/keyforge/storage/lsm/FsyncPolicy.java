package io.keyforge.storage.lsm;

import java.util.Locale;

/**
 * When the write-ahead log is forced to stable storage. In both modes a write is acknowledged only after its WAL record
 * has been handed to the operating system, so acknowledged writes survive a crash of the process ({@code kill -9}).
 * The modes differ in what survives a crash of the machine.
 */
public enum FsyncPolicy {

    /**
     * Acknowledge a write only after an {@code fsync} that covers it. Writes that arrive while an {@code fsync} is
     * running share the next one, so concurrent writers amortise its cost. Survives power loss.
     */
    ALWAYS("always"),

    /**
     * Acknowledge a write once its record is written; {@code fsync} once the configured interval has passed since the
     * previous one, or once the configured number of bytes is waiting, whichever comes first. A power loss can lose
     * at most that window.
     */
    GROUP_COMMIT("group-commit");

    private final String configName;

    FsyncPolicy(String configName) {
        this.configName = configName;
    }

    /** The name used in configuration files: {@code always} or {@code group-commit}. */
    public String configName() {
        return configName;
    }

    /** Parses a {@link #configName()}, ignoring case. */
    public static FsyncPolicy fromConfigName(String name) {
        String normalized = name.strip().toLowerCase(Locale.ROOT);
        for (FsyncPolicy policy : values()) {
            if (policy.configName.equals(normalized)) {
                return policy;
            }
        }
        throw new IllegalArgumentException("Unknown fsync policy '" + name + "'; expected always or group-commit");
    }
}
