package io.keyforge.server.command;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Immutable, case-insensitive command lookup by the raw name bytes a client sent.
 *
 * <p>Lookup runs once per request, so it hashes and compares the bytes directly (open addressing, ASCII case folding)
 * instead of building a lowercase {@code String} first.
 */
public final class CommandTable {

    private final CommandSpec[] slots;
    private final byte[][] names;
    private final int mask;
    private final List<CommandSpec> specs;

    public CommandTable(Collection<CommandSpec> commands) {
        int capacity = Integer.highestOneBit(Math.max(4, commands.size() * 4) - 1) << 1;
        slots = new CommandSpec[capacity];
        names = new byte[capacity][];
        mask = capacity - 1;
        for (CommandSpec spec : commands) {
            byte[] name = spec.name().getBytes(StandardCharsets.US_ASCII);
            int slot = hashIgnoreCase(name) & mask;
            while (slots[slot] != null) {
                if (equalsIgnoreCase(names[slot], name)) {
                    throw new IllegalArgumentException("Duplicate command: " + spec.name());
                }
                slot = (slot + 1) & mask;
            }
            slots[slot] = spec;
            names[slot] = name;
        }
        specs = Collections.unmodifiableList(new ArrayList<>(commands));
    }

    /** Returns the command whose name matches {@code name} ignoring ASCII case, or {@code null}. */
    public CommandSpec lookup(byte[] name) {
        for (int slot = hashIgnoreCase(name) & mask; ; slot = (slot + 1) & mask) {
            CommandSpec spec = slots[slot];
            if (spec == null || equalsIgnoreCase(names[slot], name)) {
                return spec;
            }
        }
    }

    public List<CommandSpec> commands() {
        return specs;
    }

    public int size() {
        return specs.size();
    }

    private static int hashIgnoreCase(byte[] name) {
        int hash = 0;
        for (byte b : name) {
            hash = 31 * hash + Arguments.toLowerCase(b);
        }
        return hash ^ (hash >>> 16);
    }

    private static boolean equalsIgnoreCase(byte[] lowercase, byte[] candidate) {
        if (lowercase.length != candidate.length) {
            return false;
        }
        for (int i = 0; i < lowercase.length; i++) {
            if (lowercase[i] != Arguments.toLowerCase(candidate[i])) {
                return false;
            }
        }
        return true;
    }
}
