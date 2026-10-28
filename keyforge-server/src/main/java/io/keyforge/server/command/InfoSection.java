package io.keyforge.server.command;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One section of {@code INFO} output.
 *
 * @param name   lowercase section name, such as {@code server} or {@code raft}
 * @param fields field names and values, in display order
 */
public record InfoSection(String name, Map<String, String> fields) {

    public InfoSection {
        Objects.requireNonNull(name, "name");
        fields = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }
}
