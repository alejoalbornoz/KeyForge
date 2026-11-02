package io.keyforge.chaos;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * The history of a test: every client operation's invocation and completion, and every nemesis action, with
 * timestamps. Thread-safe. Recording takes the timestamp under the same lock that appends the event, so indexes and
 * timestamps grow together.
 *
 * <p>{@link #writeJson} exports it in the format the Porcupine checker reads ({@code keyforge-chaos/checker}).
 */
public final class History {

    /** The format name and version written into every history file. */
    public static final String FORMAT = "keyforge-history/1";

    private final LongSupplier nanoClock;
    private final long origin;
    private final Instant started;
    private final List<HistoryEvent> events = new ArrayList<>();

    public History() {
        this(System::nanoTime);
    }

    /** @param nanoClock a monotonic clock in nanoseconds; tests supply their own */
    History(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
        this.origin = nanoClock.getAsLong();
        this.started = Instant.now();
    }

    /** Records that {@code process} is about to send an operation; complete it with {@link #complete}. */
    public synchronized HistoryEvent.Op invoke(int process, OpFunction function, String key, Long value, String node) {
        HistoryEvent.Op event = new HistoryEvent.Op(events.size(), now(), EventType.INVOKE, process, function, key,
                value, node, null);
        events.add(event);
        return event;
    }

    /**
     * Records the completion of an invocation.
     *
     * @param type  {@code ok}, {@code fail} or {@code info}
     * @param value the result of a {@code GET} or {@code INCR}; ignored otherwise
     * @param error why it failed or is indeterminate
     */
    public synchronized HistoryEvent.Op complete(HistoryEvent.Op invocation, EventType type, Long value,
                                                 String error) {
        if (invocation.type() != EventType.INVOKE || type == EventType.INVOKE) {
            throw new IllegalArgumentException("complete an invocation with ok, fail or info");
        }
        Long result = invocation.function() == OpFunction.SET ? invocation.value() : value;
        HistoryEvent.Op event = new HistoryEvent.Op(events.size(), now(), type, invocation.process(),
                invocation.function(), invocation.key(), result, invocation.node(),
                type == EventType.OK ? null : error);
        events.add(event);
        return event;
    }

    /** Records a nemesis action: {@code invoke} when it begins, {@code info} when it is done. */
    public synchronized HistoryEvent.Nemesis nemesis(EventType type, String function, FaultKind fault,
                                                     String description) {
        HistoryEvent.Nemesis event = new HistoryEvent.Nemesis(events.size(), now(), type, function, fault,
                description);
        events.add(event);
        return event;
    }

    /** A snapshot of the events so far, in order. */
    public synchronized List<HistoryEvent> events() {
        return List.copyOf(events);
    }

    /** Wall-clock time at which the history began (its time 0). */
    public Instant started() {
        return started;
    }

    /** Writes the history as JSON: the format name, the start time, {@code metadata} and the events, one per line. */
    public void writeJson(Path file, Map<String, String> metadata) throws IOException {
        try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writeJson(out, metadata);
        }
    }

    void writeJson(Writer out, Map<String, String> metadata) throws IOException {
        out.write("{\n  \"format\": " + Json.quote(FORMAT) + ",\n");
        out.write("  \"started\": " + Json.quote(started.toString()) + ",\n");
        out.write("  \"time_unit\": \"ns\",\n");
        out.write("  \"metadata\": {");
        String separator = "\n";
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            out.write(separator + "    " + Json.quote(entry.getKey()) + ": " + Json.quote(entry.getValue()));
            separator = ",\n";
        }
        out.write(metadata.isEmpty() ? "},\n" : "\n  },\n");
        out.write("  \"events\": [");
        separator = "\n";
        for (HistoryEvent event : events()) {
            out.write(separator + "    " + toJson(event));
            separator = ",\n";
        }
        out.write(separator.equals("\n") ? "]\n}\n" : "\n  ]\n}\n");
    }

    static String toJson(HistoryEvent event) {
        StringBuilder json = new StringBuilder(160);
        json.append("{\"index\":").append(event.index()).append(",\"time\":").append(event.timeNanos());
        switch (event) {
            case HistoryEvent.Op op -> {
                json.append(",\"type\":").append(Json.quote(op.type().label()))
                        .append(",\"process\":").append(op.process())
                        .append(",\"f\":").append(Json.quote(op.function().label()))
                        .append(",\"key\":").append(Json.quote(op.key()))
                        .append(",\"value\":").append(op.value() == null ? "null" : op.value().toString())
                        .append(",\"node\":").append(Json.quote(op.node()));
                if (op.error() != null) {
                    json.append(",\"error\":").append(Json.quote(op.error()));
                }
            }
            case HistoryEvent.Nemesis nemesis -> json.append(",\"type\":").append(Json.quote(nemesis.type().label()))
                    .append(",\"process\":\"nemesis\"")
                    .append(",\"f\":").append(Json.quote(nemesis.function()))
                    .append(",\"fault\":").append(Json.quote(nemesis.fault().label()))
                    .append(",\"value\":").append(Json.quote(nemesis.description()));
        }
        return json.append('}').toString();
    }

    private long now() {
        return nanoClock.getAsLong() - origin;
    }
}
