package io.keyforge.chaos;

/**
 * One event of a test history. Events are numbered in the order they were recorded, which is also the order of their
 * timestamps: nanoseconds since the history began, from one monotonic clock.
 */
public sealed interface HistoryEvent {

    long index();

    long timeNanos();

    /**
     * An invocation or a completion of a client operation.
     *
     * @param process  the client process; a process has at most one operation in flight, and takes a new number after
     *                 an indeterminate completion (its old operation may still take effect)
     * @param function what the operation does
     * @param key      the key it works on
     * @param value    the value written ({@code SET} invocations) or returned ({@code GET} and {@code INCR}
     *                 completions); {@code null} otherwise, and for a {@code GET} of a missing key
     * @param node     the node the client sent it to
     * @param error    why it failed or is indeterminate; {@code null} for invocations and {@code ok} completions
     */
    record Op(long index, long timeNanos, EventType type, int process, OpFunction function, String key, Long value,
              String node, String error) implements HistoryEvent {
    }

    /**
     * A nemesis action. Each action is an {@link EventType#INVOKE} when it begins and an {@link EventType#INFO} once
     * it is done, so the fault may be in effect anywhere from the start's invocation to the stop's completion.
     *
     * @param function    {@code start} (inject the fault) or {@code stop} (heal it)
     * @param fault       the fault's kind
     * @param description the fault: its shape and targets; on completions, also any error applying it
     */
    record Nemesis(long index, long timeNanos, EventType type, String function, FaultKind fault, String description)
            implements HistoryEvent {
    }
}
