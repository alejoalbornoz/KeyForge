package io.keyforge.raft.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.keyforge.raft.LogEntry;

/**
 * The global facts the simulator checks every step against.
 *
 * <ul>
 *   <li><b>Election safety</b>: at most one leader per term.</li>
 *   <li><b>Log matching</b>: an (index, term) pair always names the same entry, with the same predecessor term. By
 *       induction, two logs that share an entry share everything before it.</li>
 *   <li><b>State machine safety</b>: no two nodes apply different entries at the same index, and a snapshot or durable
 *       image at index i holds exactly the state of applying entries 1..i.</li>
 *   <li><b>Leader completeness</b>: a new leader's log holds every entry any node has applied.</li>
 *   <li><b>Linearizable reads</b>: a ReadIndex read reflects every entry applied anywhere before the read began.</li>
 * </ul>
 */
final class Invariants {

    private record EntryId(long index, long term) {
    }

    private record EntryFacts(LogEntry entry, long previousTerm) {
    }

    private final Map<Long, String> leaderByTerm = new HashMap<>();
    private final Map<EntryId, EntryFacts> entries = new HashMap<>();
    /** {@code applied.get(i - 1)} is the entry applied at index i. */
    private final List<LogEntry> applied = new ArrayList<>();
    private final List<Long> sequenceHashes = new ArrayList<>();
    private final List<Long> dataHashes = new ArrayList<>();

    void onLeader(String node, long term) {
        String previous = leaderByTerm.putIfAbsent(term, node);
        if (previous != null && !previous.equals(node)) {
            throw new InvariantViolation("two leaders in term " + term + ": " + previous + " and " + node);
        }
    }

    /** {@code entry} was persisted at {@code node} right after an entry of term {@code previousTerm}. */
    void onPersisted(String node, LogEntry entry, long previousTerm) {
        EntryFacts facts = new EntryFacts(entry, previousTerm);
        EntryFacts known = entries.putIfAbsent(new EntryId(entry.index(), entry.term()), facts);
        if (known != null && (!known.entry().equals(entry) || known.previousTerm() != previousTerm)) {
            throw new InvariantViolation("log matching: " + node + " stored " + entry + " after term " + previousTerm
                    + " but " + known.entry() + " after term " + known.previousTerm() + " exists with the same index"
                    + " and term" + (known.entry().equals(entry) ? "" : " and different data"));
        }
    }

    void onApplied(String node, LogEntry entry, KvMachine machine) {
        int position = (int) entry.index() - 1;
        if (position < applied.size()) {
            LogEntry known = applied.get(position);
            if (!known.equals(entry)) {
                throw new InvariantViolation("state machine safety: " + node + " applied " + entry + " at index "
                        + entry.index() + " where " + known + " was applied before");
            }
            checkState(node, "applying", machine);
        } else if (position == applied.size()) {
            applied.add(entry);
            sequenceHashes.add(machine.sequenceHash());
            dataHashes.add(machine.dataHash());
        } else {
            throw new InvariantViolation(node + " applied " + entry + " but nothing was ever applied at "
                    + (applied.size() + 1));
        }
    }

    /** A machine rebuilt from a snapshot or image must equal the state of applying its prefix of the log. */
    void checkState(String node, String what, KvMachine machine) {
        long index = machine.appliedIndex();
        if (index == 0) {
            return;
        }
        if (index > applied.size()) {
            throw new InvariantViolation(node + " " + what + " a state at " + index + ", beyond anything applied ("
                    + applied.size() + ")");
        }
        if (sequenceHashes.get((int) index - 1) != machine.sequenceHash()
                || dataHashes.get((int) index - 1) != machine.dataHash()
                || applied.get((int) index - 1).term() != machine.appliedTerm()) {
            throw new InvariantViolation("state machine safety: " + node + " " + what + " a state at index " + index
                    + " that differs from the applied log");
        }
    }

    /** A new leader must hold every entry applied anywhere; {@code disk} is its durable log. */
    void onLeaderElected(String node, long term, SimDisk disk) {
        onLeader(node, term);
        if (disk.lastIndex() < applied.size()) {
            throw new InvariantViolation("leader completeness: " + node + " became leader of term " + term
                    + " with a log ending at " + disk.lastIndex() + " but index " + applied.size() + " was applied");
        }
        for (long index = disk.logBaseIndex + 1; index <= applied.size(); index++) {
            LogEntry mine = disk.entry(index);
            LogEntry committed = applied.get((int) index - 1);
            if (!mine.equals(committed)) {
                throw new InvariantViolation("leader completeness: " + node + " became leader of term " + term
                        + " with " + mine + " where " + committed + " was applied");
            }
        }
    }

    long maxApplied() {
        return applied.size();
    }

    /** The entry applied at {@code index} (1-based). */
    LogEntry appliedEntry(long index) {
        return applied.get((int) index - 1);
    }
}
