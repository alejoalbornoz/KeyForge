package io.keyforge.raft.grpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.keyforge.raft.LogEntry;
import io.keyforge.raft.runtime.StateMachine;

/** A state machine that records the commands it applied, in order. Nothing of it is durable but its snapshots. */
final class RecordingStateMachine implements StateMachine {

    private final List<String> applied = new ArrayList<>();
    private long appliedIndex;

    @Override
    public long durableAppliedIndex() {
        return 0;
    }

    @Override
    public synchronized Object apply(LogEntry entry) {
        if (entry.kind() == LogEntry.Kind.COMMAND) {
            applied.add(new String(entry.data(), StandardCharsets.UTF_8));
        }
        appliedIndex = entry.index();
        return "applied " + entry.index();
    }

    @Override
    public synchronized void writeSnapshot(OutputStream out) throws IOException {
        DataOutputStream data = new DataOutputStream(out);
        data.writeInt(applied.size());
        for (String command : applied) {
            data.writeUTF(command);
        }
        data.flush();
    }

    @Override
    public synchronized void restoreSnapshot(long index, long term, InputStream in) throws IOException {
        DataInputStream data = new DataInputStream(in);
        int count = data.readInt();
        applied.clear();
        for (int i = 0; i < count; i++) {
            applied.add(data.readUTF());
        }
        appliedIndex = index;
    }

    synchronized List<String> applied() {
        return List.copyOf(applied);
    }

    synchronized long appliedIndex() {
        return appliedIndex;
    }
}
