package io.keyforge.raft.grpc;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

import io.keyforge.raft.LogEntry;
import io.keyforge.raft.RaftMessage;
import io.keyforge.raft.grpc.proto.Entry;
import io.keyforge.raft.grpc.proto.EntryKind;
import io.keyforge.raft.grpc.proto.Envelope;

/** Converts {@link RaftMessage}s to and from their protobuf form. InstallSnapshot travels as chunks instead. */
final class ProtoCodec {

    private ProtoCodec() {
    }

    static Envelope encode(RaftMessage message) {
        Envelope.Builder envelope = Envelope.newBuilder()
                .setFrom(message.from())
                .setTo(message.to())
                .setTerm(message.term());
        switch (message) {
            case RaftMessage.VoteRequest request -> envelope.setVoteRequest(
                    io.keyforge.raft.grpc.proto.VoteRequest.newBuilder()
                            .setLastLogIndex(request.lastLogIndex())
                            .setLastLogTerm(request.lastLogTerm())
                            .setPreVote(request.preVote()));
            case RaftMessage.VoteResponse response -> envelope.setVoteResponse(
                    io.keyforge.raft.grpc.proto.VoteResponse.newBuilder()
                            .setGranted(response.granted())
                            .setPreVote(response.preVote()));
            case RaftMessage.AppendEntries append -> {
                io.keyforge.raft.grpc.proto.AppendEntries.Builder builder =
                        io.keyforge.raft.grpc.proto.AppendEntries.newBuilder()
                                .setPrevLogIndex(append.prevLogIndex())
                                .setPrevLogTerm(append.prevLogTerm())
                                .setLeaderCommit(append.leaderCommit())
                                .setRound(append.round());
                for (LogEntry entry : append.entries()) {
                    builder.addEntries(Entry.newBuilder()
                            .setIndex(entry.index())
                            .setTerm(entry.term())
                            .setKind(entry.kind() == LogEntry.Kind.NOOP ? EntryKind.ENTRY_KIND_NOOP
                                    : EntryKind.ENTRY_KIND_COMMAND)
                            .setData(ByteString.copyFrom(entry.data())));
                }
                envelope.setAppendEntries(builder);
            }
            case RaftMessage.AppendEntriesResponse response -> envelope.setAppendEntriesResponse(
                    io.keyforge.raft.grpc.proto.AppendEntriesResponse.newBuilder()
                            .setSuccess(response.success())
                            .setMatchIndex(response.matchIndex())
                            .setHintIndex(response.hintIndex())
                            .setRound(response.round()));
            case RaftMessage.InstallSnapshotResponse response -> envelope.setInstallSnapshotResponse(
                    io.keyforge.raft.grpc.proto.InstallSnapshotResponse.newBuilder()
                            .setMatchIndex(response.matchIndex())
                            .setRound(response.round()));
            case RaftMessage.InstallSnapshot _ ->
                    throw new IllegalArgumentException("InstallSnapshot is streamed, not enveloped");
        }
        return envelope.build();
    }

    static RaftMessage decode(Envelope envelope) {
        String from = envelope.getFrom();
        String to = envelope.getTo();
        long term = envelope.getTerm();
        return switch (envelope.getBodyCase()) {
            case VOTE_REQUEST -> {
                io.keyforge.raft.grpc.proto.VoteRequest request = envelope.getVoteRequest();
                yield new RaftMessage.VoteRequest(from, to, term, request.getLastLogIndex(), request.getLastLogTerm(),
                        request.getPreVote());
            }
            case VOTE_RESPONSE -> new RaftMessage.VoteResponse(from, to, term, envelope.getVoteResponse().getGranted(),
                    envelope.getVoteResponse().getPreVote());
            case APPEND_ENTRIES -> {
                io.keyforge.raft.grpc.proto.AppendEntries append = envelope.getAppendEntries();
                List<LogEntry> entries = new ArrayList<>(append.getEntriesCount());
                for (Entry entry : append.getEntriesList()) {
                    LogEntry.Kind kind = entry.getKind() == EntryKind.ENTRY_KIND_NOOP
                            ? LogEntry.Kind.NOOP : LogEntry.Kind.COMMAND;
                    entries.add(new LogEntry(entry.getIndex(), entry.getTerm(), kind, entry.getData().toByteArray()));
                }
                yield new RaftMessage.AppendEntries(from, to, term, append.getPrevLogIndex(), append.getPrevLogTerm(),
                        entries, append.getLeaderCommit(), append.getRound());
            }
            case APPEND_ENTRIES_RESPONSE -> {
                io.keyforge.raft.grpc.proto.AppendEntriesResponse response = envelope.getAppendEntriesResponse();
                yield new RaftMessage.AppendEntriesResponse(from, to, term, response.getSuccess(),
                        response.getMatchIndex(), response.getHintIndex(), response.getRound());
            }
            case INSTALL_SNAPSHOT_RESPONSE -> new RaftMessage.InstallSnapshotResponse(from, to, term,
                    envelope.getInstallSnapshotResponse().getMatchIndex(),
                    envelope.getInstallSnapshotResponse().getRound());
            case BODY_NOT_SET -> throw new IllegalArgumentException("empty envelope from " + from);
        };
    }
}
