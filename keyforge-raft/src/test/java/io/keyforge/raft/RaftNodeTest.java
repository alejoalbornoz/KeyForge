package io.keyforge.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.keyforge.raft.RaftAction.AppendLog;
import io.keyforge.raft.RaftAction.Apply;
import io.keyforge.raft.RaftAction.CompactLog;
import io.keyforge.raft.RaftAction.PersistState;
import io.keyforge.raft.RaftAction.ProposalAccepted;
import io.keyforge.raft.RaftAction.ProposalRejected;
import io.keyforge.raft.RaftAction.ReadReady;
import io.keyforge.raft.RaftAction.ReadRejected;
import io.keyforge.raft.RaftAction.RestoreSnapshot;
import io.keyforge.raft.RaftAction.Send;
import io.keyforge.raft.RaftAction.SendSnapshot;
import io.keyforge.raft.RaftAction.TruncateLog;
import io.keyforge.raft.RaftMessage.AppendEntries;
import io.keyforge.raft.RaftMessage.AppendEntriesResponse;
import io.keyforge.raft.RaftMessage.InstallSnapshot;
import io.keyforge.raft.RaftMessage.InstallSnapshotResponse;
import io.keyforge.raft.RaftMessage.VoteRequest;
import io.keyforge.raft.RaftMessage.VoteResponse;

/** Targeted scenarios for the pure core; {@code sim.RaftSimulationTest} covers random interleavings. */
class RaftNodeTest {

    private static final List<String> MEMBERS = List.of("n1", "n2", "n3");

    private final long[] now = {0};
    private final Clock clock = () -> now[0];

    private RaftConfig config(String id) {
        return RaftConfig.of(id, MEMBERS).withTimeouts(150, 300, 50);
    }

    private RaftNode node(String id) {
        return RaftNode.create(config(id), clock, new SplittableRandom(7));
    }

    private RaftNode node(String id, PersistentState state, long applied) {
        return new RaftNode(config(id), clock, new SplittableRandom(7), state, applied);
    }

    private static <T extends RaftAction> List<T> actions(List<RaftAction> actions, Class<T> type) {
        return actions.stream().filter(type::isInstance).map(type::cast).toList();
    }

    private static <T extends RaftMessage> List<T> sent(List<RaftAction> actions, Class<T> type) {
        return actions(actions, Send.class).stream().map(Send::message).filter(type::isInstance).map(type::cast)
                .toList();
    }

    private static LogEntry entry(long index, long term) {
        return LogEntry.command(index, term, ("c" + index).getBytes(StandardCharsets.UTF_8));
    }

    /** Times n1 out and runs its pre-vote and election to completion with n2's votes; returns its last actions. */
    private List<RaftAction> elect(RaftNode candidate, RaftNode voter) {
        now[0] += 400;
        List<RaftAction> preVote = candidate.handle(RaftEvent.TICK);
        VoteRequest preVoteRequest = sent(preVote, VoteRequest.class).stream()
                .filter(request -> request.to().equals(voter.status().nodeId())).findFirst().orElseThrow();
        VoteResponse preVoteResponse = sent(voter.handle(preVoteRequest), VoteResponse.class).getFirst();
        List<RaftAction> election = candidate.handle(preVoteResponse);
        VoteRequest request = sent(election, VoteRequest.class).stream()
                .filter(r -> r.to().equals(voter.status().nodeId())).findFirst().orElseThrow();
        VoteResponse response = sent(voter.handle(request), VoteResponse.class).getFirst();
        return candidate.handle(response);
    }

    @Nested
    class Elections {

        @Test
        void aSingleNodeElectsItselfAndCommitsAtOnce() {
            RaftNode solo = RaftNode.create(RaftConfig.of("n1", List.of("n1")).withTimeouts(150, 300, 50), clock,
                    new SplittableRandom(1));
            now[0] = 400;
            solo.handle(RaftEvent.TICK);
            assertThat(solo.role()).isEqualTo(Role.LEADER);
            List<RaftAction> actions = solo.handle(new RaftEvent.Propose(1, "x".getBytes(StandardCharsets.UTF_8)));
            assertThat(actions(actions, ProposalAccepted.class)).containsExactly(new ProposalAccepted(1, 2, 1));
            assertThat(actions(actions, Apply.class).getFirst().entries()).extracting(LogEntry::index)
                    .containsExactly(2L);
            assertThat(actions(solo.handle(new RaftEvent.ReadIndex(9)), ReadReady.class))
                    .containsExactly(new ReadReady(9, 2));
        }

        @Test
        void preVoteDoesNotTouchTheTermUntilAMajorityAgrees() {
            RaftNode n1 = node("n1");
            now[0] = 400;
            List<RaftAction> actions = n1.handle(RaftEvent.TICK);
            assertThat(n1.role()).isEqualTo(Role.PRE_CANDIDATE);
            assertThat(n1.term()).isZero();
            assertThat(actions(actions, PersistState.class)).isEmpty();
            assertThat(sent(actions, VoteRequest.class)).allSatisfy(request -> {
                assertThat(request.preVote()).isTrue();
                assertThat(request.term()).isEqualTo(1);
            });
            // The voter does not adopt the proposed term either.
            RaftNode n2 = node("n2");
            List<RaftAction> answer = n2.handle(sent(actions, VoteRequest.class).getFirst());
            assertThat(n2.term()).isZero();
            assertThat(actions(answer, PersistState.class)).isEmpty();
            assertThat(sent(answer, VoteResponse.class).getFirst().granted()).isTrue();
        }

        @Test
        void aVoteIsPersistedBeforeTheGrantIsSent() {
            RaftNode n2 = node("n2");
            List<RaftAction> actions = n2.handle(new VoteRequest("n1", "n2", 1, 0, 0, false));
            int persisted = actions.indexOf(new PersistState(1, "n1"));
            int granted = actions.indexOf(new Send(new VoteResponse("n2", "n1", 1, true, false)));
            assertThat(persisted).isNotNegative();
            assertThat(granted).isGreaterThan(persisted);
        }

        @Test
        void onlyOneVotePerTerm() {
            RaftNode n3 = node("n3");
            assertThat(sent(n3.handle(new VoteRequest("n1", "n3", 1, 0, 0, false)), VoteResponse.class).getFirst()
                    .granted()).isTrue();
            assertThat(sent(n3.handle(new VoteRequest("n2", "n3", 1, 0, 0, false)), VoteResponse.class).getFirst()
                    .granted()).isFalse();
            // A retransmission from the same candidate is granted again.
            assertThat(sent(n3.handle(new VoteRequest("n1", "n3", 1, 0, 0, false)), VoteResponse.class).getFirst()
                    .granted()).isTrue();
        }

        @Test
        void aCandidateWithAStaleLogIsRejected() {
            RaftNode n2 = node("n2", new PersistentState(2, null, 0, 0, List.of(entry(1, 1), entry(2, 2))), 0);
            // Lower last term.
            assertThat(sent(n2.handle(new VoteRequest("n1", "n2", 3, 5, 1, false)), VoteResponse.class).getFirst()
                    .granted()).isFalse();
            // Same last term, shorter log.
            assertThat(sent(n2.handle(new VoteRequest("n3", "n2", 3, 1, 2, false)), VoteResponse.class).getFirst()
                    .granted()).isFalse();
            assertThat(n2.term()).as("the term still advanced").isEqualTo(3);
        }

        @Test
        void aFollowerThatHearsFromItsLeaderIgnoresDisruptiveCandidates() {
            RaftNode n2 = node("n2");
            n2.handle(new AppendEntries("n1", "n2", 1, 0, 0, List.of(), 0, 0));
            now[0] += 50;
            VoteResponse preVote = sent(n2.handle(new VoteRequest("n3", "n2", 2, 9, 9, true)), VoteResponse.class)
                    .getFirst();
            assertThat(preVote.granted()).isFalse();
            VoteResponse vote = sent(n2.handle(new VoteRequest("n3", "n2", 5, 9, 9, false)), VoteResponse.class)
                    .getFirst();
            assertThat(vote.granted()).isFalse();
            assertThat(n2.term()).as("leader stickiness keeps the term").isEqualTo(1);
            assertThat(n2.leaderId()).isEqualTo("n1");
        }

        @Test
        void aNewLeaderAppendsANoopAndReplicatesIt() {
            RaftNode n1 = node("n1");
            List<RaftAction> actions = elect(n1, node("n2"));
            assertThat(n1.role()).isEqualTo(Role.LEADER);
            assertThat(actions(actions, AppendLog.class).getFirst().entries())
                    .containsExactly(LogEntry.noop(1, 1));
            assertThat(sent(actions, AppendEntries.class)).hasSize(2)
                    .allSatisfy(append -> assertThat(append.entries()).containsExactly(LogEntry.noop(1, 1)));
        }

        @Test
        void aLeaderCutOffFromTheMajorityStepsDown() {
            RaftNode n1 = node("n1");
            elect(n1, node("n2"));
            long elected = now[0];
            List<RaftAction> actions = List.of();
            while (n1.role() == Role.LEADER && now[0] - elected < 1_000) {
                now[0] += 10;
                actions = n1.handle(RaftEvent.TICK);
            }
            // Within one minimum election timeout without hearing from anyone.
            assertThat(now[0] - elected).isLessThanOrEqualTo(160);
            assertThat(actions(actions, RaftAction.RoleChanged.class))
                    .containsExactly(new RaftAction.RoleChanged(Role.FOLLOWER, 1, null));
        }

        @Test
        void aHigherTermMakesALeaderStepDown() {
            RaftNode n1 = node("n1");
            elect(n1, node("n2"));
            n1.handle(new AppendEntriesResponse("n3", "n1", 7, false, 0, 0, 0));
            assertThat(n1.role()).isEqualTo(Role.FOLLOWER);
            assertThat(n1.term()).isEqualTo(7);
        }
    }

    @Nested
    class Replication {

        @Test
        void aGapIsRejectedWithAHintAtTheEndOfTheLog() {
            RaftNode n2 = node("n2", new PersistentState(1, null, 0, 0, List.of(entry(1, 1))), 0);
            AppendEntriesResponse response = sent(n2.handle(
                    new AppendEntries("n1", "n2", 1, 5, 1, List.of(entry(6, 1)), 0, 0)),
                    AppendEntriesResponse.class).getFirst();
            assertThat(response.success()).isFalse();
            assertThat(response.hintIndex()).isEqualTo(2);
        }

        @Test
        void aMismatchHintsTheStartOfTheConflictingTerm() {
            RaftNode n2 = node("n2", new PersistentState(3, null, 0, 0,
                    List.of(entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 2))), 0);
            AppendEntriesResponse response = sent(n2.handle(
                    new AppendEntries("n1", "n2", 3, 4, 3, List.of(), 0, 0)), AppendEntriesResponse.class).getFirst();
            assertThat(response.success()).isFalse();
            assertThat(response.hintIndex()).isEqualTo(2);
        }

        @Test
        void conflictingEntriesAreTruncatedBeforeTheNewOnesArePersisted() {
            RaftNode n2 = node("n2", new PersistentState(2, null, 0, 0,
                    List.of(entry(1, 1), entry(2, 1), entry(3, 1))), 0);
            List<RaftAction> actions = n2.handle(new AppendEntries("n1", "n2", 3, 1, 1,
                    List.of(entry(2, 1), entry(3, 3)), 1, 0));
            assertThat(actions(actions, TruncateLog.class)).containsExactly(new TruncateLog(3));
            assertThat(actions(actions, AppendLog.class).getFirst().entries()).containsExactly(entry(3, 3));
            assertThat(actions.indexOf(new TruncateLog(3)))
                    .isLessThan(actions.indexOf(actions(actions, AppendLog.class).getFirst()));
            AppendEntriesResponse response = sent(actions, AppendEntriesResponse.class).getFirst();
            assertThat(response.success()).isTrue();
            assertThat(response.matchIndex()).isEqualTo(3);
            assertThat(n2.status().lastLogTerm()).isEqualTo(3);
        }

        @Test
        void aDelayedShorterAppendDoesNotTruncateNewerEntries() {
            RaftNode n2 = node("n2");
            n2.handle(new AppendEntries("n1", "n2", 1, 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0, 0));
            List<RaftAction> late = n2.handle(new AppendEntries("n1", "n2", 1, 0, 0, List.of(entry(1, 1)), 0, 0));
            assertThat(actions(late, TruncateLog.class)).isEmpty();
            assertThat(n2.status().lastLogIndex()).isEqualTo(3);
            assertThat(sent(late, AppendEntriesResponse.class).getFirst().matchIndex()).isEqualTo(1);
        }

        @Test
        void aFollowerCommitsOnlyEntriesVerifiedAgainstTheLeader() {
            // n2 holds stale entries 2..3 from term 1; the leader of term 2 verifies only index 1.
            RaftNode n2 = node("n2", new PersistentState(2, null, 0, 0,
                    List.of(entry(1, 1), entry(2, 1), entry(3, 1))), 0);
            List<RaftAction> actions = n2.handle(new AppendEntries("n1", "n2", 2, 1, 1, List.of(), 10, 0));
            assertThat(n2.status().commitIndex()).isEqualTo(1);
            assertThat(actions(actions, Apply.class).getFirst().entries()).containsExactly(entry(1, 1));
        }

        @Test
        void entriesArePersistedBeforeTheyAreAcknowledged() {
            RaftNode n2 = node("n2");
            List<RaftAction> actions = n2.handle(new AppendEntries("n1", "n2", 1, 0, 0, List.of(entry(1, 1)), 0, 0));
            int persisted = actions.indexOf(actions(actions, AppendLog.class).getFirst());
            int acknowledged = actions.indexOf(actions(actions, Send.class).getFirst());
            assertThat(persisted).isLessThan(acknowledged);
        }

        /**
         * Raft paper, figure 8: an entry from an earlier term that is stored on a majority must not be committed by
         * counting replicas, since a later leader could still overwrite it. It commits only once an entry of the current
         * term is replicated on top of it.
         */
        @Test
        void aLeaderNeverCommitsAnEarlierTermsEntryByCountingReplicas() {
            PersistentState log = new PersistentState(3, null, 0, 0, List.of(entry(1, 1), entry(2, 2)));
            RaftNode n1 = node("n1", log, 1);
            RaftNode n2 = node("n2", log, 1);
            elect(n1, n2);
            assertThat(n1.term()).isEqualTo(4);
            assertThat(n1.status().lastLogIndex()).isEqualTo(3); // the no-op of term 4

            // n2 confirms it holds index 2 (term 2): a majority stores it, yet it must stay uncommitted.
            List<RaftAction> actions = n1.handle(new AppendEntriesResponse("n2", "n1", 4, true, 2, 0, 0));
            assertThat(n1.status().commitIndex()).isEqualTo(1);
            assertThat(actions(actions, Apply.class)).isEmpty();

            // Once the term-4 no-op is on a majority, everything up to it commits.
            actions = n1.handle(new AppendEntriesResponse("n2", "n1", 4, true, 3, 0, 0));
            assertThat(n1.status().commitIndex()).isEqualTo(3);
            assertThat(actions(actions, Apply.class).getFirst().entries()).extracting(LogEntry::index)
                    .containsExactly(2L, 3L);
        }

        @Test
        void aFollowerRejectsProposalsWithALeaderHint() {
            RaftNode n2 = node("n2");
            n2.handle(new AppendEntries("n1", "n2", 1, 0, 0, List.of(), 0, 0));
            assertThat(actions(n2.handle(new RaftEvent.Propose(5, new byte[] {1})), ProposalRejected.class))
                    .containsExactly(new ProposalRejected(5, "n1"));
            assertThat(actions(n2.handle(new RaftEvent.ReadIndex(6)), ReadRejected.class))
                    .containsExactly(new ReadRejected(6, "n1"));
        }

        @Test
        void aLeaderPipelinesBatchesOnceAFollowerKeepsUp() {
            RaftNode n1 = node("n1");
            RaftNode n2 = node("n2");
            elect(n1, n2);
            // Acknowledge the no-op: n2 leaves the probing state.
            n1.handle(new AppendEntriesResponse("n2", "n1", 1, true, 1, 0, 0));
            List<RaftAction> first = n1.handle(new RaftEvent.Propose(1, new byte[] {1}));
            List<RaftAction> second = n1.handle(new RaftEvent.Propose(2, new byte[] {2}));
            assertThat(sent(first, AppendEntries.class)).filteredOn(append -> append.to().equals("n2"))
                    .singleElement().satisfies(append -> assertThat(append.prevLogIndex()).isEqualTo(1));
            assertThat(sent(second, AppendEntries.class)).filteredOn(append -> append.to().equals("n2"))
                    .singleElement().satisfies(append -> assertThat(append.prevLogIndex()).isEqualTo(2));
            // n3 never answered: it is still probed one batch at a time.
            assertThat(sent(second, AppendEntries.class)).noneMatch(append -> append.to().equals("n3"));
        }

        @Test
        void aRestartedNodeResumesFromItsAppliedIndex() {
            PersistentState state = new PersistentState(4, "n2", 0, 0, List.of(entry(1, 1), entry(2, 4)));
            RaftNode n1 = node("n1", state, 1);
            RaftStatus status = n1.status();
            assertThat(status.term()).isEqualTo(4);
            assertThat(status.commitIndex()).isEqualTo(1);
            assertThat(status.lastApplied()).isEqualTo(1);
            assertThat(status.lastLogIndex()).isEqualTo(2);
            assertThatThrownBy(() -> node("n1", state, 3)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class ReadIndex {

        private RaftNode leaderWithCommittedNoop() {
            RaftNode n1 = node("n1");
            elect(n1, node("n2"));
            n1.handle(new AppendEntriesResponse("n2", "n1", 1, true, 1, 0, 0));
            assertThat(n1.status().commitIndex()).isEqualTo(1);
            return n1;
        }

        @Test
        void aReadIsReadyOnlyAfterAMajorityConfirmsLeadership() {
            RaftNode n1 = leaderWithCommittedNoop();
            List<RaftAction> actions = n1.handle(new RaftEvent.ReadIndex(42));
            assertThat(actions(actions, ReadReady.class)).isEmpty();
            List<AppendEntries> round = sent(actions, AppendEntries.class);
            assertThat(round).hasSize(2);
            long roundNumber = round.getFirst().round();
            // An acknowledgement of an older round proves nothing about leadership after the read arrived.
            assertThat(actions(n1.handle(new AppendEntriesResponse("n3", "n1", 1, true, 0, 0, roundNumber - 1)),
                    ReadReady.class)).isEmpty();
            // A rejection still proves n3 follows this leader in this term.
            assertThat(actions(n1.handle(new AppendEntriesResponse("n3", "n1", 1, false, 0, 1, roundNumber)),
                    ReadReady.class)).containsExactly(new ReadReady(42, 1));
        }

        @Test
        void readsWaitForTheNoopOfTheNewTerm() {
            RaftNode n1 = node("n1");
            elect(n1, node("n2"));
            List<RaftAction> actions = n1.handle(new RaftEvent.ReadIndex(1));
            assertThat(actions(actions, ReadReady.class)).isEmpty();
            assertThat(sent(actions, AppendEntries.class)).isEmpty();
            actions = n1.handle(new AppendEntriesResponse("n2", "n1", 1, true, 1, 0, 0));
            assertThat(actions(actions, ReadReady.class)).isEmpty();
            AppendEntries confirmation = sent(actions, AppendEntries.class).getLast();
            assertThat(actions(n1.handle(new AppendEntriesResponse("n2", "n1", 1, true, 1, 0,
                    confirmation.round())), ReadReady.class)).containsExactly(new ReadReady(1, 1));
        }

        @Test
        void pendingReadsFailWhenTheLeaderStepsDown() {
            RaftNode n1 = leaderWithCommittedNoop();
            n1.handle(new RaftEvent.ReadIndex(7));
            List<RaftAction> actions = n1.handle(new AppendEntries("n3", "n1", 2, 0, 0, List.of(), 0, 0));
            assertThat(actions(actions, ReadRejected.class)).containsExactly(new ReadRejected(7, "n3"));
        }
    }

    @Nested
    class Snapshots {

        @Test
        void aSnapshotCompactsTheLogAndLaggingFollowersReceiveIt() {
            RaftNode n1 = node("n1");
            elect(n1, node("n2"));
            n1.handle(new AppendEntriesResponse("n2", "n1", 1, true, 1, 0, 0));
            for (int i = 0; i < 3; i++) {
                List<RaftAction> actions = n1.handle(new RaftEvent.Propose(i, new byte[] {(byte) i}));
                long index = actions(actions, ProposalAccepted.class).getFirst().index();
                n1.handle(new AppendEntriesResponse("n2", "n1", 1, true, index, 0, 0));
            }
            assertThat(n1.status().lastApplied()).isEqualTo(4);
            assertThat(actions(n1.handle(new RaftEvent.SnapshotTaken(4, 1)), CompactLog.class))
                    .containsExactly(new CompactLog(4, 1));
            assertThat(n1.status().snapshotIndex()).isEqualTo(4);

            // n3 has nothing: its rejection sends it the snapshot.
            List<RaftAction> actions = n1.handle(new AppendEntriesResponse("n3", "n1", 1, false, 0, 1, 0));
            assertThat(actions(actions, SendSnapshot.class)).singleElement()
                    .satisfies(send -> assertThat(send.to()).isEqualTo("n3"));

            RaftNode n3 = node("n3");
            SnapshotPayload payload = SnapshotPayload.of(new byte[] {9});
            List<RaftAction> installed = n3.handle(new InstallSnapshot("n1", "n3", 1, 4, 1, 0, payload));
            assertThat(actions(installed, RestoreSnapshot.class)).containsExactly(new RestoreSnapshot(4, 1, payload));
            assertThat(installed.indexOf(actions(installed, RestoreSnapshot.class).getFirst()))
                    .isLessThan(installed.indexOf(actions(installed, Send.class).getFirst()));
            assertThat(n3.status()).extracting(RaftStatus::commitIndex, RaftStatus::lastApplied,
                    RaftStatus::snapshotIndex).containsExactly(4L, 4L, 4L);
            InstallSnapshotResponse response = sent(installed, InstallSnapshotResponse.class).getFirst();
            assertThat(response.matchIndex()).isEqualTo(4);

            // The leader resumes normal replication after the snapshot.
            List<RaftAction> resumed = n1.handle(response);
            assertThat(actions(resumed, SendSnapshot.class)).isEmpty();
        }

        @Test
        void aSnapshotThatIsAlreadyCommittedIsAcknowledgedWithoutRestoring() {
            RaftNode n2 = node("n2");
            n2.handle(new AppendEntries("n1", "n2", 1, 0, 0, List.of(entry(1, 1), entry(2, 1)), 2, 0));
            List<RaftAction> actions = n2.handle(new InstallSnapshot("n1", "n2", 1, 1, 1, 0,
                    SnapshotPayload.of(new byte[0])));
            assertThat(actions(actions, RestoreSnapshot.class)).isEmpty();
            assertThat(sent(actions, InstallSnapshotResponse.class).getFirst().matchIndex()).isEqualTo(2);
        }

        @Test
        void aSnapshotAheadOfTheAppliedIndexIsRefused() {
            RaftNode n2 = node("n2");
            n2.handle(new AppendEntries("n1", "n2", 1, 0, 0, List.of(entry(1, 1), entry(2, 1)), 1, 0));
            assertThatThrownBy(() -> n2.handle(new RaftEvent.SnapshotTaken(2, 1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void messagesForOtherNodesOrFromStrangersAreIgnored() {
        RaftNode n1 = node("n1");
        List<RaftAction> actions = new ArrayList<>();
        actions.addAll(n1.handle(new VoteRequest("n2", "n9", 5, 0, 0, false)));
        actions.addAll(n1.handle(new VoteRequest("x", "n1", 5, 0, 0, false)));
        assertThat(actions).isEmpty();
        assertThat(n1.term()).isZero();
    }
}
