# ADR 0017: Forwarded commands are retried only when they certainly had no effect

- Status: accepted; amends ADR 0014
- Date: 2026-11-02
- Phase: 4 (a fix found by Phase 5)

## Context

A follower forwards data commands to the leader (ADR 0014). When the forward failed with gRPC `UNAVAILABLE`, the
follower waited for the next leader and sent the command again. `UNAVAILABLE` does not mean "not delivered", though. The
connection can break after the request reached the leader, and the leader may have committed it before it died. The
Phase 5 linearizability test caught exactly that. The nemesis killed the leader while three followers had `INCR`s of one
key forwarded to it; one increment was applied twice. The key went from 2,043,000 to 2,043,002, and no client ever saw
2,043,001 (`NEMESIS=all`, ADR 0015).

The forwarder also answered `CLUSTERDOWN` when it found no leader before the request timeout, including after such a
retry. Clients, and the Phase 5 checker, may read `CLUSTERDOWN` as a definite failure, but a write that answered
`CLUSTERDOWN` could still have been committed by an earlier attempt.

ADR 0014 named two remedies: client sessions with request ids that the state machine deduplicates, or no retry once a
request may have reached the leader. The user chose the second for now.

## Decision

**A forwarded command is retried only if it certainly had no effect:**

- the receiver answered `NotLeader`: it rejected the command before executing anything; or
- the request was never sent: `GrpcTransport.forward` sends only on a connection in state `READY`, and fails at once
  with `ForwardNotSentException` otherwise. Sending only on an established connection is what makes that failure
  unambiguous.

**Any failure after the request was sent** (`UNAVAILABLE` from a broken connection, `DEADLINE_EXCEEDED`, the request
timeout) ends the command with `ERR forwarding to the leader <node> failed (<code>); the command may or may not have
been applied`. That is what a client connected to the leader itself would see if the leader died mid-command: a broken
connection whose outcome it cannot know.

**`CLUSTERDOWN` only when nothing was sent**: when every attempt within the request timeout was a `NotLeader` answer or
a request that was never sent.

## Consequences

- No retry of the forwarder can apply a command twice. `LeaderForwarderTest` checks the rules one by one with a
  scripted transport. `GrpcTransportTest` checks the transport side: a forward without a connection is reported as not
  sent, and a forward cut off while the leader executes it is not.
- During a failover, clients of followers see more errors: commands in flight to the dying leader end with an
  unknown-outcome error instead of being retried, as they would on a direct connection to it. Commands sent after the
  leader is gone are still retried and usually succeed on the next leader.
- Reads get the same treatment although retrying them would be harmless. Marking read-only commands as retryable is a
  possible refinement.
- Exactly-once semantics for retries done by clients themselves still need client sessions and deduplication in the
  state machine. A client that retries an `INCR` after an unknown outcome can still double it, as with Redis.

## Alternatives considered

- **Client sessions with request ids, deduplicated by the replicated state machine.** It gives exactly-once retries,
  invisible to clients. But the deduplication table is replicated state: it must be written atomically with each
  command's effect, survive restarts in the LSM store and travel in snapshots, and it needs an expiry policy. It is a
  bigger change, worth doing if failover errors turn out to matter.
- **Classifying failures by gRPC status or message text.** `UNAVAILABLE` covers both "never connected" and "connection
  lost mid-call", and descriptions are not an API.
- **Detecting "not sent" with a gRPC stream tracer** (`ClientStreamTracer.streamCreated`). It is more precise than the
  connection-state check, but relies on experimental API and on when exactly gRPC creates streams. The state check
  errs only on the safe side: a connection that breaks just after the check counts as "maybe sent".
