# ADR 0014: gRPC transport with a bidirectional stream per node pair, and forwarding to the leader

- Status: accepted
- Date: 2026-10-28
- Phase: 4

## Context

The Raft core sends messages through an injected `Transport`; production uses gRPC, with bidirectional streaming for
AppendEntries. Clients such as `redis-benchmark` should work against any node, so a follower must forward client
commands to the leader. The servers already use Netty 4.2; gRPC Java's Netty transport is built on Netty 4.1.

## Decision

**Wire format** (`keyforge-raft/src/main/proto/keyforge/raft/v1/raft.proto`), one service, `RaftTransport`:

- `Exchange(stream Envelope) returns (stream Envelope)`. Long-lived bidirectional streams between each pair of nodes
  carry every Raft message: AppendEntries, heartbeats, votes and their answers. An `Envelope` mirrors `RaftMessage` one
  to one. A node sends on the stream its peer opened to it when there is one, writing on its response side, and
  otherwise opens its own. With a single stream between two nodes, AppendEntries and their responses flow on the two
  halves of one stream. A message that finds no usable stream is dropped and Raft retransmits. A broken stream is
  reopened on the next send, at most every 100 ms. Keepalives every 5 s on both sides detect dead peers.
- `InstallSnapshot(stream SnapshotChunk) returns (SnapshotTransferred)`. The leader streams the snapshot file in 1 MiB
  chunks, with gRPC flow control, on its own call, so a large snapshot never delays heartbeats. The receiver spools it
  to a file and hands Raft an InstallSnapshot whose payload is that file; the answer travels on the Exchange stream. One
  transfer per follower runs at a time.
- `Forward(ForwardRequest) returns (ForwardResponse)`. Payloads are opaque to the transport.

**Dependencies and build.** `grpc-netty-shaded` keeps gRPC's own Netty 4.1 apart from the servers' Netty 4.2;
`grpc-protobuf`, `grpc-stub` and `protobuf-java` 4.36.2, all at gRPC 1.85.0. Code generation uses the build plugin
`io.github.ascopes:protobuf-maven-plugin` 5.1.11, which fetches `protoc` and `protoc-gen-grpc-java` from Maven Central for
the build platform. It is build tooling, not a runtime library. gRPC callbacks run on virtual threads, so handlers may
block.

**Forwarding, at the command level.** In a cluster the data commands use `ReplicatedKeyspace`. On a node that is not
the leader it raises a `RedirectException`, before anything is executed. The dispatcher then hands the *original RESP
request* to `LeaderForwarder`, which sends it with `Forward` to the leader it knows of. The leader runs it with
`dispatchLocally`, which never forwards again, and returns the RESP reply, which the follower relays unchanged.
Forwarding the request rather than a half-executed command means the leader parses it and computes TTL instants with its
own clock (ADR 0011). If the receiver has lost leadership, it answers with the leader it now knows of and the sender
retries there. While no leader is known it waits, polling every 20 ms, up to `raft.request-timeout-ms` (5 s), then
answers `CLUSTERDOWN no Raft leader is available`. `UNAVAILABLE` (the leader is gone) is retried the same way; any other
failure is reported as an error with an unknown outcome.

**Why the peer's stream comes first.** The first version sent requests on its own stream and answers on the peer's.
The Docker failover test then showed a restarted node that never heard from the new leader. While the node was dead,
Docker's DNS had dropped its name. gRPC backed off name resolution and reconnection exponentially, and kept buffering the
leader's messages in a call waiting for a connection instead of failing it. The restarted node, meanwhile, had opened
its own stream to the leader for its pre-votes. That stream proves the peer is up and reachable now, so it is
preferred. The transport also resets gRPC's connection backoff, at most every 100 ms, when it needs a channel in
`TRANSIENT_FAILURE`. The Docker setup caches DNS answers for 2 s, since a restarted container may change address.

**Commands block until committed.** A write returns after its entry is committed and applied on the leader, a read
after its ReadIndex confirmation (ADR 0010). With `server.type=virtual-threads` only the connection's virtual thread
waits. With Netty the event loop waits, and every connection on that loop with it. Both work; the Docker cluster uses
virtual threads.

**`INFO raft`** reports role, term, leader, commit, applied and snapshot indexes and members. Scripts use it to find the
leader.

## Consequences

- One stream per direction per node pair carries the steady-state traffic. gRPC preserves order on a stream, which is
  what makes AppendEntries pipelining (ADR 0009) effective. `RaftClusterIT` runs three runtimes over real gRPC through
  election, replication, ReadIndex, forwarding, a leader crash, compaction and InstallSnapshot to a restarted node.
- Clients need no cluster awareness: `redis-cli`, Jedis and `redis-benchmark` work against any node. The cost is an
  extra network hop for every command sent to a follower.
- Retrying a forwarded write after `UNAVAILABLE` can apply it twice if the old leader committed it just before dying.
  That is harmless for `SET`, `MSET` and `DEL`, and a double count for `INCR`/`DECR`. Exactly-once semantics need client
  sessions with request ids deduplicated by the state machine; that is an open issue for Phase 5's linearizability
  checks.
- Pipelined commands from one connection still run one after another, each waiting for its commit, so pipelining
  (`redis-benchmark -P`) gains little in a cluster. Batching a connection's pipelined writes into one proposal is a
  possible optimisation.
- Netty event loops block on Raft round trips in cluster mode (ADR 0006 already listed asynchronous command execution
  as an open issue).

## Alternatives considered

- **Unary RPCs for every message** (AppendEntries, RequestVote as request/response calls). This is simpler to read, but
  each call has its own HTTP/2 stream, and nothing orders one call's arrival relative to another, so pipelining would
  degrade to probing.
- **Plain Netty or raw TCP with a hand-written framing.** It has fewer dependencies, but the specification names gRPC,
  and gRPC brings flow control, keepalives and deadlines.
- **gRPC with hand-written marshallers instead of Protobuf.** It needs no code generation, but gives up a schema that
  other tools (Phase 5's chaos tooling) can read.
- **Redis-Cluster-style `MOVED` redirects.** Plain clients and `redis-benchmark` without `--cluster` would break.
- **Forwarding the decoded `KvCommand`.** The follower would compute TTL instants with its own clock, which ADR 0011
  forbids.
