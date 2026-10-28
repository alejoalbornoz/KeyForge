# ADR 0011: Leader-assigned expiry, log time, and expiry decisions every replica agrees on

- Status: accepted; closes the open item of ADR 0003 and the expiry caveat of ADR 0006
- Date: 2026-10-28
- Phase: 4

## Context

Replicated state machines must be deterministic: every replica applies the same entries and must reach the same state.
Time-based expiry threatens that in three places.

1. A relative TTL (`SET k v EX 10`) converted on each replica with its own clock gives a different instant everywhere.
2. Writes whose effect depends on whether a key has expired (`SET NX/XX`, `DEL` counts, `INCR` on an expired key) give
   different answers if each replica decides "now" with its own clock, and different answers mean diverging states.
3. Physical removal of expired data: the in-memory engine's lazy and active expiry (ADR 0003), and the LSM engine's
   flushes and compactions, which drop expired values using the local clock (ADR 0006). If a follower whose clock runs
   ahead drops a value that the leader still considers live, a later `SET XX` succeeds on the leader and fails on the
   follower.

The specification requires that the leader assign absolute expiry timestamps inside the log entry, and that followers
never consult their own clock to compute expiry.

## Decision

**The leader converts TTLs.** Followers forward the client's command unexecuted (ADR 0014). The leader's command layer
turns `EX`/`PX` into an absolute epoch-millisecond instant with its clock, as in standalone mode (ADR 0003). The instant
goes into the log entry (`KvCommand.Set.expireAtMillis`).

**Every entry carries the leader's wall-clock time**, stamped when the leader proposes it (`KvCommand.time`).

**Log time.** The state machine applies entry *i* at log time `T_i = max(T_{i-1}, time_i)`. It is a function of the
log alone, so every replica computes the same value, and it never goes backwards, even after an election to a leader
whose clock lags. It is persisted with the store's applied position and in snapshots, so a restart resumes the same
sequence.

**Writes decide expiry at log time.** Each replicated write carries its log position `(index, T_i)`. The LSM store
evaluates `NX`/`XX`, delete counts and read-modify-writes at `T_i`, not with its clock (`LsmKeyValueStore.put(position,
...)` and its siblings).

**Physical removal at log time.** In replicated mode, flushes drop only values expired at the flushed memtable's log
time, and compactions only values expired at the latest applied log time. Because log time is monotonic, a value
expired at `T_i` is expired for every later entry on every replica. Removing it changes no future decision, so physical
layouts may differ between replicas but logical states never do. The in-memory engine, whose lazy and active expiry
follow the wall clock, is rejected in cluster mode (`store.engine` must be `lsm`).

**Reads hide what has expired at `max(wall clock, latest log time)`.** A `GET` after a `PX 100` key's deadline returns
nothing even if no write has happened since, which is Redis' behaviour. The maximum ensures that a value already
dropped at log time is never shown as live by a node whose clock lags the log.

## Consequences

- No replica ever computes expiry from its own clock for anything that changes state. `KeyValueStateMachineTest` applies
  the same log on two stores whose clocks differ by hours and gets identical results and contents. `LsmReplicatedModeTest`
  checks that a clock far ahead neither expires a key early nor makes a compaction drop it.
- Expiry follows the leader's wall clock. After a failover to a leader whose clock lags, log time holds until the new
  leader's clock catches up. Keys expire no earlier than before, possibly a little later, which is safe. A leader whose
  clock is far ahead advances log time for everyone. This is the usual dependence of TTLs on reasonably synchronised
  clocks, now confined to one clock at a time.
- Reads hide expired keys by the reading leader's wall clock. Followers forward reads (ADR 0010), so the leader's clock
  decides, consistently with how it stamped the write. Expiry precision is a wall-clock notion and is not part of the
  linearizability argument.
- `DBSIZE` may count keys that expired since the last flush or compaction, as the `KeyValueStore` contract already
  allows.

## Alternatives considered

- **The leader proposes deletions** (Redis' replicas receive `DEL` for expired keys from the master). It is
  deterministic, but every expiration costs a log entry and an fsync on a majority, and the leader has to sample keys
  and replicate the result. Log time gives the same safety for free.
- **Replicas expire independently with their own clocks** (Redis before 3.2 for reads, ADR 0003's active expiry). This
  is the divergence described above.
- **Store relative TTLs and compute the instant at apply time from the entry's time.** It is equivalent in effect, but
  the specification puts the absolute instant in the entry, and doing it on the leader keeps the command layer
  identical in both modes.
- **Use the log time for reads as well.** Reads would be fully determined by the log, but with no writes, log time
  stands still and keys would never appear to expire.
