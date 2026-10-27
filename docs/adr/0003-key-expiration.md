# ADR 0003: Absolute expiry instants, lazy expiry and sampled active expiry

- Status: accepted
- Date: 2026-10-27
- Phase: 1

## Context

`SET ... EX/PX` gives keys a time to live. An expired key must be invisible at once, and its memory must eventually be
reclaimed even if nobody reads it again. Later phases constrain the representation: the LSM engine stores expiry with
the value as absolute epoch milliseconds (Phase 3), and under Raft the leader puts the absolute instant into the log
entry, so followers never compute expiry from their own clocks (Phase 4).

## Decision

**Relative TTLs become absolute instants at the command layer.** `SET` reads the injected `InstantSource` once and
passes `now + ttl` to the store. The store's API only accepts absolute instants (`KeyValueStore.NO_EXPIRY` = 0 means
none). The conversion checks for overflow the way Redis does (`EX` over `Long.MAX_VALUE / 1000`, or `now + ttl`
overflowing, gives `invalid expire time in 'set' command`).

**A key with `expireAt <= now` is logically absent.** `GET`, `MGET`, `EXISTS`, `DEL` counts, `SET NX/XX` conditions
and `INCR` all treat it as missing.

**Lazy expiry.** Every access path checks the entry it finds; an expired entry is removed on the spot, under the
segment lock it already holds.

**Active expiry, adapted from Redis' `activeExpireCycle`.** A background task (default every 100 ms; 0 disables it)
visits segments round-robin, resuming where the previous cycle stopped:

1. Lock the segment, sample 20 random entries among those that carry an expiry, delete the expired ones, unlock.
2. If more than 10% of the sample had expired, sample the same segment again; otherwise move on.
3. Stop after one full pass, or when 25% of the interval has been spent.

Locks are released between sampling rounds, so foreground commands interleave with the cycle.

**O(1) sampling.** Each segment keeps the entries that carry an expiry in a dense array, and each entry stores its slot
in that array. Adding, removing and sampling a random entry are all O(1); removal swaps the last element into the freed
slot. The array shrinks when it drops below a quarter full.

**Clock injection.** The store and the commands take a `java.time.InstantSource`. Tests use a manual clock and disable
the background task, so expiry tests are deterministic.

## Consequences

- Keys that are written once and never read are reclaimed within a few cycles. An integration test checks that 500
  never-read keys disappear.
- Sampling is probabilistic: a segment can keep up to about 10% stale keys between cycles, the same trade-off Redis
  makes. Like Redis, `DBSIZE` may count keys that have expired but have not been reclaimed yet.
- Expiry follows the wall clock, so a clock jump moves expirations, as in Redis.
- Phase 4 caveat: replicas must not each delete keys based on their own clock and sampling, or their state machines
  would diverge. Reads can hide expired keys locally, but physical deletion will have to be decided deterministically,
  for example by the leader proposing deletions or by applying expiry at log-entry timestamps. This is an open item for
  that phase.

## Alternatives considered

- **Priority queue, timer wheel or expiry-sorted index.** Exact, but costs O(log n) or extra bookkeeping on every write
  with a TTL, plus memory per entry, to reclaim keys that are already invisible. Sampling costs nothing on the write
  path.
- **Lazy expiry only.** Simplest, but write-once keys with a TTL (sessions, caches) would leak memory forever.
- **Relative TTLs in the store.** Each replica, and each restart, would compute a different expiry instant.
