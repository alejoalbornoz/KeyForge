# Architecture Decision Records

One file per decision: context, decision, consequences, alternatives considered.

| ADR | Decision | Phase |
| --- | --- | --- |
| [0001](0001-resp-codec.md) | A transport-agnostic, incremental RESP2 codec | 1 |
| [0002](0002-threading-model-and-in-memory-store.md) | Run commands on I/O threads against a segmented, lock-based store | 1 |
| [0003](0003-key-expiration.md) | Absolute expiry instants, lazy expiry and sampled active expiry | 1 |
| [0004](0004-flushing-and-backpressure.md) | Flush once per read, and stop reading while the client is not reading | 1 |
