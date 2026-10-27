# ADR 0004: Flush once per read, and stop reading while the client is not reading

- Status: accepted
- Date: 2026-10-27
- Phase: 1

## Context

Pipelining clients send many requests before reading any reply: `redis-benchmark -P 16` sends 16 at a time, and a bulk
loader may send millions. Two things can go wrong:

- **Syscall per reply.** Flushing after every reply turns a 16-request pipeline into 16 `write` calls and wastes most
  of the benefit of pipelining.
- **Unbounded memory.** A client that writes requests but never reads replies makes the server queue replies without
  limit, until it runs out of memory.

## Decision

**Write per request, flush per read.** `CommandChannelHandler` writes each reply as its request completes, but flushes
only in `channelReadComplete`, once per batch of bytes read from the socket. Replies keep request order because a
connection's requests run sequentially on its event loop.

**Backpressure through channel writability.** Each connection has a write-buffer water mark of 32 KiB (low) and
64 KiB (high), Netty's defaults, stated explicitly. When queued replies exceed the high mark, the channel becomes
unwritable and the handler sets `autoRead = false`: the server stops reading that socket. When the client drains
replies below the low mark, `channelWritabilityChanged` turns `autoRead` back on. While the server is not reading, TCP
flow control pushes back on the client, whose writes eventually block.

**Final replies close cleanly.** After `QUIT` or a protocol error, the handler stops reading, ignores any requests
already decoded, writes the final reply, flushes, and closes once the flush completes.

## Consequences

- One flush per read covers a whole pipelined batch.
- Server memory per connection is bounded by the high water mark plus what one read cycle decodes. A read cycle is up
  to 16 reads of the adaptive receive buffer, and the replies to those requests are produced even when the channel
  turns unwritable partway through the batch.
- Two tests cover this. An `EmbeddedChannel` test checks that reading pauses above the high mark and resumes after a
  flush. An integration test pipelines ~6 MB of requests from a client that only starts reading later, and checks that
  every reply arrives (no deadlock).
- `MessageToByteEncoder` allocates one pooled buffer per reply. Encoding a whole batch into one buffer would save
  allocations; that is left for profiling (Phase 6) rather than done speculatively.

## Alternatives considered

- **Flush after every reply.** Simple, but it costs a syscall per command under pipelining.
- **`FlushConsolidationHandler`.** It batches flushes generically. Our handler already knows where batch boundaries are
  (`channelReadComplete`), so it adds nothing.
- **Unbounded outbound queue (no backpressure).** A single misbehaving client could exhaust the heap.
- **Closing slow consumers** (Redis' `client-output-buffer-limit`). It is complementary, not a replacement, and can be
  added later for connections that stay unwritable too long.
