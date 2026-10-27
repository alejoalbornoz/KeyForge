# ADR 0005: Serve each connection on its own virtual thread with blocking I/O; rely on JEP 491 for monitors

- Status: accepted
- Date: 2026-10-27
- Phase: 2

## Context

Phase 2 adds a second network front end next to Netty: a classic blocking server with one virtual thread per
connection, written with `ServerSocket`, `InputStream` and `OutputStream`. Both servers must share the codec and the
command dispatcher, behave identically (they run the same integration suite), and be compared in a benchmark
(`docs/benchmarks/netty-vs-virtual-threads.md`).

A virtual thread runs mounted on a carrier, a platform thread of a small `ForkJoinPool` with one carrier per core by
default. When a virtual thread blocks on a socket, a lock or a sleep, it unmounts: its stack is saved to the heap and the
carrier runs another virtual thread. A virtual thread that cannot unmount while it blocks is **pinned**, and blocks its
carrier too. With one carrier per core, a handful of pinned threads stalls every connection, and if the thread that
would release them needs a carrier, the server deadlocks.

**JDK 21 to 23.** A virtual thread was pinned whenever it blocked inside `synchronized` (including `Object.wait()`) or
with a native frame on its stack. The common advice was to replace `synchronized` with `ReentrantLock` around anything
that blocks, and to find offenders with `-Djdk.tracePinnedThreads` or the JFR event `jdk.VirtualThreadPinned`.

**JDK 24, JEP 491 ("Synchronize Virtual Threads without Pinning").** The JVM now tracks monitor ownership per virtual
thread instead of per carrier. A virtual thread unmounts when it blocks inside `synchronized`, when it waits to enter a
contended monitor, and in `Object.wait()`. What still pins:

- blocking with a native frame on the stack: a JNI or Foreign Function call that calls back into Java code that blocks;
- blocking while loading a class, or inside a class initializer (`<clinit>`), which also runs under a native frame;
- waiting for another thread to finish initializing a class.

JEP 491 removed `jdk.tracePinnedThreads`; setting it has no effect. It kept `jdk.VirtualThreadPinned` for the remaining
cases and added the reason for the pinning and the carrier thread to the event. It also says that code already migrated
to `ReentrantLock` need not be reverted, and that new migrations are no longer necessary.

We checked this on the development machine with the same probe on both JDKs: one virtual thread sleeps inside a
`synchronized` block, another sleeps inside a static initializer, while a JFR `RecordingStream` listens for
`jdk.VirtualThreadPinned`. On JDK 21.0.11 both are reported pinned. On JDK 25.0.5 only the class initializer is, with
`pinnedReason = "VM call to ...<clinit> on stack"`, `blockingOperation = "LockSupport.park"` and the carrier's name.

KeyForge requires Java 25, so `synchronized` no longer pins it. Holding a lock across blocking I/O remains a mistake
anyway: a lock held during a socket write is held for as long as the slowest client takes to read.

## Decision

**`server.type` selects the front end.** `netty` (the default) or `virtual-threads`. `KeyForgeNode` builds one or the
other behind the `KeyForgeServer` interface. `server.io-threads` applies to Netty only.

**`VirtualThreadServer`: one platform accept thread, one virtual thread per connection.**

- The accept loop runs on one non-daemon platform thread, `keyforge-accept`, with the same backlog as Netty (1024).
  Being non-daemon, it keeps the JVM alive, as Netty's event loops do; virtual threads are always daemon threads.
- Each accepted socket (`TCP_NODELAY`, `SO_KEEPALIVE`, like Netty) runs on a virtual thread named `keyforge-conn-N`,
  created through `Executors.newThreadPerTaskExecutor(Thread.ofVirtual()...factory())`. The executor tracks the threads,
  so `close()` can close every open socket and then wait (up to 5 s) for the threads to finish.
- Every thread the server creates puts `nodeId` in its MDC before running.

**The connection loop is sequential blocking code** (`Connection`), with the same semantics as the Netty handler
(ADR 0004):

- Read once from the socket into a `ReceiveBuffer`, decode every complete request with `RespRequestDecoder`, run each
  on the same virtual thread through `CommandDispatcher`, and encode the replies into one `ByteArrayRespSink`.
- Write the replies once per read, like Netty's flush in `channelReadComplete`. If a batch's replies pass 32 KiB, write
  them in the middle of the batch, so the reply buffer stays bounded.
- Backpressure needs no code. While the client does not read, `write` blocks, and a thread blocked in `write` reads no
  more requests; TCP flow control then pushes back on the client.
- After `QUIT` or a protocol error, ignore the remaining input, write the final reply, and close the socket.
- The receive buffer starts at 4 KiB, because a parked virtual thread keeps its buffer while the client is idle. It
  grows only when one partial request fills it (the decoder consumes a bulk payload only once all of it has arrived), and
  drops back to 4 KiB once drained.

**No lock is held across blocking I/O.** The session, decoder and buffers belong to one thread and need no locks. The
only locks a request takes are the store's segment `ReentrantLock`s, held around in-memory map operations only
(ADR 0002). The rule for new code: never hold any lock, `synchronized` or `java.util.concurrent`, across a blocking
call. We do not ban `synchronized`: on Java 25 it is fine for short critical sections, and the store keeps
`ReentrantLock` because multi-key commands need a variable number of locks, not because of pinning.

**A test guards against pinning.** `VirtualThreadServerIT.servingRequestsNeverPinsAVirtualThread` streams
`jdk.VirtualThreadPinned` events (threshold 0, with stack traces) while 16 Jedis clients run single-key, multi-key,
pipelined and 256 KiB commands, and fails on any event other than its control: the same recording must see a virtual
thread that parks inside a static initializer, so finding nothing else means "no pinning" rather than "JFR saw
nothing". One client warms up every code path before recording starts, because waiting for another thread's class
initialization still pins, and that one-off startup effect is not what the test is about.

## Consequences

- The connection logic is about 50 lines of straight-line code: no pipeline, no writability callbacks, no `autoRead`
  toggling. A stack trace of a stuck connection shows exactly where it is.
- Platform threads do not grow with connections: carriers (one per core), the JDK's I/O poller threads and the accept
  thread. `VirtualThreadServerIT` asserts that 1,000 open connections add fewer than 64 platform threads.
- An idle connection costs more memory than on Netty. It keeps its 4 KiB receive buffer, a reply buffer, a decoder and
  the parked virtual thread's stack chunk, whereas an idle Netty channel holds no read buffer because pooled buffers go
  back to the pool after each read. The benchmark measures the difference at 1k, 10k and 50k connections.
- Each read wakes the connection through the JDK's poller (epoll on Linux), which unparks the virtual thread onto any
  carrier. Netty instead keeps a connection on one event loop that does its own `epoll_wait`, reads and writes. Which
  costs more under load is a question for the benchmark, not for this ADR.
- Pinning remains possible through JNI or FFM callbacks and class initialization. KeyForge has neither on the request
  path today, and the test above catches regressions.
- For Phase 3: blocking *file* I/O does not unmount a virtual thread either. The JDK compensates by temporarily adding
  carriers. The WAL's `fsync` path needs that in mind (for example a dedicated platform thread for group commit) rather
  than an `fsync` per request on connection threads.

## Alternatives considered

- **A platform thread per connection.** Same code, but 50k connections would mean 50k OS threads, each with its own
  reserved stack, plus scheduler and context-switch costs and per-process thread limits. Virtual threads keep the
  programming model and remove the cost.
- **Selectors plus a virtual thread per request.** A hand-off queue on every request, and the selector half is what
  Netty already does.
- **Accept on a virtual thread.** It works, but nothing would keep the JVM alive, because virtual threads are daemon
  threads. One platform thread is the simpler fix.
- **Starting threads directly with `Thread.ofVirtual().start()`.** That would be one less map entry per connection,
  but `close()` would have no way to wait for the connection threads to finish.
- **Replacing every `synchronized` with `ReentrantLock` defensively, as JDK 21 advice had it.** Unnecessary on Java 25
  (JEP 491), and the request path holds no monitor across I/O anyway.
- **`-Djdk.tracePinnedThreads` to detect pinning.** Removed in JDK 24. The JFR event works on every JDK we support and
  can be asserted in a test.
- **`BufferedInputStream` and `BufferedOutputStream`.** One more copy and one more layer. `ReceiveBuffer` and
  `ByteArrayRespSink` already give the batching we want: one write per read.
