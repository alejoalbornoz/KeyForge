# ADR 0001: A transport-agnostic, incremental RESP2 codec

- Status: accepted
- Date: 2026-10-27
- Phase: 1

## Context

KeyForge must speak RESP2 well enough for stock `redis-cli`, `redis-benchmark` and client libraries. Two server
implementations will share the codec: Netty (Phase 1) and blocking I/O on virtual threads (Phase 2). The codec runs on
the hot path of every request, and it reads bytes from untrusted clients.

Requirements that shape the design:

- TCP delivers a byte stream, not messages. A request can be split across reads at any byte, and one read can carry
  many pipelined requests (`redis-benchmark -P 16`).
- Clients send arrays of bulk strings; humans on telnet send inline commands (`SET k v\r\n`).
- A hostile client can announce a 2 GB bulk string or a billion-element array in a 15-byte header.

## Decision

**No I/O dependency.** `keyforge-protocol` depends on nothing. Decoders read from a `java.nio.ByteBuffer`: they read
with absolute indexes and signal consumption by advancing the position. The encoder writes into a small `RespSink`
interface (`writeByte`, `writeBytes`). The Netty adapter wraps a `ByteBuf`; the Phase 2 server will wrap a socket
buffer.

**Two decoders, one per direction.**

- `RespRequestDecoder` (server side) returns each request as a `byte[][]` of its arguments. It accepts only what clients
  send (arrays of bulk strings and inline commands) and never builds `RespValue` objects, so a `GET` costs one
  `byte[][]` plus one `byte[]` per argument.
- `RespDecoder` (client side) decodes any RESP2 value, including nested arrays, into `RespValue` records. Tests use it
  to check the encoder, and the Phase 2/5 load generators will use it to read replies.

**Stateful and incremental.** `RespRequestDecoder` remembers how many arguments it expects, the arguments decoded so
far and the length of a pending bulk string. When more bytes arrive it resumes where it stopped and never rescans an
argument it already consumed. A bulk payload is consumed only once it has fully arrived, so memory follows the bytes
actually received: a header announcing 512 MB allocates nothing until 512 MB show up. Argument arrays are preallocated
up to 1024 slots and grow as arguments arrive. `RespDecoder` keeps a stack of partially filled arrays for the same
reason.

**Redis' limits and wording.** 512 MB per bulk string (Redis' `proto-max-bulk-len`), 64 KB for a line with no
terminator yet (`PROTO_INLINE_MAX_SIZE`), and 1 Mi arguments per request (configurable). Violations raise
`RespProtocolException` carrying Redis' message (`invalid multibulk length`, `expected '$', got ':'`,
`unbalanced quotes in request`, ...). The server answers every request that preceded the bad bytes, replies
`-ERR Protocol error: <message>`, and closes the connection, as Redis does.

**Stricter framing than Redis.** Redis skips the two bytes after a bulk payload without looking at them, and assumes
the byte after `\r` in a header is `\n`. KeyForge checks both. A client that gets framing wrong then gets a protocol
error instead of a silently desynchronised stream.

**One integer grammar.** Headers, command arguments (`EX 10`) and values read by `INCR` all follow Redis'
`string2ll`: optional `-`, no leading zeros, no `+`, no whitespace, within 64 bits (`RespNumbers`).

**Inline commands** follow `sdssplitargs`: whitespace-separated, with `"..."` (escapes `\n \r \t \b \a \\ \" \xHH`)
and `'...'` (escape `\'`), where a closing quote must be followed by whitespace.

**Allocation-free encoding** of ASCII simple strings, errors, integers and bulk strings. `encodedLength` computes the
exact size of a reply, so the Netty encoder allocates the output buffer once at the right size.

**Error messages cannot break framing.** `RespError` replaces CR and LF with spaces, like Redis' `addReplyErrorFormat`,
because errors often echo client input (`unknown command '<name>'`).

## Consequences

- Both servers share one tested codec. Property tests (jqwik) check encode/decode round trips for arbitrary values
  and decoding under arbitrary fragmentation of the byte stream.
- The server never decodes replies and the client never decodes inline commands, so each decoder stays small.
- Computing `encodedLength` walks a reply twice. That is cheap for the replies we produce, and it saves buffer
  reallocation for large values.
- Differences from Redis: a NUL byte in an inline command is an ordinary character (C stops at it), and the extra
  CRLF checks reject malformed streams that Redis would tolerate.

## Alternatives considered

- **Stateless, restartable parser** (try to parse a whole frame; if incomplete, consume nothing and retry later).
  Simpler, but every new read rescans the frame from its start. That is quadratic for large requests that arrive
  slowly, and it needs the whole frame buffered before any of it is consumed.
- **Netty's `netty-codec-redis`.** It ties the protocol to Netty (the virtual-thread server could not reuse it), emits
  reference-counted message objects per argument, and has no inline-command support that matches Redis'.
- **A single generic decoder for both directions.** It would allocate a `RespBulkString` wrapper per argument and a
  `RespArray` per request on the server's hot path, for no benefit.
