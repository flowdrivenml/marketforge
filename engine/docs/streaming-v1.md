# MarketForge streaming v1

## Public entry points

`live::transport::unix::UnixStreamingServer::bind(Arc<StreamRouter>, PublicationConfig)`
returns an owned server; `shutdown().await` joins its connections and removes only
its own socket inode. Bind it to the application's existing Router, after loading
`GeneralConfig` through the existing loader. The transport never starts, creates,
or reconfigures streams. The headless `marketforge-live` executable now uses the owned
[LiveApplication](live-application-v1.md), which drives demand and lifecycle services.
The streaming protocol itself remains read-only.

`client::streaming::MarketForgeStreamClient` requires `stream-client`, not `live`
or `postgres`. `connect_unix(path, ClientOptions)` negotiates v1; `list_streams`,
`describe_stream`, `subscribe`, `ping`, and `close` are asynchronous. A Subscription
has cancellation-safe `recv()` and deterministic `close()`. Clone the client for
concurrent requests. All subscriptions share one connection and its writer.

Run the consumer against an application embedding the server:

```sh
cargo run --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features stream-client --example stream-consumer -- /private/path/data.sock stream_id
```

`stream_transport` is a runnable integration target exercising the production
server and client over local sockets with actual Router, SourceProcessor,
production Merge, and production Convert. Exchange input is normalized test data.
No historical dataset is read.

## Framing and schemas

Each frame is a four-byte **unsigned big-endian** length followed by exactly that
many bytes of CBOR (RFC 8949, ciborium 0.2.2). Length excludes the prefix. There is
no compression or fragmentation. One market publication is one complete frame;
oversize publications terminate that subscription, without truncating records.

CBOR structs are maps with the exact snake_case field names in
`src/stream_protocol/{message,market,integrity}.rs`. Enum discriminators are textual,
not Rust numeric discriminants. `Message` and `StreamMessage` use
`{"kind": discriminator, "body": payload}`; `NativeEvent` uses
`{"type": "trade" | "depth", "data": payload}`. Optional fields are CBOR null.
Unknown mandatory protocol/schema versions are errors, never skipped data.

Every envelope has:

| Field | Meaning |
|---|---|
| protocol_version | unsigned 1 |
| schema_id, schema_version | unsigned 1, 1: fixed v1 session and market schema |
| request_id | nonzero control request/response identity; zero for streaming data or a fatal connection error |
| subscription_id | zero for control, nonzero for subscription messages |
| subscription_sequence | zero for control; starts at 1 per accepted subscription |
| message | tagged message map |

Negotiation is `hello {versions,schemas}` / `welcome {version,schemas,capabilities}`.
Commands are `list_streams {after,limit}`, `describe_stream {stream_id}`,
`subscribe {stream_id,subscription_id}`, `unsubscribe {subscription_id}`, and `ping`.
Responses are `streams`, `description`, `subscribed`, `unsubscribed`, and `pong`.
Errors have `code` and bounded `detail`. Fatal decode/version errors use request ID
zero, send an explicit error where the socket can progress, and close the session. List pages contain at most 128 entries;
`next` is an exclusive stream-name pagination key. Discovery describes payload
semantics and dependencies, not runtime state. Source descriptions include exchange,
instrument ID, symbol, market/instrument kind and optional base/quote assets.
Derived descriptions identify the operation and distinguish reference dependencies.
For depth records, resolve the canonical stream ID or original Source provenance ID
through DescribeStream to obtain its instrument identity. Implemented capabilities are
`live_tail`, `integrity`, and `depth_snapshot_omission`.

Subscribe acceptance is written before starting its delivery task. The initial
message is an integrity snapshot. IDs cannot be reused, including after close;
v1 retains at most 65,536 used subscription IDs per connection. Unsubscribe waits
for cancellation and Router ownership release; stale queued frames are suppressed
before the completion response. No subscription frames follow that response.
Subscription sequence, Router sequence, runtime generation, Source generation,
book generation, native sequence and integrity revision are different identities.
The first live-tail Router sequence is arbitrary. None is a durable cursor.

## Numeric and market representation

Decimal maps contain `coefficient` (signed base-10 integer text) and `scale`
(unsigned decimal places). Native rust_decimal scale is preserved. Conversion
Decimal256 values have scale 28 and up to 76 significant digits. No financial
value passes through a float. Timestamps are signed 64-bit CBOR integers and
sequence/position identifiers are unsigned 64-bit integers; cross-language
consumers must preserve their full integer range. The exact conversion numerator/denominator are
bounded integer strings. References retain dataset ID, source ID and runtime
generation, timestamp, stream rank, row/admission sequence, event ordinal,
direction and exact reference price. The adapter copies the calculation witness;
it never evaluates conversion paths or recalculates prices.

Records retain their ordinal within the original Router publication. Trades
include every native optional quantity, volatility, price and RPI field. Derived
records retain original Source position, immediate upstream position, processing input
position, admission ordinal, baseline barrier, original
integrity evidence and the calculated valuation. Reference evidence is captured
when the reference is consumed and copied into each valuation; later references
do not mutate earlier valuations. The publication-level recovered flag describes
canonical recovery batches; derived recovered provenance is in record evidence.

Each depth delta has one ordered operations array, one book identity and baseline
metadata. SET/REMOVE is decided with the canonical `BookLevel::is_zero()` predicate,
using all available canonical quantities. Native price and side deletion keys
survive conversion. Every full snapshot becomes `snapshot_omitted` with empty
operations and no valuation, including replacement snapshots in the same book.
Neighboring records survive unchanged. `client_book_initialized` is always false;
internal synchronization is descriptive metadata, never a client book guarantee.
Deltas are forwarded to late subscribers immediately.

## Integrity

`subscribe_live` / `recv_live` extend Router observation without changing its
internal processing subscriptions. Registration and initial snapshot capture
hold the Router state lock. The snapshot revision and queued updates therefore
form one cut. Only registered live observers retain update queues. Source
metadata-only observations neither increment Router publication sequences nor
enter the CPU processor, establish readiness, or advance reference/watermark state.

The trade adapter preserves accepted-batch duplicates, numeric-ID/cross-sequence
observations, bounded identity-history eviction and unverified coverage. Coverage
review is a metadata-only observation even with no accepted market records.
Obsolete and repeatedly pending inputs are not new gap observations. Numeric-ID
arithmetic is never interpreted as a missing-execution count. Unknown counts are
null. Accepted trade batches and coverage-review observations now carry their
authoritative Source generation independently from registry runtime generation. Recovery batches identify recovered records and their Source generation; where
known they link the preceding same-generation coverage-review evidence identity.
Raw acquisition counts remain unavailable for this callback. It does not prove
committed complete recovery, so completeness is unavailable.

Routed depth Source processing copies the depth owner's recovery reason and
coverage (subscribed/declared depth, retained bid/ask levels and snapshot source).
Accepted Source/Router depth publications carry the authoritative baseline and
book identities. Where a recovery outcome does not expose observed native
positions, the interface leaves them unavailable. The legacy GapTracker is not
wired into Source processing in this checkout; the adapter does not synthesize
tracker windows or history from raw exchange traffic.

Metadata-only observations expose `after_publication_sequence`, the last accepted
Source Router position at their observation cut; `publication_sequence` remains
null when no market publication was accepted.

Original event evidence is immutable and attached at Router admission. Derived
records reference that original observation identity; repeated IDs after batch
splitting must be deduplicated, not summed. Snapshots traverse existing graph
dependencies, including references, deduplicating shared ancestors. Updates remain
observable while event-time processing is waiting for progress. An active observer
services a queued market publication after at most eight consecutive ordinary
integrity updates; initial and final snapshots retain priority. Market publication
order stays intact. This bounds starvation without ordering integrity by event time. They may describe
a newer observation than a delayed market event: that event's attached evidence
remains the applicable historical revision.

Live observation scopes resolve at most 128 original Sources and 4,096 graph
nodes; larger scopes are rejected explicitly before acquiring demand. Resolved
Source interests are retained per subscription, so publication fanout does not
re-walk the dependency graph.

Bounds: 128 current Source entries, 64 sequence details per observation, 32 recent
relevant observations across the Router, and 256 KiB per snapshot view (measured
conservatively with JSON serialization). Scope/history/detail truncation is explicit.
A replaced generation has unavailable integrity until it supplies new evidence.
An unbound Source has null runtime generation and observation revision; no zero
generation or zero observation is invented. The snapshot's own Router revision
can be zero at the initial observation cut.
A Source has one reserved bounded pending-evidence slot. Relevant observations
survive failed market admission or metadata lock acquisition and are transferred
through terminal cleanup; rejection still returns failure and never commits
Source trade identities. Rejected candidates do not publish their uncommitted
baselines or prospective identity-eviction claims.

Terminal cleanup discards market backlog but retains bounded integrity metadata.
Known ordering, depth continuity and conversion-integrity failures from production
processors are retained as one bounded terminal integrity snapshot before End.
The processor captures the conclusion before returning its failure, avoiding a
race with the mailbox publication fence. Its `discontinuity` describes the
processing owner's conclusion, with an explicit
truncation flag and last accepted derived publication position. Worker statistics
and general lifecycle/configuration state are not included.
An integrity queue overflow retains a final bounded summary and terminates the
subscription. It never silently resumes continuity.

Custody validates the complete retained original trade and immutable depth-level
allocation plus metadata. A modified trade or replacement level array cannot
reuse genuine custody. Retained native allocations and integrity/reference evidence
are charged, including depth allocations pinned by a derived view. Custody objects
are process-local and never serialized or presented as portable authentication.

## Resources and connection behavior

The existing `[publication]` fields retain their meanings. `subscriber_queue_bytes`
is a connection-wide encoded frame budget. Optional `[publication.streaming]`:

| Setting | Default |
|---|---:|
| max_subscriptions | 64 |
| max_frame_bytes | 1,048,576 |
| pending_commands | 32 |
| handshake_timeout_ms | 5,000 |
| read_timeout_ms (partial frame assembly) | 300,000 |
| write_timeout_ms | 5,000 |
| shutdown_timeout_ms | 3,000 |

Idle sessions do not expire merely because a consumer reads market data without
sending control commands. Read deadlines start with the first frame byte.

Zero and excessive limits are rejected. A frame cannot exceed the connection's
queue budget. Default sections are omitted during serialization to preserve legacy
configuration round trips. The pre-existing default derived-processing section
also now follows that compatibility rule. No second configuration loader exists.

Before decoding allocation, CBOR is checked for definite lengths, at most 24
nesting levels, 65,536 total items, 16,384 array entries, 64 map pairs and 4,096-byte
strings. Floats, tags and indefinite-length values are rejected. The absolute frame
ceiling is 16 MiB. Decoded model-specific bounds are checked as well.

Discovery construction is byte-budgeted before cloning descriptions, using
ordered registry ranges; a page inspects at most 256 definitions and returns at
most 128 entries. Oversized descriptions return an explicit request error.
Completed connection-task records count toward the server connection cap until
reaped, bounding cleanup backlog as well as active sessions.

Each connection has one bounded command channel, one coordinated writer and one
worker per subscription. Workers retain at most one Router delivery and one
projected/encoding work unit, in addition to queued frames. Source/derived payload
retention remains bounded by Router ingress/consumer limits and processor budgets;
transport-owned deliveries add at most one such bounded delivery per subscription.
Projection caps record/operation counts by the frame limit before constructing
arrays. Frames retain byte-budget permits through partial writes. The legacy byte budget
counts CBOR payload bytes; length prefixes and per-frame queue overhead are bounded
separately by frame count, preserving that setting's existing meaning. Each worker may
reserve one small terminal frame outside that data budget; the reserve is bounded
by subscription count. Each Router observer reserves at most one initial and one terminal integrity
summary, each bounded to 256 KiB, plus its bounded Source-interest set. Each
producer/processor has one bounded pending Source-evidence slot and one bounded
data-discontinuity slot. Router market and integrity queues have separate bounded
budgets. Intermediate decoded/projected allocations are bounded by these collection
limits and the frame ceiling, not solely by socket queue bytes.

Client options bound frame size, pending requests, subscription count, and local
queue messages/bytes. A local overflow releases that subscription and reports an
error after already queued messages. Queue accounting uses `stream_protocol::accounting::retained_charge`, with checked
arithmetic: one inline StreamMessage plus String capacities, Vec capacities times
element size, and recursively owned allocations of populated elements. Inline fields
inside a parent or Vec are not charged twice. No serialization is used for this
charge. A reservation travels with each queued payload and is released on receive
or destruction, including failed insertion, receiver Drop, close and cancellation.
Already queued messages may still be drained before a local overflow/connection
error; continuity does not resume after that error.

This bounds the documented application-owned payload representation, not allocator
headers, allocator rounding, Tokio channel blocks, process RSS, or messages already
returned to the application. Queue slot/reservation overhead is separately bounded
by `subscription_queue_messages` (Tokio may allocate slots in blocks).
`Subscription::queued_payload_bytes()` reports the current payload reservation
count; it is not a process-memory measurement. The server setting
`subscriber_queue_bytes` remains an encoded CBOR payload budget.

The independent reader-to-session channel holds at most `max_pending_requests`
decoded envelopes, plus one envelope waiting to enqueue and one being handled.
It is **not** covered by `subscription_queue_bytes`. A deliberately coarse bound
for this representation is `(max_pending_requests + 2) * (4*F + 4*65536*S)`,
where F is `max_frame_bytes` and S is the largest inline size of the existing
protocol structs/enums on the target architecture. This uses the 65,536 CBOR-item
limit and a conservative allowance for decoded String/Vec capacity growth. One
additional F-byte input buffer and bounded decoder stack can exist during decode.
Allocator and channel overhead remain outside this payload bound. This ceiling is
intentionally loose; message count and frame/collection limits bound this stage,
not the per-subscription reservation counter. Cancellation leaves framing in
the connection reader. Dropped subscriptions use bounded best-effort cleanup;
if that cleanup channel is full the connection is closed. Request expiration closes
the connection, bounding correlation lifetime. No asynchronous Drop blocks a thread.

Independent subscriptions have no cross-stream time-order promise. A blocked socket
writer stalls that connection and closes it after its deadline, including all its
subscriptions. The generic session loop accepts async read/write halves; the Unix
connector supplies them without owning discovery or subscription semantics. Independent connections and processing consumers continue. Socket
write completion does not acknowledge application processing or persistence.
Unexpected EOF, malformed frames and connection loss are errors, not clean End.
Explicit finite-replay bindings, including reference dependencies, are rejected
by live subscriptions. Attaching one after subscription registration ends those
live observers before any replay publication; ordinary internal consumers keep
their existing behavior.

There is no automatic reconnect, durable delivery, replay, bootstrap, full-book
initialization or historical access.

The Unix adapter requires a private parent owned by the server user, rejects
symlink parents and every existing socket/file, and sets socket permissions to
0600. It deliberately does not recover stale socket paths automatically. Cleanup
checks device/inode identity before removing the created endpoint. Unix connectors
are platform-gated; protocol/projection semantics are separate from socket framing
and ownership. Only Unix transport is implemented.

## Reading integrity and ordering together

```rust,ignore
// `latest` is current knowledge, not a replacement for old event evidence.
match subscription.recv().await? {
    Some(StreamMessage::IntegrityUpdate(update)) => {
        latest.insert(update.evidence.id.source.clone(), update.evidence);
    }
    Some(StreamMessage::Publication(publication)) => {
        // router_sequence orders market publications within this generation.
        for record in publication.records {
            consume(record.native, record.integrity, record.valuation);
            // record.integrity is the applicable immutable historical observation.
            // valuation.reference_integrity belongs to the references actually used.
            // Deduplicate repeated EvidenceId values after a Source batch is split.
        }
    }
    _ => {}
}
```

Subscription-message sequence orders all transport messages. Router-global integrity
revisions may skip within a subscription because unrelated Sources also publish. A
newer integrity update can precede an older queued market publication; its explicit
applicability position does not rewrite that publication's evidence. The first
Router market position of a live tail is arbitrary. Initial snapshot capture and
update registration still share one Router-lock cut. Snapshot bounding uses one
nonallocating JSON byte count and linear removals; JSON bytes are a conservative
publication-view limit, distinct from client structural retention and wire framing.

A Subscription retains shared connection ownership. Dropping all client and
subscription handles requests connection shutdown; dropping a client handle while
a Subscription remains alive allows that Subscription to continue. Explicit client
close ends the connection, and explicit subscription close waits for the unsubscribe
boundary. Full bounded cleanup channels trigger connection closure rather than an
unbounded backlog of asynchronous cleanup work.
