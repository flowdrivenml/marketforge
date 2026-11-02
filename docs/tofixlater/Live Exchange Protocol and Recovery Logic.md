## Quick Navigation

- [Purpose](#purpose)
- [Core Principle](#core-principle)
- [Generic Connection State Machine](#generic-connection-state-machine)
- [Book State Machine](#book-state-machine)
- [Sequence Model](#sequence-model)
- [Reconnect Rules](#reconnect-rules)
- [Binance](#binance)
- [Bybit](#bybit)
- [Bitget](#bitget)
- [OKX](#okx)
- [Gateio](#gateio)
- [REST Snapshot Polling](#rest-snapshot-polling)
- [Trade Streams](#trade-streams)
- [Book Validity](#book-validity)
- [Failure Classification](#failure-classification)
- [Implementation Requirements](#implementation-requirements)

## Purpose

This document defines the exchange-specific live acquisition, bootstrap, sequencing, recovery, and reconnection behavior required by `marketforge-live`.

It is intentionally separate from raw-format and normalization specifications.

Raw-format specifications answer:

```text
What does the exchange send?
```

This document answers:

```text
How must the live engine behave?
```

The implementation must handle:

```text
WebSocket connection
subscription
heartbeat
initial bootstrap
REST snapshot acquisition
WebSocket buffering
snapshot/update synchronization
sequence validation
book reconstruction
sequence gaps
reconnection
rebootstrap
periodic REST snapshots
trade ingestion
duplicate prevention
shutdown
```

Exchange-specific behavior must remain explicit.

A single generic algorithm must not be forced onto protocols with different sequencing and bootstrap semantics.

---

## Core Principle

A live order book is either:

```text
VALID
```

or:

```text
INVALID
```

There is no partially trusted state.

Once MarketForge detects that required updates may have been missed:

```text
VALID
    ↓
sequence gap / protocol reset / connection loss
    ↓
INVALID
```

the current reconstructed book must not continue producing trusted canonical state.

Recovery requires exchange-specific rebootstrap.

```text
INVALID
    ↓
bootstrap
    ↓
VALID
```

MarketForge must never guess missing order-book changes.

---

## Generic Connection State Machine

Every live WebSocket connection should follow an explicit state machine.

```text
DISCONNECTED
    ↓
CONNECTING
    ↓
CONNECTED
    ↓
SUBSCRIBING
    ↓
SUBSCRIBED
    ↓
BOOTSTRAPPING
    ↓
LIVE
```

Failure from any active state may produce:

```text
RECONNECTING
```

followed by:

```text
CONNECTING
```

or complete termination when shutdown was requested.

Suggested states:

```text
Disconnected
Connecting
Connected
Subscribing
Subscribed
Bootstrapping
Live
Reconnecting
ShuttingDown
Stopped
```

Connection state and book state are related but should not be identical.

For example:

```text
WebSocket = Connected
Book      = Bootstrapping
```

is valid.

Likewise:

```text
WebSocket = Connected
Book      = Invalid
```

may temporarily exist while a REST snapshot is being reacquired.

---

## Book State Machine

Each subscribed instrument/book should maintain independent state.

```text
UNINITIALIZED
    ↓
BOOTSTRAPPING
    ↓
VALID
```

Failures transition:

```text
VALID
    ↓
INVALID
    ↓
BOOTSTRAPPING
```

Recommended states:

```text
Uninitialized
Buffering
SnapshotPending
Synchronizing
Valid
Invalid
```

A connection may carry multiple books.

One book becoming invalid must not automatically invalidate unrelated instruments unless the exchange protocol requires reconnecting the entire connection.

---

## Sequence Model

The canonical live sequence representation should support:

```text
update_sequence_first
update_sequence_last
update_sequence_previous
cross_sequence
```

All fields are nullable.

Different exchanges map into this representation differently.

### Binance

```text
update_sequence_first    = U
update_sequence_last     = u
update_sequence_previous = pu where available
cross_sequence           = null
```

### Gate.io

```text
update_sequence_first    = U
update_sequence_last     = u
update_sequence_previous = null
cross_sequence           = null
```

### Bitget

```text
update_sequence_first    = seq
update_sequence_last     = seq
update_sequence_previous = pseq
cross_sequence           = null
```

### OKX

```text
update_sequence_first    = seqId
update_sequence_last     = seqId
update_sequence_previous = prevSeqId
cross_sequence           = null
```

### Bybit

```text
update_sequence_first    = u
update_sequence_last     = u
update_sequence_previous = null
cross_sequence           = seq
```

Sequence normalization does not replace exchange-specific validation rules.

---

## Reconnect Rules

A WebSocket reconnect must be treated as a potential data discontinuity.

The engine must not assume:

```text
old connection state
    +
new connection updates
    =
continuous book
```

unless the exchange protocol explicitly provides enough information to prove continuity.

Default rule:

```text
connection lost
    ↓
mark affected books INVALID
    ↓
reconnect
    ↓
resubscribe
    ↓
exchange-specific bootstrap
    ↓
VALID
```

Trades and books have different recovery requirements.

For trades, reconnection may create a temporal gap but does not require maintaining state.

For reconstructed books, reconnection normally requires rebootstrap.

---

# Binance

## Connection Model

Binance uses separate public WebSocket endpoints for major market families.

Observed live families include:

```text
Spot
Linear
Inverse
```

Subscriptions may be represented directly by stream URLs.

The connection manager must support:

```text
connect
receive
detect disconnect
reconnect
resubscribe
```

---

## Binance Trades

Trade messages are stateless.

Each valid trade message can be normalized independently.

```text
WebSocket
    ↓
trade message
    ↓
normalize
    ↓
emit Trade
```

No order-book bootstrap state is required.

After reconnect:

```text
reconnect
    ↓
resume trade ingestion
```

MarketForge must not manufacture trades for the disconnected interval.

Any resulting gap belongs to data-integrity metadata.

---

## Binance L2 Bootstrap

Binance incremental depth requires a REST snapshot plus buffered WebSocket updates.

The required architecture is:

```text
open WebSocket
    ↓
buffer depth updates
    ↓
request REST snapshot
    ↓
read lastUpdateId
    ↓
discard obsolete buffered updates
    ↓
find first applicable update
    ↓
initialize local book
    ↓
replay buffered updates
    ↓
LIVE
```

The engine must start receiving WebSocket updates before or concurrently with REST snapshot acquisition.

It must not:

```text
fetch snapshot
    ↓
then open WebSocket
```

because updates occurring between those operations could be lost.

---

## Binance Sequence Range

Binance updates expose an update range:

```text
U
u
```

and some derivative streams additionally expose:

```text
pu
```

Canonical mapping:

```text
first    = U
last     = u
previous = pu when available
```

### Bootstrap

Given:

```text
snapshot_id = lastUpdateId
```

discard buffered events where:

```text
u <= snapshot_id
```

Find the first event covering the required next update:

```text
U <= snapshot_id + 1 <= u
```

Apply that event and continue.

### Continuous Validation

Range continuity is validated using the exchange-specific sequence rules.

Where `pu` exists:

```text
current.pu == previous.u
```

provides an explicit continuity check.

A detected gap invalidates the book.

---

## Binance Recovery

```text
gap detected
    ↓
INVALID
    ↓
discard local book
    ↓
start buffering updates
    ↓
request new REST snapshot
    ↓
synchronize
    ↓
replay
    ↓
VALID
```

The same process applies after connection loss.

---

# Bybit

Bybit has two materially different L2 protocols.

```text
Normal L2
Full-depth L2
```

They must not share the same bootstrap state machine.

---

## Bybit Normal L2

Normal streams provide:

```text
snapshot
    ↓
delta
    ↓
delta
```

The WebSocket therefore bootstraps itself.

### Bootstrap

```text
connect
    ↓
subscribe
    ↓
wait for type=snapshot
    ↓
replace local book
    ↓
record u
    ↓
record seq
    ↓
VALID
```

REST is not required.

### Delta Processing

Each delta contains:

```text
u
seq
```

Observed `u` behavior is consecutive:

```text
next.u = previous.u + 1
```

`seq` is a separate cross-sequence domain and is not expected to increment by one.

Processing:

```text
receive delta
    ↓
validate u
    ↓
apply levels
    ↓
update local u
    ↓
preserve seq
```

### Snapshot During Live Operation

If Bybit sends another:

```text
type = snapshot
```

the existing local book must be replaced rather than merged as a delta.

```text
snapshot
    ↓
clear local book
    ↓
load snapshot
    ↓
reset sequence state
```

---

## Bybit Normal Recovery

On sequence failure:

```text
INVALID
    ↓
resubscribe / reconnect
    ↓
wait for new WebSocket snapshot
    ↓
rebuild
    ↓
VALID
```

REST synchronization is unnecessary for this protocol.

---

## Bybit Full-Depth L2

Full-depth uses:

```text
orderbook.full.<symbol>
```

and was observed to produce:

```text
delta only
```

No WebSocket snapshot is provided.

Therefore full-depth requires:

```text
WebSocket buffer
        +
REST full-depth snapshot
        ↓
synchronization
```

### Bootstrap

```text
connect full-depth WebSocket
    ↓
buffer deltas
    ↓
request full-depth REST snapshot
    ↓
obtain snapshot u / seq
    ↓
synchronize with buffered stream
    ↓
load snapshot
    ↓
replay later deltas
    ↓
VALID
```

The full-depth implementation must preserve both:

```text
u
seq
```

because they belong to different sequence domains.

### Continuous Validation

Observed:

```text
next.u = previous.u + 1
```

A gap invalidates the book.

A protocol reset signal such as a reset update ID must trigger rebootstrap according to the source specification.

---

## Bybit Full-Depth Recovery

```text
gap / reset / reconnect
    ↓
INVALID
    ↓
buffer new WS deltas
    ↓
fetch new full-depth REST snapshot
    ↓
synchronize
    ↓
replay
    ↓
VALID
```

---

## Bybit Trades

Bybit trade messages must not be treated as bootstrap history merely because the envelope contains:

```text
type = snapshot
```

Observed Option trade streams repeatedly used `snapshot` for normal newly arriving trade messages.

Therefore:

```text
type=snapshot
```

has channel-specific meaning.

Trade processing must follow the Bybit trade format rather than generic snapshot semantics.

---

# Bitget

## Connection Model

Bitget uses a public WebSocket endpoint with explicit subscriptions.

The inspected markets include:

```text
Spot
USDT Linear
USDC Linear
```

---

## Bitget L2 Bootstrap

Bitget `books` is self-bootstrapping.

```text
subscribe
    ↓
action=snapshot
    ↓
action=update
    ↓
action=update
```

No REST snapshot is required for normal streaming.

### Snapshot

Observed:

```text
pseq = 0
seq  = A
```

Bootstrap:

```text
wait for snapshot
    ↓
verify bootstrap state
    ↓
construct local book
    ↓
last_seq = seq
    ↓
VALID
```

---

## Bitget L2 Updates

Each update exposes:

```text
pseq
seq
```

Observed continuity:

```text
current.pseq == previous.seq
```

Processing:

```text
receive update
    ↓
validate pseq
    ↓
apply changed levels
    ↓
last_seq = seq
```

Unlike Bybit, `seq` itself is not required to increment by one.

The explicit previous/current link is authoritative.

---

## Bitget Recovery

On mismatch:

```text
current.pseq != previous.seq
```

the book becomes invalid.

Recovery:

```text
INVALID
    ↓
resubscribe / reconnect
    ↓
wait for new action=snapshot
    ↓
rebuild
    ↓
VALID
```

No REST dependency is required.

---

## Bitget REST Books

REST books remain first-class live acquisition sources even though WebSocket bootstrap does not require them.

They may be used for:

```text
periodic snapshot polling
independent validation
diagnostics
reconciliation
```

They should not automatically be inserted into the normal WebSocket bootstrap path.

---

## Bitget Trades

The initial Bitget trade message may contain:

```text
action = snapshot
```

with multiple recent trades.

This differs from Bybit.

The live engine must explicitly distinguish:

```text
Bitget trade bootstrap history
```

from:

```text
new trade updates
```

A reconnect must not blindly emit the initial historical trade batch as new trades.

Possible handling:

```text
discard initial trade snapshot
```

or:

```text
deduplicate using tradeId
```

The final runtime policy should be explicit.

---

# OKX

## Connection Model

OKX public market data uses explicit WebSocket subscriptions.

Inspected families include:

```text
Spot
Linear Swap
Linear Future
Inverse Future
Option
```

The normal `books` channel is self-bootstrapping.

---

## OKX L2 Bootstrap

Initial message:

```text
action = snapshot
```

Observed:

```text
prevSeqId = -1
seqId     = A
```

Bootstrap:

```text
connect
    ↓
subscribe books
    ↓
wait for snapshot
    ↓
construct local book
    ↓
last_seq = seqId
    ↓
VALID
```

REST is not required.

---

## OKX L2 Updates

Subsequent messages use:

```text
action = update
```

with:

```text
prevSeqId
seqId
```

Observed continuity:

```text
current.prevSeqId == previous.seqId
```

Processing:

```text
receive update
    ↓
validate prevSeqId
    ↓
apply changed levels
    ↓
last_seq = seqId
```

`seqId` is not required to increment numerically by one.

---

## OKX Checksum

The inspected WebSocket format contains:

```text
checksum
```

but all observed values were:

```text
0
```

The implementation should parse and preserve the field but must not assume that checksum validation is available unless the source format/rule explicitly enables it.

Checksum validation should therefore be optional capability metadata rather than unconditional OKX behavior.

---

## OKX Recovery

Sequence mismatch:

```text
current.prevSeqId != previous.seqId
```

invalidates the book.

Recovery:

```text
INVALID
    ↓
resubscribe / reconnect
    ↓
wait for new snapshot
    ↓
rebuild
    ↓
VALID
```

---

## OKX REST Books

REST books are independent point-in-time snapshots.

They support:

```text
periodic snapshot acquisition
validation
reconciliation
```

They are not required by the normal `books` bootstrap algorithm.

---

# Gateio

## Connection Model

Gate.io exposes different WebSocket endpoints for:

```text
Spot
USDT derivatives
BTC-settled derivatives
USDT delivery
```

The inspected L2 update streams are delta-only.

---

## Gate.io L2 Sequence Model

Every update exposes:

```text
U
u
```

representing:

```text
first update ID
last update ID
```

Observed continuity:

```text
current.U = previous.u + 1
```

Canonical mapping:

```text
update_sequence_first = U
update_sequence_last  = u
```

---

## Gate.io L2 Bootstrap

Gate.io requires REST snapshot synchronization.

```text
connect WebSocket
    ↓
subscribe order_book_update
    ↓
buffer U/u updates
    ↓
fetch REST order book with with_id=true
    ↓
snapshot_id = REST.id
    ↓
discard obsolete buffered updates
    ↓
find range containing snapshot_id + 1
    ↓
load REST snapshot
    ↓
replay buffered updates
    ↓
VALID
```

The first applicable buffered range satisfies:

```text
U <= snapshot_id + 1 <= u
```

After applying it:

```text
local_sequence = u
```

Subsequent messages require:

```text
next.U = local_sequence + 1
```

---

## Gate.io Recovery

On:

```text
next.U != previous.u + 1
```

the local book becomes invalid.

Recovery:

```text
INVALID
    ↓
continue/begin buffering
    ↓
request new REST snapshot
    ↓
synchronize REST.id with U/u
    ↓
rebuild
    ↓
VALID
```

The same procedure applies after WebSocket reconnect.

---

## Gate.io Spot vs Derivatives

Gate.io uses different physical level formats.

Spot:

```text
[price, quantity]
```

Derivatives:

```text
{
    p: price,
    s: quantity
}
```

This affects parsing but not bootstrap state logic.

Both use:

```text
quantity > 0 → set
quantity = 0 → delete
```

---

# REST Snapshot Polling

REST snapshot acquisition should exist independently from WebSocket reconstruction.

A user may request:

```text
one book snapshot every N milliseconds/seconds
```

without requiring a continuously reconstructed book.

Generic polling:

```text
request
    ↓
parse
    ↓
normalize L2Snapshot
    ↓
emit/store
    ↓
wait
    ↓
repeat
```

Each exchange adapter defines:

```text
endpoint
maximum depth
rate limit
timestamp interpretation
snapshot sequence metadata
```

The scheduler determines the legal polling cadence.

Polling and WebSocket reconstruction must remain separate runtime modes.

---

## Trade Streams

Trade streams are stateless compared with reconstructed order books.

The generic trade lifecycle is:

```text
DISCONNECTED
    ↓
CONNECT
    ↓
SUBSCRIBE
    ↓
LIVE
    ↓
parse
    ↓
normalize
    ↓
deduplicate if required
    ↓
emit
```

A trade stream does not require:

```text
REST snapshot
book bootstrap
sequence reconstruction
```

However, reconnect behavior still matters.

---

## Trade Reconnects

A connection failure creates an unknown interval:

```text
last received trade
    ↓
DISCONNECT
    ↓
unknown interval
    ↓
RECONNECT
    ↓
first received trade
```

MarketForge must not assume the interval contains no trades.

Possible future recovery mechanisms include:

```text
REST recent trades
trade-ID reconciliation
exchange historical endpoint
```

but these must be explicitly supported by the exchange adapter.

Default behavior:

```text
record disconnect
    ↓
reconnect
    ↓
resume ingestion
    ↓
record potential data gap
```

The live runtime must never synthesize missing executions.

---

## Trade Deduplication

Reconnects can cause duplicate executions where an exchange sends recent trade history after subscription.

Deduplication should use stable exchange identifiers where available.

Preferred key:

```text
exchange
market
instrument
trade_id
```

If trade IDs are unique only within another scope, that scope must be included.

A small bounded recent-ID cache can protect the live ingestion path:

```text
receive trade
    ↓
trade_id already seen?
    ├── yes → discard duplicate
    └── no  → emit + remember
```

The cache must be bounded.

It is not a replacement for database uniqueness constraints.

---

## Bitget Trade Bootstrap

Bitget deserves explicit handling because the first trade message may contain recent trade history:

```text
subscribe
    ↓
action=snapshot
    ↓
multiple historical/recent trades
    ↓
action=update
    ↓
new trades
```

The engine should not blindly interpret every trade in the initial snapshot as newly occurring after subscription.

Possible implementation:

```text
SUBSCRIBING
    ↓
receive trade snapshot
    ↓
normalize
    ↓
deduplicate against persisted/recent trade IDs
    ↓
transition to LIVE
```

For a fresh acquisition with no prior state, policy may permit storing the snapshot as bootstrap history.

For reconnects, deduplication is required if bootstrap trades are persisted.

This behavior should be configurable rather than hidden inside the parser.

---

## Bybit Trade Messages

Bybit demonstrates why generic interpretation of fields such as:

```text
snapshot
update
```

must be avoided outside the channel-specific adapter.

Observed Bybit trade messages can use:

```text
type = snapshot
```

for normal live trade delivery.

Therefore:

```text
if type == snapshot:
    treat as historical bootstrap
```

is invalid generic logic.

Only the Bybit trade adapter knows the semantic meaning of the envelope.

---

## Heartbeats

Heartbeat handling belongs to the connection layer rather than market-data parsers.

Each exchange adapter should define:

```text
heartbeat mode
ping interval
pong expectation
timeout
server-ping handling
```

Possible protocol models include:

```text
WebSocket protocol ping/pong
application-level "ping"/"pong"
JSON ping message
server-driven heartbeat
no explicit application heartbeat
```

The connection manager should expose a generic outcome:

```text
ConnectionHealthy
ConnectionTimedOut
```

rather than leaking exchange heartbeat payloads into book logic.

---

## Heartbeat Timeout

The runtime should track:

```text
last_message_time
last_ping_time
last_pong_time
```

where applicable.

A connection should be considered stale when its exchange-specific heartbeat policy is violated.

```text
no required response
    ↓
STALE
    ↓
close socket
    ↓
RECONNECT
```

Do not leave a socket indefinitely connected merely because the TCP connection has not formally closed.

---

## Silent Connections

A lack of market-data messages does not necessarily indicate failure.

This is particularly important for:

```text
illiquid futures
options
delivery contracts
```

A valid subscription may legitimately receive no trades for long periods.

Therefore connection liveness must not be determined by:

```text
time since last trade
```

alone.

Use:

```text
heartbeat state
socket state
subscription state
protocol-level activity
```

instead.

Order-book channels on active markets may provide additional liveness evidence, but this should not replace heartbeat policy.

---

## Subscription State

Subscriptions must be explicitly tracked.

Conceptually:

```text
Pending
Confirmed
Active
Failed
```

A successful TCP/WebSocket connection does not imply a successful market-data subscription.

The runtime should not enter:

```text
LIVE
```

until the required subscription has been accepted and the required bootstrap has completed.

For example:

```text
CONNECTED
    ↓
send subscription
    ↓
SUBSCRIBING
    ↓
receive acknowledgement
    ↓
SUBSCRIBED
```

Only then does book bootstrap proceed.

---

## Subscription Failure

An explicit subscription rejection should not be treated as a sequence failure.

It is a configuration/protocol failure.

Examples include:

```text
invalid symbol
unsupported channel
invalid depth
invalid market type
rate-limit rejection
authentication requirement
```

The runtime should surface the actual exchange error and avoid infinite aggressive reconnect loops.

Classification:

```text
subscription rejected
    ↓
configuration/protocol error
```

rather than:

```text
network error
    ↓
retry forever
```

---

## Reconnect Backoff

Network reconnects require bounded exponential backoff with jitter.

Conceptually:

```text
attempt 1 → short delay
attempt 2 → longer delay
attempt 3 → longer delay
...
```

with a configured maximum.

Example policy:

```text
base delay
maximum delay
jitter
attempt counter
```

A successful stable connection resets the reconnect counter.

Do not reconnect in a tight loop.

This protects:

```text
exchange rate limits
local CPU
network resources
exchange connection limits
```

---

## Failure Classification

Failures should be classified before deciding recovery behavior.

### Transport Failure

Examples:

```text
TCP disconnect
WebSocket close
TLS failure
DNS failure
heartbeat timeout
```

Recovery:

```text
reconnect
    ↓
resubscribe
    ↓
rebootstrap stateful books
```

### Subscription Failure

Examples:

```text
invalid channel
invalid instrument
unsupported depth
exchange rejection
```

Recovery:

```text
report error
```

Retry only where the error is explicitly transient.

### Sequence Failure

Examples:

```text
missing update range
previous sequence mismatch
unexpected reset
```

Recovery:

```text
invalidate affected book
    ↓
exchange-specific rebootstrap
```

A full connection reconnect is not always required.

### Parsing Failure

Examples:

```text
malformed payload
unexpected field type
unsupported source-format version
```

The raw message should be retained or logged sufficiently for diagnosis.

Whether processing continues depends on whether sequence continuity can still be guaranteed.

For a stateful book stream, dropping an unparsed update generally means:

```text
book continuity lost
    ↓
INVALID
```

### REST Snapshot Failure

Examples:

```text
timeout
HTTP 5xx
rate limit
malformed response
```

For REST+WS bootstrap:

```text
keep buffering within configured bounds
    ↓
retry snapshot according to policy
```

If the buffer exceeds safe limits:

```text
abort bootstrap
    ↓
restart bootstrap
```

### Database Failure

Database failure must not silently block the market-data receive loop indefinitely.

The sink requires explicit backpressure and failure policy.

Possible result:

```text
sink unavailable
    ↓
bounded queue fills
    ↓
runtime cannot preserve complete stream
    ↓
invalidate/stop affected pipeline
```

Silent unbounded memory growth is forbidden.

---

## Buffer Management

REST+WebSocket bootstrap requires bounded buffering.

Used by:

```text
Binance
Gate.io
Bybit full-depth
```

The buffer should contain normalized or minimally parsed updates with sequence metadata.

Conceptually:

```text
VecDeque<L2Update>
```

Required limits include:

```text
maximum update count
maximum bytes
maximum bootstrap duration
```

If any limit is exceeded before synchronization succeeds:

```text
bootstrap failed
    ↓
discard buffer
    ↓
restart bootstrap
```

Do not allow an unavailable REST endpoint to produce unlimited memory growth.

---

## Buffer Ordering

Buffered updates must be processed according to source sequence semantics, not local arrival timestamp.

Arrival order should normally already reflect WebSocket order, but sequence metadata remains authoritative for validation.

Do not sort valid WebSocket updates unnecessarily.

Instead:

```text
receive order
    ↓
validate sequence relationship
```

Sorting can hide protocol failures.

---

## Duplicate L2 Updates

An update may be:

```text
new
obsolete
duplicate
overlapping
gap-producing
```

These states must be distinguished during bootstrap.

### Obsolete

Example:

```text
update.last <= snapshot_sequence
```

Discard.

### Applicable

Example range protocol:

```text
update.first <= expected <= update.last
```

Use as bootstrap transition.

### Duplicate

An already-applied sequence range must not be applied twice.

### Gap

```text
update.first > expected
```

when exact range continuity is required.

This invalidates synchronization.

---

## Book Replacement

A native snapshot always represents authoritative state for the depth represented by that source.

Processing should be:

```text
receive snapshot
    ↓
clear corresponding local book state
    ↓
load bids
    ↓
load asks
    ↓
replace sequence state
```

Do not merge a new native snapshot into stale state as though it were an update.

This applies particularly to:

```text
Bybit normal L2
Bitget
OKX
```

---

## Book Depth

The runtime must distinguish:

```text
source depth
maintained depth
output depth
```

Example:

```text
source provides 400 levels
engine maintains 400
user requests top 50 output
```

The parser should not unnecessarily truncate before book maintenance unless the acquisition mode explicitly requests a smaller source depth.

For incremental streams, dropping deeper updates may prevent correct future top-N state if those levels later move into the requested range.

---

## Book Validity

A book may be emitted as trusted only when:

```text
bootstrap complete
sequence valid
source connection valid
required snapshot loaded
all required buffered updates applied
```

Suggested metadata:

```text
book_state = valid
```

Internally, the engine should know:

```text
snapshot sequence
current sequence
last event timestamp
last arrival timestamp
bootstrap generation
connection generation
```

A useful concept is:

```text
generation
```

Every rebootstrap increments the generation.

This prevents updates belonging to an old connection/bootstrap cycle from being accidentally applied to a new book.

---

## Connection Generation

Every successful connection can receive a monotonically increasing local generation ID:

```text
connection_generation
```

Example:

```text
connection 1 → generation 1
disconnect
connection 2 → generation 2
```

Buffered messages carry their generation.

An update from generation 1 must never mutate a book bootstrapped under generation 2.

Likewise, REST snapshot requests should carry the bootstrap generation that initiated them.

If an old asynchronous REST response arrives after a new bootstrap has begun:

```text
snapshot.generation != active_generation
    ↓
discard
```

This prevents race conditions during rapid reconnect/rebootstrap cycles.

---

## Bootstrap Generation

Book bootstrap should additionally have its own generation.

```text
bootstrap_generation += 1
```

whenever:

```text
initial bootstrap begins
sequence gap occurs
protocol reset occurs
manual resync occurs
```

This protects against:

```text
old REST response
old buffered update
old timeout callback
```

modifying a newer book state.

---

## Reconnect and Resubscribe

Generic reconnect procedure:

```text
detect connection failure
    ↓
mark connection unavailable
    ↓
invalidate dependent stateful books
    ↓
increment connection generation
    ↓
backoff
    ↓
connect
    ↓
restore subscriptions
    ↓
start exchange-specific bootstrap
```

Trade streams can resume immediately after subscription.

Book streams cannot become valid until their bootstrap strategy completes.

---

## Multiple Subscriptions Per Connection

The runtime should not assume:

```text
one socket = one instrument
```

even if early implementation begins that way.

A connection may eventually carry:

```text
multiple instruments
multiple channels
trades + books
```

State should therefore be keyed by something equivalent to:

```text
exchange
market
instrument
channel
```

Sequence state belongs to the individual book stream unless the source protocol explicitly defines connection-wide sequencing.

---

## REST Snapshot Requests

Snapshot requests used for bootstrap should carry:

```text
exchange
market
instrument
requested depth
bootstrap generation
request start time
```

When the response arrives:

```text
generation still active?
    ├── no  → discard
    └── yes → parse and synchronize
```

A successful HTTP response is not sufficient.

The snapshot must also satisfy the exchange-specific synchronization rules against the buffered WebSocket stream.

---

## REST Rate Limits

Bootstrap retries and periodic polling share exchange REST capacity.

The runtime must coordinate:

```text
bootstrap snapshot requests
recovery snapshot requests
periodic snapshot polling
other live REST requests
```

through exchange-aware rate limiting.

Recovery/bootstrap requests should normally have higher priority than optional periodic polling because a stateful book cannot return to `VALID` without them.

Conceptually:

```text
REST scheduler

priority:
1. book recovery
2. initial bootstrap
3. required validation
4. periodic polling
```

Exact priorities remain runtime policy.

---

## REST Snapshot Polling Failure

Periodic snapshot mode is stateless between successful requests.

Failure:

```text
request failed
    ↓
record missing observation
    ↓
retry according to polling policy
```

Do not fabricate a snapshot for the failed interval.

A previous snapshot may remain stored historically but must not be relabeled with the new requested timestamp.

---

## Book Reconciliation

REST reconciliation is optional for self-bootstrapping WebSocket books.

Useful for:

```text
Bitget
OKX
Bybit normal L2
```

Conceptually:

```text
maintained WS book
        │
        │ periodically
        ▼
REST snapshot
        ↓
compare common depth
```

Possible outcomes:

```text
MATCH
MISMATCH
INCONCLUSIVE
```

`INCONCLUSIVE` is necessary because REST and WebSocket snapshots are not guaranteed to represent the exact same instant.

Reconciliation must account for:

```text
REST request latency
exchange event timestamps
sequence metadata where available
updates occurring during request
different source depths
```

A naïve equality check between asynchronously sampled books can produce false failures.

---

## Reconciliation Policy

Reconciliation should initially be diagnostic rather than destructive unless sequence-compatible comparison is available.

Recommended initial behavior:

```text
mismatch
    ↓
emit integrity event
```

Optional stricter mode:

```text
persistent mismatch
    ↓
force rebootstrap
```

Do not rebootstrap on every transient REST/WS difference without temporal/sequence reasoning.

---

## Multi-Exchange Ordering

Cross-exchange merging cannot use arrival order alone.

Different exchanges have:

```text
different network latency
different matching-engine latency
different batching
different timestamp precision
different WebSocket intervals
```

Every canonical event should therefore preserve at least:

```text
event_timestamp
arrival_timestamp
exchange
instrument
source sequence metadata
```

Ordering within one exchange should prefer exchange sequence semantics where available.

Ordering across exchanges generally uses:

```text
event_timestamp
```

with:

```text
arrival_timestamp
```

as additional latency/provenance information.

---

## Cross-Exchange Reorder Buffer

For merged output, a bounded reorder window may be required.

Conceptually:

```text
Exchange A ─┐
Exchange B ─┤
Exchange C ─┼→ reorder buffer → ordered merged stream
Exchange D ─┤
Exchange E ─┘
```

The buffer waits a configured amount of time for delayed events.

Then events older than the watermark are released.

Conceptually:

```text
watermark =
    observed_time - reorder_window
```

Events older than the watermark can be emitted.

The exact algorithm should be designed separately from exchange connection/bootstrap logic.

The exchange adapters must simply preserve sufficient timestamps and sequence metadata.

---

## Late Events

A merged pipeline must define policy for events arriving after the reorder watermark.

Possible handling:

```text
emit with late flag
store separately
drop
```

Dropping should not be the silent default for research-grade acquisition.

Late-event status is valuable analytical information.

---

## Shutdown

Graceful shutdown should be explicit.

```text
RUNNING
    ↓
shutdown requested
    ↓
stop new subscriptions
    ↓
stop new REST polling
    ↓
close WebSockets
    ↓
drain bounded internal queues
    ↓
flush sink
    ↓
STOPPED
```

A forced shutdown may skip draining but should be distinguishable from a clean shutdown.

---

## Failure Observability

Every important runtime transition should be observable.

Examples:

```text
connected
subscription accepted
bootstrap started
snapshot requested
snapshot received
bootstrap synchronized
book valid
sequence gap
book invalidated
reconnect scheduled
reconnected
rebootstrap completed
REST rate limited
heartbeat timeout
sink backpressure
```

Metrics should eventually include:

```text
reconnect_count
sequence_gap_count
bootstrap_count
bootstrap_failure_count
bootstrap_duration
messages_received
messages_parsed
parse_failures
duplicate_trades
late_events
REST_requests
REST_failures
book_invalid_duration
```

These are operational metadata, not canonical market data.

---

## Exchange Recovery Matrix

| Exchange / Mode | Initial Book | Gap Recovery | Reconnect Recovery |
|---|---|---|---|
| Binance L2 | REST + buffered WS | REST + buffered WS | REST + buffered WS |
| Bybit Normal L2 | WS snapshot | New WS snapshot | New WS snapshot |
| Bybit Full Depth | REST + buffered WS | REST + buffered WS | REST + buffered WS |
| Bitget L2 | WS snapshot | New WS snapshot | New WS snapshot |
| OKX `books` | WS snapshot | New WS snapshot | New WS snapshot |
| Gate.io L2 | REST + buffered WS | REST + buffered WS | REST + buffered WS |

This distinction should drive implementation.

Do not encode recovery as:

```text
if exchange == ...
```

throughout the runtime.

Instead, formats should declare a recovery/bootstrap strategy.

---

## Protocol Capabilities

Each live format should eventually expose capabilities equivalent to:

```text
transport
bootstrap_strategy
sequence_strategy
has_native_snapshot
requires_rest_snapshot
supports_rest_polling
has_explicit_previous_sequence
has_update_range
has_cross_sequencehas_checksum
supports_reconciliation
trade_bootstrap_behavior
heartbeat_strategy
```

For example:

```text
Gate.io L2
────────────────────────────────
bootstrap_strategy      = rest_snapshot_with_buffer
sequence_strategy       = update_range
has_native_snapshot     = false
requires_rest_snapshot  = true
has_update_range        = true
has_cross_sequence      = false
```

Bitget:

```text
Bitget L2
────────────────────────────────
bootstrap_strategy              = websocket_snapshot
sequence_strategy               = explicit_previous
has_native_snapshot             = true
requires_rest_snapshot          = false
has_explicit_previous_sequence  = true
supports_rest_polling           = true
```

Bybit normal:

```text
Bybit Normal L2
────────────────────────────────
bootstrap_strategy      = websocket_snapshot
sequence_strategy       = consecutive_single
has_native_snapshot     = true
requires_rest_snapshot  = false
has_cross_sequence      = true
```

Bybit full depth:

```text
Bybit Full L2
────────────────────────────────
bootstrap_strategy      = rest_snapshot_with_buffer
sequence_strategy       = consecutive_single
has_native_snapshot     = false
requires_rest_snapshot  = true
has_cross_sequence      = true
```

OKX:

```text
OKX books
────────────────────────────────
bootstrap_strategy              = websocket_snapshot
sequence_strategy               = explicit_previous
has_native_snapshot             = true
requires_rest_snapshot          = false
has_explicit_previous_sequence  = true
has_checksum                    = physical field present
```

Binance:

```text
Binance Depth
────────────────────────────────
bootstrap_strategy      = rest_snapshot_with_buffer
sequence_strategy       = update_range
has_native_snapshot     = false
requires_rest_snapshot  = true
has_update_range        = true
```

This allows the runtime to operate primarily from protocol capabilities rather than exchange names.

---

## Suggested Bootstrap Strategy Enum

The implementation can reduce the observed protocols to a small number of bootstrap strategies:

```text
WebSocketSnapshot
RestSnapshotWithBufferedUpdates
StatelessRestSnapshot
```

Conceptually:

```rust
enum BootstrapStrategy {
    WebSocketSnapshot,
    RestSnapshotWithBufferedUpdates,
    StatelessRestSnapshot,
}
```

Observed mapping:

```text
WebSocketSnapshot
    Bitget books
    OKX books
    Bybit normal books

RestSnapshotWithBufferedUpdates
    Binance depth
    Gate.io books
    Bybit full-depth

StatelessRestSnapshot
    REST polling mode
```

The exchange adapter still owns the exact synchronization condition.

---

## Suggested Sequence Strategy Enum

Likewise, sequence validation can be represented by a small number of strategies:

```text
UpdateRange
ExplicitPrevious
ConsecutiveSingle
None
```

Conceptually:

```rust
enum SequenceStrategy {
    UpdateRange,
    ExplicitPrevious,
    ConsecutiveSingle,
    None,
}
```

Mapping:

```text
Binance
    → UpdateRange

Gate.io
    → UpdateRange

Bitget
    → ExplicitPrevious

OKX
    → ExplicitPrevious

Bybit
    → ConsecutiveSingle
```

`cross_sequence` remains additional metadata rather than its own continuity strategy.

---

## Exchange-Specific Synchronization Must Remain Explicit

Bootstrap strategy can be generic.

The exact synchronization rule cannot always be generic.

For example:

```text
Binance
    REST lastUpdateId
        ↔
    WS U/u

Gate.io
    REST id
        ↔
    WS U/u

Bybit full-depth
    REST u/seq
        ↔
    WS u/seq
```

All belong to:

```text
RestSnapshotWithBufferedUpdates
```

but use different snapshot/update relationships.

Therefore each format needs something equivalent to:

```text
synchronize(snapshot, buffered_updates)
```

rather than placing all synchronization rules inside the generic book engine.

Conceptually:

```rust
trait BootstrapSynchronizer {
    fn synchronize(
        &self,
        snapshot: &L2Snapshot,
        updates: &VecDeque<L2Update>,
    ) -> BootstrapResult;
}
```

The result should identify:

```text
snapshot accepted
updates to discard
first update to apply
sequence state after bootstrap
```

---

## WebSocket Snapshot Strategy

For self-bootstrapping streams:

```text
Bitget
OKX
Bybit normal
```

the engine begins:

```text
SUBSCRIBED
    ↓
BOOTSTRAPPING
```

Updates received before the required snapshot must not be applied to an uninitialized book unless the source explicitly defines such behavior.

Generic behavior:

```text
waiting for snapshot
    ↓
receive control message
        → ignore/handle

receive update before snapshot
        → source-specific handling / remain uninitialized

receive snapshot
        → replace book
        → initialize sequence
        → VALID
```

Once valid:

```text
snapshot
    → replace

update
    → validate
    → apply
```

---

## REST + Buffered WebSocket Strategy

For:

```text
Binance
Gate.io
Bybit full-depth
```

the connection lifecycle should be:

```text
CONNECT
    ↓
SUBSCRIBE
    ↓
BUFFERING
    ↓
start REST request
    ↓
continue receiving WS
    ↓
REST response arrives
    ↓
SYNCHRONIZING
    ↓
validate snapshot against buffer
    ↓
load snapshot
    ↓
replay buffer
    ↓
VALID
```

The WebSocket receive loop must continue while REST is in flight.

Do not block WebSocket reception on the HTTP request.

Architecturally:

```text
                ┌→ REST snapshot task ───────┐
SUBSCRIBED ─────┤                             ├→ synchronize
                └→ WS receive + buffer ──────┘
```

This is one of the most important concurrency requirements in `marketforge-live`.

---

## Bootstrap Retry

If the REST snapshot cannot be synchronized with the current buffer:

```text
snapshot too old
buffer starts too late
sequence gap inside buffer
buffer overflow
snapshot malformed
```

do not force synchronization.

Instead:

```text
bootstrap attempt failed
    ↓
increment bootstrap generation
    ↓
clear obsolete state
    ↓
continue/restart buffering
    ↓
request new snapshot
```

A bounded number of immediate retries may be allowed before normal backoff applies.

---

## Sequence Gap During LIVE

Generic processing:

```text
receive update
    ↓
sequence valid?
    ├── yes
    │     ↓
    │   apply
    │     ↓
    │   advance sequence
    │
    └── no
          ↓
        INVALID
```

Once invalid:

```text
do not apply further updates to trusted book
```

The engine may immediately begin buffering those later updates for rebootstrap if the exchange uses REST+buffer recovery.

For native-snapshot exchanges, later updates should generally be ignored until a fresh snapshot establishes state.

---

## Recovery by Bootstrap Type

### Native WebSocket Snapshot

Used by:

```text
Bitget
OKX
Bybit normal
```

Recovery:

```text
gap
    ↓
INVALID
    ↓
resubscribe/reconnect as required
    ↓
wait for snapshot
    ↓
replace book
    ↓
VALID
```

### REST + Buffered Updates

Used by:

```text
Binance
Gate.io
Bybit full-depth
```

Recovery:

```text
gap
    ↓
INVALID
    ↓
BUFFERING
    ↓
REST snapshot
    ↓
synchronize
    ↓
replay
    ↓
VALID
```

---

## Connection Loss During Bootstrap

A disconnect invalidates the entire bootstrap attempt.

```text
BUFFERING generation=5
    ↓
disconnect
    ↓
discard generation=5 buffer
    ↓
increment connection generation
    ↓
reconnect
    ↓
new bootstrap generation
```

An outstanding REST response from the old bootstrap must be ignored when it eventually returns.

This is why connection/bootstrap generation IDs are required.

---

## Connection Loss During LIVE

For a stateful book:

```text
LIVE
    ↓
disconnect
    ↓
INVALID immediately
```

Do not wait to determine whether updates were actually missed.

After reconnect, perform the source's complete bootstrap procedure.

For trades:

```text
LIVE
    ↓
disconnect
    ↓
reconnect
    ↓
resubscribe
    ↓
LIVE
```

while recording the potential missing interval.

---

## Reconnect During Exchange Outage

A long exchange outage should not generate an infinite flood of:

```text
connect
fail
connect
fail
```

Use exponential backoff with jitter and a maximum delay.

Conceptually:

```text
1s
2s
4s
8s
16s
30s
30s
...
```

Exact values should be configuration rather than protocol constants.

After a sufficiently stable successful connection:

```text
reconnect_attempt = 0
```

---

## HTTP and WebSocket Independence

REST and WebSocket failures must be tracked separately.

Possible state:

```text
WebSocket = healthy
REST      = temporarily unavailable
Book      = buffering
```

For a REST-dependent bootstrap, the engine can continue buffering while REST retries within safe limits.

Likewise:

```text
REST polling = healthy
WebSocket    = disconnected
```

may still permit stateless REST snapshot acquisition.

Do not model an exchange as a single binary:

```text
online/offline
```

The individual transports can fail independently.

---

## Exchange Adapter Responsibilities

Each exchange adapter should define:

```text
WebSocket endpoint
REST endpoint
subscription message
subscription acknowledgement recognition
heartbeat behavior
trade message recognition
snapshot recognition
update recognition
control/error recognition
raw parsing
sequence extraction
snapshot synchronization
recovery requirements
timestamp extraction
```

The adapter should not own:

```text
PostgreSQL
cross-exchange merging
global scheduling
analytics
generic book storage
```

---

## Format Adapter Responsibilities

Where one exchange has multiple materially different protocols, behavior should be attached to the format rather than only the exchange.

Example Bybit:

```text
BYBIT-LIVE-B1
    native WS snapshot

BYBIT-LIVE-B3
    delta-only + REST bootstrap
```

Both are Bybit but require different state machines.

Gate.io:

```text
GATE-LIVE-B1
    Spot physical parser

GATE-LIVE-B2
    derivative physical parser
```

Both share the same range/bootstrap concept despite different level formats.

This argues for:

```text
Exchange
    ↓
Format
    ↓
Protocol capabilities
```

rather than:

```text
Exchange
    ↓
one hardcoded behavior
```

---

## Raw Message Handling

The receive path should distinguish:

```text
control
heartbeat
subscription acknowledgement
trade
book snapshot
book update
error
unknown
```

Conceptually:

```rust
enum IncomingMessage {
    Control(ControlMessage),
    Trade(Vec<Trade>),
    L2Snapshot(L2Snapshot),
    L2Update(L2Update),
    Error(SourceError),
    Unknown,
}
```

Unknown messages should not automatically crash the connection.

For a book channel, however, an unknown market-data payload that may represent a missed state transition must be treated conservatively.

---

## Parse Errors on Stateful Streams

If a book update cannot be parsed:

```text
raw update received
    ↓
parse failed
```

then the engine cannot prove continuity.

Therefore:

```text
book = INVALID
```

even if the WebSocket itself remains connected.

Recovery then follows the normal exchange-specific recovery strategy.

For stateless trades, one malformed trade can be recorded as a parse failure without necessarily invalidating the entire trade stream.

---

## Control Messages Must Not Advance Sequence

Messages such as:

```text
subscription acknowledgement
pong
connection metadata
server notice
```

must never mutate:

```text
book sequence
book timestamp
book state
```

unless the exchange protocol explicitly defines the control message as a state transition.

---

## Backpressure

The WebSocket receive loop must remain fast.

It should not directly perform slow operations such as:

```text
database transaction
cross-exchange merge
heavy analytics
REST request
```

on every message.

Preferred flow:

```text
WS receive
    ↓
parse
    ↓
normalize
    ↓
bounded channel
    ↓
state processor
    ↓
bounded channel
    ↓
sink
```

Every queue must be bounded.

If the runtime cannot keep up and a stateful L2 queue overflows:

```text
updates lost
    ↓
book INVALID
```

Do not silently drop book updates.

---

## Trades Under Backpressure

Dropping trades also creates an incomplete dataset.

If a bounded trade queue overflows:

```text
record integrity failure
```

and follow configured policy:

```text
stop pipeline
reconnect
mark data gap
```

Silent loss is unacceptable for MarketForge's research-data goals.

---

## Runtime Integrity Events

The live engine should generate internal integrity events for conditions such as:

```text
sequence_gap
snapshot_sync_failed
buffer_overflow
parse_failure
connection_loss
heartbeat_timeout
trade_duplicate
book_rebootstrap
REST_failure
late_event
sink_backpressure
```

These should be persistable separately from canonical market events.

They will later support the MarketForge analytics/quality layer.

---

## Minimum Per-Book Runtime State

Each active reconstructed book should retain at least:

```text
exchange
market
instrument
format
book_state
bootstrap_strategy
sequence_strategy

update_sequence_first
update_sequence_last
update_sequence_previous
cross_sequence

last_event_timestamp
last_arrival_timestamp

connection_generation
bootstrap_generation

bid state
ask state
```

REST-dependent books additionally need:

```text
buffered updates
snapshot request state
snapshot sequence metadata
```

---

## Minimum Per-Connection Runtime State

Each WebSocket connection should retain:

```text
exchange
endpoint
connection_state
connection_generation

connected_at
last_message_at
last_ping_at
last_pong_at

subscriptions
reconnect_attempt
```

Exchange-specific heartbeat state may add fields where required.

---

## Implementation Invariants

The following invariants should hold throughout the Rust implementation.

### Invariant 1

```text
A book marked VALID has a completed bootstrap.
```

### Invariant 2

```text
A sequence gap immediately invalidates the book.
```

### Invariant 3

```text
Updates from an old connection/bootstrap generation
cannot mutate the current book.
```

### Invariant 4

```text
A REST-dependent bootstrap receives WebSocket updates
while the REST request is in flight.
```

### Invariant 5

```text
Book updates are never silently dropped.
```

### Invariant 6

```text
A reconnect never assumes old and new book state are continuous.
```

### Invariant 7

```text
Native snapshots replace existing book state.
```

### Invariant 8

```text
Exchange sequence semantics are validated before applying an update.
```

### Invariant 9

```text
REST polling and WebSocket reconstruction are separate acquisition modes.
```

### Invariant 10

```text
Connection liveness is not inferred solely from market activity.
```

An illiquid instrument can legitimately produce no trades.

---

## Implementation Priority

The live engine should be implemented in increasing protocol complexity.

### Phase 1 — Native WebSocket Snapshot

Implement first:

```text
Bitget
OKX
Bybit normal
```

These require:

```text
connect
subscribe
snapshot
sequence validation
updates
gap detection
reconnect
new snapshot
```

This establishes the generic connection and book state machines.

### Phase 2 — REST + Buffered Bootstrap

Then implement:

```text
Gate.io
Binance
```

Add:

```text
concurrent REST + WS
update buffering
snapshot synchronization
buffer replay
bootstrap retry
```

### Phase 3 — Bybit Full Depth

Implement after the generic REST+buffer machinery exists.

It adds:

```text
REST/WS synchronization using u + seq
full-depth handling
Bybit-specific reset behavior
```

### Phase 4 — REST Polling

Add stateless:

```text
periodic L2 snapshots
```

using the same REST parsers.

### Phase 5 — Reconciliation

Add optional:

```text
maintained WS book
        ↔
REST snapshot
```

validation.

### Phase 6 — Cross-Exchange Merge

Only after individual exchange streams are reliable:

```text
canonical events
    ↓
reorder buffer
    ↓
cross-exchange merged stream
```

---

## Final Protocol Matrix

| Exchange | Book Source | Initial State | Sequence | Gap Recovery |
|---|---|---|---|---|
| Binance | WS + REST | REST snapshot + buffered WS | `U/u`, optional `pu` | REST rebootstrap |
| Bybit Normal | WS | WS snapshot | `u` + cross `seq` | New WS snapshot |
| Bybit Full | WS + REST | REST snapshot + buffered WS | `u` + cross `seq` | REST rebootstrap |
| Bitget | WS | WS snapshot | `pseq → seq` | New WS snapshot |
| OKX | WS | WS snapshot | `prevSeqId → seqId` | New WS snapshot |
| Gate.io | WS + REST | REST snapshot + buffered WS | `U → u` range | REST rebootstrap |

REST snapshot polling remains available independently where supported.

The central runtime distinction is therefore:

```text
SELF-BOOTSTRAPPING
────────────────────────
Bitget
OKX
Bybit normal


REST-SYNCHRONIZED
────────────────────────
Binance
Gate.io
Bybit full-depth
```

That distinction should drive `marketforge-live` connection, bootstrap, reconnection, and recovery logic.