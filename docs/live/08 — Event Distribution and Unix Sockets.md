# 08 — Event Distribution and Unix Sockets

## 1. Purpose

The Event Distribution subsystem publishes accepted canonical market-data events to local consumers through Unix domain sockets.

It provides a reusable, high-throughput communication layer between MarketForge Live and downstream applications without requiring those applications to participate in exchange connectivity, normalization, or integrity validation.

The subsystem is responsible for:

- Distributing canonical trades and L2 events.
- Accepting local subscriber connections.
- Managing subscriber identities and lifecycles.
- Maintaining stream-to-subscriber routing.
- Enforcing subscriber admission limits.
- Preserving required per-stream event ordering.
- Providing bounded outbound buffering.
- Detecting slow or disconnected consumers.
- Reporting delivery failures explicitly.
- Supporting subscriber reconnection.
- Coordinating replay through the existing retention infrastructure.
- Exposing publisher health and delivery metrics.

The Publisher must remain independent of exchange-specific protocol implementations.

### 1.1 Primary Objectives

MarketForge Live must support multiple independent local consumers requesting different combinations of canonical market-data streams.

Examples include:

- Trading applications.
- Quantitative research processes.
- Feature-engineering pipelines.
- Imbalance-bar and run-bar generators.
- Market-microstructure analysis.
- Local monitoring processes.
- Other MarketForge components.

A consumer should be able to subscribe to canonical events without knowing how the exchange's native WebSocket protocol works.

### 1.2 Transport Decision

The initial IPC transport is:

    Unix Domain Sockets

Shared memory is deferred.

The architecture should permit additional transport implementations later without changing canonical event contracts.

The initial Publisher must not depend on:

- Redis.
- Kafka.
- RabbitMQ.
- ZeroMQ.
- External message brokers.
- HTTP streaming.
- Shared-memory ring buffers.

Unix domain sockets provide local interprocess communication using operating-system-managed socket buffers and standard asynchronous I/O.

### 1.2.1 Separation from the Control API

MarketForge Live exposes two independent Unix-domain socket interfaces:

| Interface | Protocol | Responsibility | Authoritative Document |
|---|---|---|---|
| Market-Data Socket | Versioned binary event protocol | Canonical event distribution, subscriptions, and supported replay delivery | Document 08 |
| Control Socket | Versioned, length-prefixed JSON | Runtime management, configuration, operational commands, and status queries | Document 13 |

These interfaces have separate application protocols and ownership boundaries.

The Event Distribution subsystem owns the market-data publication interface.
The Control API subsystem owns the management interface.

Control API messages must not be interpreted as canonical market-data frames.
Canonical market-data frames must not be interpreted as Control API messages.

Clients must use the appropriate socket and protocol for each operation.

The Ratatui interface and Python orchestration communicate with the daemon
through the Control API defined in Document 13.

Market-data consumers receive canonical events through the binary publication
interface defined in this document.

The two interfaces may reuse generic transport or framing utilities where
compatible, but they must not share incompatible application-level message
contracts.

The optional Prometheus HTTP exposition endpoint is separate from both
Unix-domain sockets and does not provide runtime management operations.

### 1.3 Supported Event Types

The initial Publisher distributes:

    Trade
    L2Snapshot
    L2Update

These events are produced by the normalization and integrity subsystems.

The Publisher must not reinterpret exchange-native messages.

Additional event types may be introduced through versioned contracts.

### 1.4 Core Principle

An event is eligible for publication only after the responsible processing subsystem accepts it.

For trades:

    Native Execution
          |
          v
    Trade Normalization
          |
          v
    Trade Integrity Validation
          |
          v
    Canonical Trade Accepted
          |
          v
    Publisher

For L2:

    Native L2 Message
          |
          v
    L2 Normalization
          |
          v
    Sequence Validation
          |
          v
    Atomic BookStore Commit
          |
          v
    Canonical L2 Event Accepted
          |
          v
    Publisher

The Publisher does not decide whether a native execution is economically valid or an order-book sequence is synchronized.

It trusts the accepted canonical-event contract while independently enforcing distribution and delivery guarantees.

---

## 2. Architectural Boundaries

The Event Distribution subsystem is downstream of canonical market-data processing.

It owns subscriber-facing transport and delivery state.

It does not own exchange connections or market-data reconstruction.

### 2.1 Component Responsibilities

| Component | Responsibility |
|---|---|
| Stream Registry | Logical source-stream identity and lifecycle |
| Trade Processor | Accepted canonical trade events |
| BookStore | Accepted L2 state transitions |
| Publisher | Canonical event routing and distribution |
| Subscriber Registry | Subscriber identities, subscriptions, and delivery state |
| Unix Socket Transport | Local socket connections and frame transmission |
| Live Retention Manager | Temporary event retention and replay availability |
| Recording Manager | Optional PostgreSQL market-data persistence |
| Metrics Registry | Distribution and transport measurements |
| TUI / API | Inspection and control-plane commands |
| Recovery Coordinator | Cross-component failure coordination |

The Publisher must not independently create exchange-native subscriptions.

Requests requiring new source streams are coordinated through the Stream Registry and Subscription Manager.

### 2.2 Data Distribution Architecture

    Exchange Connections
             |
             v
    Canonical Processing
             |
       +-----+-----+
       |           |
       v           v
    Trades      L2 BookStore
       |           |
       +-----+-----+
             |
             v
      Canonical Event Bus
             |
             v
          Publisher
             |
       +-----+-----+----------------+
       |           |                |
       v           v                v
    Subscriber A Subscriber B   Subscriber C
       |           |                |
       v           v                v
    Unix Socket  Unix Socket     Unix Socket
       |           |                |
       v           v                v
    Trading App   Feature Engine   Research App

The diagram represents logical distribution.

One canonical source stream may feed multiple subscribers.

A subscriber may consume events from multiple source streams.

### 2.3 Retention and Recording Independence

Canonical-event distribution, temporary retention, and optional recording serve different purposes.

Conceptually:

    Accepted Canonical Events
               |
         +-----+-----+
         |     |     |
         v     v     v
    Publisher Retention Recording
         |     |     |
         v     v     v
    Subscribers Parquet PostgreSQL

The exact internal fan-out implementation may differ.

However, these destinations must retain independent ownership and completion semantics.

A successful Unix socket write does not imply successful PostgreSQL persistence.

Likewise, successful PostgreSQL recording does not prove that a subscriber received an event.

### 2.4 Design Constraints

- Rust-only Publisher implementation.
- Unix domain sockets as the initial IPC transport.
- No exchange-specific logic inside the Publisher.
- No Python dependency during publication.
- No PostgreSQL dependency on the publication hot path.
- No direct TUI dependencies.
- No unbounded subscriber queues.
- No silent accepted-event dropping.
- No assumption that a successful socket write proves application consumption.
- No implicit global ordering across independent source streams.
- No automatic replay guarantee beyond available retention.
- No subscriber access to mutable BookStore internals.
- No duplicate native source subscriptions merely because multiple consumers request the same source.

The Publisher must remain independently testable using local IPC clients and mock canonical-event producers.

---

## 3. Event Distribution Model

The Publisher distributes events using logical source-stream identities.

A physical Unix socket connection is a subscriber transport session, not a market-data source.

### 3.1 Source Streams

Every canonical event belongs to a logical source stream.

Conceptually:

    StreamId

Examples:

    Binance BTCUSDT Trades

    Binance BTCUSDT L2

    Bybit BTCUSDT Trades

Each source stream is managed by the Stream Registry.

The Publisher references these identities without owning their exchange subscriptions.

### 3.2 Subscriber Sessions

A subscriber session represents one connected local consumer.

Conceptually:

    SubscriberSessionId

A subscriber may request several source streams.

Example:

    Subscriber A
         |
         +-- Binance BTCUSDT Trades
         |
         +-- Binance BTCUSDT L2
         |
         +-- Bybit BTCUSDT Trades

The Publisher manages routing between accepted canonical events and interested subscriber sessions.

### 3.3 Many-to-Many Distribution

The relationship between source streams and subscribers is many-to-many.

    Source Stream A
          |
          +------------+
          |            |
          v            v
    Subscriber 1   Subscriber 2

    Source Stream B
          |
          +------------+
          |            |
          v            v
    Subscriber 1   Subscriber 3

One source stream must not create a new exchange WebSocket subscription for every downstream consumer.

Source subscription deduplication remains owned by the Stream Registry and Subscription Manager.

### 3.4 Distribution Identity

Canonical events are routed by:

    StreamId

Additional event metadata may include:

    InstrumentId
    EventType
    StreamCursor
    BookGeneration
    ConfigurationRevision
    MetadataRevision

The Publisher must not infer routing identity from native exchange symbols.

### 3.5 Event Ordering

The Publisher must preserve accepted-event ordering within each logical source stream.

Conceptually:

    Stream A:

        Cursor 100
        Cursor 101
        Cursor 102

A subscriber receiving Stream A must observe events in that publication order.

The Publisher must not promise a global chronological order between unrelated source streams.

For example:

    Stream A Cursor 100

and:

    Stream B Cursor 500

cannot be compared as though they belonged to one shared ordering domain.

### 3.6 Trade and L2 Ordering

Trades and L2 books have different integrity requirements.

**Trades**

Publication order follows the accepted source-stream cursor.

Recovered executions may have earlier event timestamps than already published live executions.

**L2**

Published deltas must follow the committed BookStore transition order.

A subscriber requiring synchronized book reconstruction must receive a valid initialization snapshot before deltas from that book generation.

The Publisher must preserve these differences through its delivery contracts.

### 3.7 Delivery Boundaries

The Publisher distinguishes:

    Event Accepted by Producer

    Event Admitted to Publisher

    Event Queued for Subscriber

    Event Written to Unix Socket

    Event Acknowledged by Subscriber

Not every delivery mode requires application-level acknowledgements.

Nevertheless, the implementation must not treat these boundaries as equivalent.

In particular:

    Successful Socket Write
        !=
    Subscriber Application Processed Event

---

## 4. Publisher Domain Models

The Publisher maintains a small set of runtime entities describing subscriber connections, routing, and delivery progress.

These entities must remain independent of exchange-native transport models.

### 4.1 PublisherRuntime

Conceptual representation:

    PublisherRuntime {
        publisher_id
        runtime_instance_id

        lifecycle_state

        subscriber_registry
        routing_registry
        transport_listener

        admission_policy
        delivery_policy

        configuration_revision
    }

The actual Rust implementation may divide these responsibilities into independent modules.

### 4.2 PublisherState

Supported lifecycle states:

    Created
    Starting
    Listening
    Degraded
    Stopping
    Stopped
    Failed

| State | Meaning |
|---|---|
| Created | Publisher initialized but not started |
| Starting | IPC listener and internal routing being prepared |
| Listening | Ready to accept subscriber connections |
| Degraded | One or more required distribution guarantees impaired |
| Stopping | Graceful shutdown in progress |
| Stopped | Listener and sessions terminated |
| Failed | Nonrecoverable publisher failure |

Subscriber failures must not automatically transition the entire Publisher to `Failed`.

### 4.3 SubscriberDescriptor

Immutable subscriber identity and admission information.

| Field | Purpose |
|---|---|
| subscriber_id | Logical subscriber identity |
| session_id | Current transport session |
| connected_at | Connection establishment time |
| transport_kind | Unix domain socket |
| peer_identity | Validated local peer information |
| protocol_version | Negotiated IPC protocol |
| requested_capabilities | Requested delivery capabilities |
| configuration_revision | Effective subscriber configuration |

A subscriber's transport session may change after reconnection.

Its persistent logical identity, if supported, must be represented separately.

### 4.4 SubscriberRuntime

Mutable subscriber state.

Conceptually:

    SubscriberRuntime {
        subscriber_id
        session_id
        session_generation

        connection_state
        subscribed_streams

        delivery_mode
        queue_state

        last_queued_cursor
        last_written_cursor
        last_acknowledged_cursor

        last_error
        connected_at
        state_changed_at
    }

Cursor tracking is maintained per subscribed source stream.

A single scalar cursor is insufficient when one subscriber consumes multiple independent source streams.

### 4.5 SubscriberState

Supported states:

    Connected
    Negotiating
    Ready
    Subscribed
    Degraded
    Disconnecting
    Disconnected

| State | Meaning |
|---|---|
| Connected | Unix socket established |
| Negotiating | IPC version and capabilities being negotiated |
| Ready | Subscriber authenticated or admitted and ready for commands |
| Subscribed | At least one source-stream subscription active |
| Degraded | Delivery guarantees impaired |
| Disconnecting | Session closure underway |
| Disconnected | Transport session terminated |

The state of one subscriber must remain independent of unrelated subscriber sessions.

### 4.6 SubscriberSubscription

A subscriber subscription describes interest in one logical source stream.

Conceptually:

    SubscriberSubscription {
        subscriber_id
        stream_id

        delivery_mode
        starting_position

        state

        last_queued_cursor
        last_written_cursor
        last_acknowledged_cursor
    }

The Publisher owns this downstream relationship.

It does not own the native exchange subscription.

### 4.7 Subscription Identity

A downstream subscription is identified by:

    SubscriberSessionId
    StreamId

The same subscriber must not accidentally register duplicate delivery subscriptions to the same source stream.

Repeated compatible subscription requests should be idempotent.

Conflicting requests must produce explicit errors or controlled replacements.

### 4.8 Session Generations

Every subscriber transport reconnection receives a new session generation.

Conceptually:

    SubscriberId = 10

    SessionGeneration = 1
        Initial connection

    SessionGeneration = 2
        Reconnected session

Delayed writes, acknowledgements, and disconnection events from an obsolete session must not modify current subscriber state.

The session generation must not be confused with the exchange connection generation or source subscription generation.

### 4.9 Subscriber Capacity

The Publisher enforces the maximum subscriber count defined by the Configuration System.

The initial hot-editable setting:

    maximum_subscribers

applies to future subscriber admission.

Changing this limit must not automatically terminate existing accepted subscribers unless an explicit policy requires it.

The Publisher may additionally enforce:

    MaximumSubscriptionsPerSubscriber

    MaximumOutboundQueueBytes

    MaximumOutboundQueueMessages

    MaximumFrameSize

These limits must be defined through configuration and enforced before resource exhaustion.

---

## 5. Unix Domain Socket Server

The Publisher exposes a local IPC server using Unix domain sockets.

The initial transport is a Unix stream socket:

    AF_UNIX
    SOCK_STREAM

The server must support multiple concurrent subscriber sessions through asynchronous Rust I/O.

The Publisher owns the socket listener and subscriber-facing connection lifecycle.

### 5.1 Socket Responsibilities

The Unix Socket Server must:

- Create and bind the configured socket endpoint.
- Validate the socket directory and filesystem permissions.
- Accept local client connections.
- Enforce subscriber admission limits.
- Initialize subscriber transport sessions.
- Negotiate IPC protocol compatibility.
- Read subscriber control messages.
- Write canonical event frames.
- Detect subscriber disconnections.
- Apply bounded transport backpressure.
- Release resources after session termination.
- Remove owned socket filesystem entries during shutdown.

The server must not:

- Establish exchange WebSocket connections.
- Decode exchange-native market data.
- Normalize canonical events.
- Modify BookStore state.
- Perform historical trade recovery.
- Write market data directly to PostgreSQL.

### 5.2 Socket Endpoint

The socket path is supplied by the Configuration System.

Conceptually:

    PublisherConfig {
        socket_path
        socket_permissions
        maximum_subscribers
        maximum_frame_size
        maximum_session_queue_bytes
    }

The exact configuration keys are defined in document 03.

An illustrative endpoint is:

    data/.runtime/live/marketforge.sock

The final location must be configurable.

The socket should reside in a runtime directory controlled by the MarketForge process.

### 5.3 Socket Path Ownership

Unix socket endpoints appear as filesystem entries.

The server must not blindly delete an existing path.

Before binding:

1. Inspect the configured path.
2. Determine whether it already exists.
3. Verify whether it is a Unix socket.
4. Determine whether an active compatible server owns it.
5. Apply the configured stale-endpoint recovery policy.
6. Bind only when ownership is safe.

An existing regular file or unrelated socket must not be removed automatically.

### 5.4 Stale Socket Recovery

A process crash may leave a socket filesystem entry behind.

The startup procedure must distinguish:

    Stale Socket Entry

from:

    Active Publisher Endpoint

A stale endpoint may be removed only after verifying that it does not belong to an active server and that the runtime has authority over the path.

A connection attempt alone may not be sufficient to establish safe ownership in every failure condition.

The implementation should use an appropriate process-instance lock or equivalent ownership mechanism.

### 5.5 Filesystem Permissions

The socket directory and socket entry must use restrictive permissions.

The default security policy should allow only the intended local user or explicitly authorized group to connect.

The server must not assume that filesystem permissions alone provide sufficient authorization for every deployment.

Where supported, local peer credentials should also be validated.

### 5.6 Socket Lifecycle

The listener follows:

    Created
       |
       v
    ValidatingEndpoint
       |
       v
    Binding
       |
       v
    Listening
       |
       v
    Stopping
       |
       v
    Closed

A failed bind operation must produce a structured error.

The server must not silently select another filesystem path unless the configuration explicitly permits it.

### 5.7 Listener Failure

Possible listener failures include:

- Invalid socket path.
- Permission denied.
- Existing active endpoint.
- Filesystem unavailable.
- File descriptor exhaustion.
- Resource admission failure.
- Unexpected listener termination.

The Publisher reports these failures through the runtime failure interface.

The Recovery Coordinator determines system-level escalation.

---

## 6. Subscriber Transport Sessions

Each accepted Unix socket connection creates a transport session.

A transport session is distinct from a persistent logical subscriber identity.

### 6.1 Session Identity

Conceptually:

    SubscriberTransportSession {
        session_id
        session_generation
        runtime_instance_id
        connected_at
        peer_identity
        protocol_version
        state
    }

The session identity must be unique within the current runtime instance.

A reconnecting subscriber receives a new transport session generation.

### 6.2 Session Task Architecture

Each subscriber session contains:

- A control-message reader.
- An event writer.
- A bounded outbound event queue.
- A session supervisor.
- Cancellation and deadline state.

Conceptually:

    Unix Socket Connection
              |
       +------+------+
       |             |
       v             v
    Reader         Writer
       |             ^
       v             |
    Control        Bounded
    Messages       Event Queue
       |             ^
       v             |
    Subscriber       |
    Registry --------+
       |
       v
    Session Supervisor

The reader and writer operate independently but belong to the same session lifecycle.

### 6.3 Reader Responsibilities

The session reader must:

- Receive framed control messages.
- Validate frame boundaries.
- Decode the IPC control protocol.
- Enforce negotiated protocol versions.
- Reject malformed commands.
- Forward valid commands to the Subscriber Registry.
- Detect remote closure.
- Enforce command-rate and resource limits.

It must not perform exchange subscription operations directly.

### 6.4 Writer Responsibilities

The session writer must:

- Serialize outbound IPC frames.
- Preserve required event ordering.
- Enforce frame-size limits.
- Respect session-generation fencing.
- Handle partial socket writes.
- Apply bounded queue backpressure.
- Detect broken pipes and closed connections.
- Report write failures.

Only one writer task may write to a subscriber's Unix stream socket.

### 6.5 Partial Reads and Writes

Unix stream sockets provide an ordered byte stream.

They do not preserve application-message boundaries.

Therefore, one application frame may be delivered through several socket reads.

Likewise, one socket read may contain multiple application frames.

The IPC implementation must not assume:

    One read = One message

or:

    One write = One message

Framing is mandatory.

### 6.6 Session Cancellation

When a subscriber disconnects or is removed:

- Fence its current session generation.
- Stop accepting new commands.
- Stop admitting outbound events.
- Cancel pending transport operations.
- Release queued event references.
- Remove session-specific routing assignments.
- Report final delivery boundaries.

Cancellation must be idempotent.

An obsolete writer completion must not modify a newer subscriber session.

---

## 7. IPC Message Framing

MarketForge requires an explicit framing protocol over Unix stream sockets.

The framing layer is independent of canonical event serialization.

### 7.1 Framing Objectives

The framing mechanism must:

- Delimit application messages.
- Support partial reads and writes.
- Reject oversized frames.
- Preserve message boundaries.
- Support protocol versioning.
- Permit multiple frame types.
- Avoid unnecessary copies.
- Detect malformed framing.
- Prevent unbounded allocation from malicious or corrupted length fields.

### 7.2 Initial Framing Strategy

The initial recommended strategy is:

    Length-Prefixed Frames

Each frame contains a fixed-size length prefix followed by a serialized IPC message.

Conceptually:

    +----------------------+
    | Payload Length       |
    +----------------------+
    | Serialized Message   |
    |                      |
    +----------------------+

The initial framing contract uses:

    Length Prefix: u32

    Byte Order: Network byte order (big-endian)

    Length Meaning: Number of payload bytes

The length prefix does not include its own four bytes.

### 7.3 Frame Representation

Conceptually:

    Frame {
        payload_length: u32
        payload: [u8; payload_length]
    }

The payload contains a versioned IPC message envelope.

The exact binary serialization of the envelope is defined separately from the framing layer.

### 7.4 Frame Size Validation

Before allocating the payload buffer:

    Read Length Prefix
           |
           v
    Validate Length
           |
       +---+---+
       |       |
       v       v
     Valid   Invalid
       |       |
       v       v
    Allocate Reject
       |
       v
    Read Exact Payload

The declared frame length must not exceed the configured maximum.

Arithmetic involving frame lengths must use checked operations.

A malformed length field must not trigger an unbounded allocation.

### 7.5 Zero-Length Frames

The initial protocol should reject zero-length application frames.

Heartbeat and control messages must use explicit message types rather than empty payloads.

### 7.6 Incomplete Frames

If the connection closes before the full declared payload is received:

    IncompleteFrame

The session must report a framing error or connection termination.

The incomplete payload must not be decoded as a valid message.

### 7.7 Frame Ordering

Frames are transmitted in session write order.

The session writer must not interleave bytes from different frames.

This is particularly important when event frames and control responses share the same socket.

### 7.8 Framing and Serialization Separation

The framing layer determines where messages begin and end.

The serialization layer determines what those messages contain.

Conceptually:

    Canonical Event
          |
          v
    IPC Serialization
          |
          v
    Length-Prefixed Frame
          |
          v
    Unix Socket Writer

The framing layer must not inspect exchange-native trade or L2 fields.

---

## 8. IPC Protocol Envelope

Every IPC message must identify its type and protocol version.

The initial design uses one shared envelope for control messages, data events, acknowledgements, and errors.

### 8.1 MessageEnvelope

Conceptual representation:

    MessageEnvelope {
        protocol_version
        message_type
        request_id
        session_id
        payload
    }

Not every message requires every field.

For example, canonical event frames may not require a client request identifier.

### 8.2 Message Types

The initial protocol supports:

    Hello
    HelloAck

    Subscribe
    SubscribeAck

    Unsubscribe
    UnsubscribeAck

    Event

    DeliveryAck

    Error

    Close

Additional message types may be introduced through versioned protocol evolution.

### 8.3 Control and Data Messages

**Control messages** manage subscriber sessions.

Examples:

    Hello
    Subscribe
    Unsubscribe
    DeliveryAck
    Close

**Data messages** carry accepted canonical market-data events.

Example:

    Event

The Publisher must not confuse IPC subscription commands with exchange-native subscription commands.

### 8.4 Request Correlation

Client control requests receive a request identifier.

Conceptually:

    RequestId

The server includes the corresponding identifier in its response.

This permits asynchronous control operations without depending on response arrival order.

A request identifier is scoped to the subscriber session unless a broader identity contract is explicitly defined.

### 8.5 Error Responses

Protocol errors must use structured error messages.

Conceptual fields:

    Error {
        request_id
        error_code
        message
        retryable
    }

Error messages should remain concise and machine-readable.

They must not expose internal credentials or sensitive configuration values.

### 8.6 Unknown Message Types

An unknown message type must not be interpreted as an ordinary canonical event.

The server must follow the negotiated protocol's compatibility rules.

Unsupported required message types produce an explicit protocol error.

Unknown optional fields may be ignored only when the protocol version defines forward-compatible behavior.

---

## 9. IPC Protocol Negotiation

Subscribers must establish protocol compatibility before receiving canonical events.

Protocol negotiation prevents incompatible clients from interpreting event bytes incorrectly.

### 9.1 Handshake Sequence

    Subscriber Connects
             |
             v
    Unix Socket Accepted
             |
             v
    Subscriber Sends Hello
             |
             v
    Validate Protocol Version
             |
             v
    Validate Requested Capabilities
             |
             v
    Validate Peer Identity
             |
             v
    Server Sends HelloAck
             |
             v
    Subscriber Ready

No canonical market-data events may be delivered before successful negotiation.

### 9.2 Hello Message

Conceptual structure:

    Hello {
        protocol_version
        client_name
        requested_capabilities
    }

Additional optional fields may include:

    client_instance_id
    supported_serializations
    supported_delivery_modes

The exact wire representation is defined by the finalized IPC schema.

### 9.3 HelloAck Message

Conceptual structure:

    HelloAck {
        negotiated_protocol_version
        session_id
        runtime_instance_id
        supported_capabilities
        limits
    }

The acknowledgement confirms the session's negotiated capabilities.

### 9.4 Version Compatibility

The initial protocol should use explicit major and minor versions.

Conceptually:

    ProtocolVersion {
        major
        minor
    }

Rules:

- Incompatible major versions are rejected.
- Compatible minor versions may negotiate a shared feature set.
- Unknown mandatory fields are rejected.
- Optional fields follow explicit forward-compatibility rules.

The server must not silently reinterpret an incompatible canonical schema.

### 9.5 Serialization Negotiation

The framing protocol is independent of the serialization format.

Possible serialization strategies include:

    BinarySchema
    JSON

The initial recommended production design is a versioned binary schema for canonical event frames.

JSON may be supported for control-plane debugging or development clients.

The final serialization choice must remain compatible with the canonical contracts defined in document 02.

### 9.6 Capability Negotiation

Capabilities may include:

    TradeEvents
    L2Snapshots
    L2Updates
    CursorReplay
    DeliveryAcknowledgements
    RecoveryNotifications

A client may request only capabilities supported by the server.

Unsupported required capabilities cause negotiation failure.

### 9.7 Negotiation Timeout

The server must enforce a bounded handshake timeout.

A client that opens a socket but never sends `Hello` must not occupy a subscriber slot indefinitely.

Negotiation failure releases session resources.

---

## 10. Subscriber Admission

The Publisher enforces subscriber limits and validates local client access.

Admission occurs before the session is allowed to subscribe to canonical streams.

### 10.1 Admission Requirements

A subscriber may be admitted only when:

- The Unix socket connection is valid.
- The IPC protocol version is supported.
- Required capabilities are available.
- Peer identity satisfies the configured access policy.
- Subscriber capacity is available.
- Session resource reservations succeed.
- The Publisher is accepting new subscribers.

Admission must produce an explicit success or rejection outcome.

### 10.2 Admission Limits

Relevant limits include:

    MaximumSubscribers

    MaximumSubscriptionsPerSubscriber

    MaximumSessionQueueBytes

    MaximumSessionQueueMessages

    MaximumFrameSize

    MaximumPendingControlRequests

These limits belong to the Configuration System.

The Publisher must enforce them atomically under concurrent connection attempts.

### 10.3 Subscriber Reservations

Admission should reserve capacity before completing the session handshake.

Conceptually:

    Incoming Connection
            |
            v
    Check Subscriber Capacity
            |
            v
    Reserve Session Slot
            |
            v
    Negotiate Protocol
            |
       +----+----+
       |         |
       v         v
    Success    Failure
       |         |
       v         v
    Commit    Release
    Session   Reservation

Failed negotiation must not leak subscriber capacity.

### 10.4 Peer Identity

On supported Unix platforms, the server may inspect operating-system-provided peer credentials.

Relevant information may include:

    UserId
    GroupId
    ProcessId

The exact available fields are platform-dependent.

Peer credentials must be obtained from the operating system, not trusted from client-supplied JSON or binary fields.

### 10.5 Access Policy

Supported conceptual policies:

    SameUser
    AllowedGroup
    ExplicitAllowlist

The initial recommended policy is:

    SameUser

This limits IPC access to processes running under the same operating-system user as MarketForge Live.

More permissive policies require explicit configuration.

### 10.6 Subscriber Limit Changes

The existing hot-editable setting:

    maximum_subscribers

applies to future admission decisions.

Reducing the limit below the current active subscriber count must not automatically disconnect existing sessions unless an explicit eviction policy is configured.

### 10.7 Admission Failures

Possible outcomes include:

    CapacityExceeded
    UnsupportedProtocol
    UnsupportedCapability
    UnauthorizedPeer
    HandshakeTimeout
    ResourceUnavailable
    PublisherStopping

Failures must be visible through operational metrics and diagnostics.

The server must not silently accept an incompatible or unauthorized subscriber.

---
## 11. Subscriber Registry

The Subscriber Registry maintains authoritative runtime information about connected IPC consumers and their subscriptions to canonical source streams.

It is part of the Event Distribution subsystem.

It must not be confused with the Stream Registry defined in **05 — Stream Registry and Subscriptions**.

The two registries manage different resources.

### 11.1 Registry Responsibilities

The Subscriber Registry must:

- Register admitted subscriber sessions.
- Maintain subscriber identities.
- Track requested source streams.
- Manage subscriber subscription lifecycles.
- Maintain subscriber-to-stream relationships.
- Enforce subscriber-level resource limits.
- Coordinate subscription activation and removal.
- Track per-stream delivery progress.
- Reject obsolete session operations.
- Support subscriber reconnection.
- Expose subscriber state and diagnostics.

It does not establish exchange-native subscriptions directly.

### 11.2 Registry Ownership

| Resource | Owner |
|---|---|
| Logical source stream | Stream Registry |
| Exchange-native subscription | Subscription Manager |
| Physical exchange connection | Connection Manager |
| IPC subscriber identity | Subscriber Registry |
| Unix socket session | Unix Socket Server |
| Downstream subscription | Subscriber Registry |
| Stream-to-subscriber routing | Publisher |
| Subscriber delivery queue | Publisher |
| Canonical event retention | Live Retention Manager |

Each resource must have one authoritative owner.

### 11.3 SubscriberRegistry

Conceptual representation:

    SubscriberRegistry {
        subscribers
        sessions
        subscriptions
        routing_index
        configuration_revision
    }

The exact Rust representation is an implementation detail.

The registry must support concurrent access without introducing a global lock into every market-data delivery operation.

### 11.4 Subscriber Identity

MarketForge distinguishes:

    SubscriberId

from:

    SubscriberSessionId

`SubscriberId` represents a logical consumer identity.

`SubscriberSessionId` represents one physical Unix socket connection.

A client may reconnect and receive a new session while retaining its logical identity when persistent subscriber identity is supported.

For anonymous or ephemeral clients, subscriber identity may be scoped to the session.

### 11.5 Subscriber Persistence

The initial implementation does not require persistent subscriber registrations.

An ordinary disconnected subscriber loses its active IPC subscriptions.

Persistent subscriber identities and durable delivery checkpoints are optional future capabilities.

A reconnecting client may restore subscriptions by explicitly requesting them again.

The Publisher must not assume that the client's previous session state survived reconnection.

### 11.6 Session Generations

Each subscriber session receives a generation.

Conceptually:

    Subscriber 12
         |
         +-- Session Generation 1
         |
         +-- Session Generation 2
         |
         +-- Session Generation 3

Only the current authoritative session generation may modify that subscriber's active delivery state.

Late commands and acknowledgements from obsolete sessions must be rejected.

### 11.7 Registry Concurrency

Subscriber registration and removal must be serialized for the affected subscriber identity.

Unrelated subscribers may be managed concurrently.

The implementation must avoid holding global registry locks while:

- Writing to Unix sockets.
- Waiting for subscriber acknowledgements.
- Waiting for source-stream activation.
- Performing replay.
- Blocking on outbound queue capacity.

---

## 12. Downstream Stream Subscriptions

A downstream subscription represents one subscriber's interest in one logical source stream.

It is not equivalent to an exchange-native WebSocket subscription.

### 12.1 Subscription Identity

A downstream subscription is identified by:

    SubscriberSessionId
    StreamId

Conceptually:

    SubscriberSubscription {
        subscriber_session_id
        stream_id
        delivery_policy
        starting_position
        state
        generation
    }

Multiple subscribers may reference the same `StreamId`.

### 12.2 Subscription Operations

Supported operations:

    Subscribe
    Unsubscribe
    ReplaceSubscription
    ListSubscriptions

Subscription requests are processed asynchronously.

Each request produces an explicit result.

### 12.3 Subscription States

Supported states:

    Requested
    Validating
    PendingSource
    Preparing
    Active
    Degraded
    Unsubscribing
    Removed
    Failed

| State | Meaning |
|---|---|
| Requested | Subscriber request received |
| Validating | Stream identity and capabilities being checked |
| PendingSource | Waiting for required source-stream readiness |
| Preparing | Establishing routing and delivery boundaries |
| Active | Events may be delivered according to the selected policy |
| Degraded | Delivery requirements are impaired |
| Unsubscribing | Subscription removal underway |
| Removed | Downstream relationship released |
| Failed | Subscription cannot be established |

A downstream subscription may remain pending while the source stream is initializing.

For L2, readiness may require an authoritative book snapshot.

### 12.4 Subscribe Request

Conceptual request:

    Subscribe {
        request_id
        stream_id
        delivery_mode
        starting_position
    }

The request must identify the desired logical source stream.

The subscriber must not be required to know the exchange's native channel name or WebSocket subscription syntax.

### 12.5 Source Identification

Subscribers should normally select streams by `StreamId`.

The IPC protocol may additionally support a structured stream selector:

    Exchange
    Market
    InstrumentId
    Channel
    ChannelParameters

Such selectors must be resolved through the Stream Registry.

The Publisher must not implement its own exchange-specific instrument discovery.

### 12.6 Source Availability

Before activating a downstream subscription, the Publisher checks whether the source stream is:

    Registered
    Active
    Synchronizing
    Inactive
    Failed
    Removed

The precise readiness requirements depend on the delivery mode.

For example, a subscriber requesting synchronized L2 state cannot begin ordinary delta delivery before receiving a valid initialization snapshot.

### 12.7 Source Activation Policy

The initial design should distinguish two operations:

    SubscribeToExistingSource

    RequestSourceActivation

The first creates only a downstream subscription.

The second requests activation through the Stream Registry.

A subscriber must not implicitly create arbitrary exchange-native subscriptions unless the configured admission and authorization policy explicitly permits it.

The initial recommended policy is:

    SubscribeToExistingSource

Source creation and activation remain control-plane operations.

### 12.8 Duplicate Subscriber Requests

Repeated compatible requests must be idempotent.

Example:

    Subscribe(StreamId=15)

    Subscribe(StreamId=15)

The second request must not create a second delivery route for the same subscriber session.

It may return the existing downstream subscription.

### 12.9 Conflicting Requests

If a subscriber requests the same stream with incompatible delivery parameters, the Publisher must:

- Reject the conflicting request.
- Or perform an explicit controlled replacement.

It must not silently change the active subscription's delivery guarantees.

### 12.10 Unsubscription

Unsubscription removes one downstream subscription.

Conceptually:

    Subscriber
         |
         +-- Stream A
         |
         +-- Stream B

Removing Stream A must not affect Stream B.

The operation must release:

- Routing membership.
- Stream-specific queue state.
- Delivery cursor tracking.
- Replay state.
- Subscription resource reservations.

### 12.11 Unsubscription and Source Lifetime

Removing a downstream subscription does not automatically remove the underlying source stream.

The source may still be required by:

- Other subscribers.
- Recording.
- Temporary retention.
- Derived streams.
- Explicit user activation.
- Other registered consumers.

The Stream Registry remains authoritative for source lifetime.

---

## 13. Source-Stream Deduplication

MarketForge must avoid creating redundant exchange subscriptions when several downstream consumers request the same source.

This responsibility is shared between the Stream Registry and Subscription Manager.

The Publisher participates by referencing existing logical source streams rather than independently establishing native subscriptions.

### 13.1 Deduplication Principle

Suppose three consumers request:

    Binance BTCUSDT Trades

The desired architecture is:

    Binance WebSocket
           |
           v
    One Native Trade Subscription
           |
           v
    One Logical Source Stream
           |
           v
        Publisher
           |
      +----+----+
      |    |    |
      v    v    v
      A    B    C

The Publisher fans out one accepted canonical event stream to three consumers.

It must not create three independent exchange subscriptions.

### 13.2 Source Identity

Source deduplication uses the canonical subscription identity defined in document 05.

Conceptually:

    SubscriptionKey {
        exchange
        market
        instrument_id
        channel
        channel_parameters
    }

The Stream Registry resolves duplicate source requests using this identity.

### 13.3 Channel Parameter Compatibility

Two source requests are equivalent only when their subscription-defining parameters are compatible.

For example:

    BTCUSDT L2, depth 20

and:

    BTCUSDT L2, depth 100

may represent different native subscriptions.

The Publisher must not assume that they are identical.

A deeper subscription may potentially satisfy a shallower consumer through an explicitly supported derived view.

However, this requires a defined compatibility and depth-projection contract.

The initial implementation should not automatically substitute one depth configuration for another.

### 13.4 Shared Source Ownership

Source streams may exist independently of downstream subscribers.

For example:

    Source Stream
         |
         +-- Subscriber A
         |
         +-- Subscriber B
         |
         +-- Recording Manager
         |
         +-- Live Retention Manager

Subscriber count is not the sole determinant of source lifetime.

### 13.5 Reference Accounting

The Stream Registry or a dedicated dependency manager tracks source consumers.

Conceptually:

    SourceConsumers {
        subscriber_references
        recording_references
        retention_references
        derived_stream_references
        explicit_activation
    }

This representation is illustrative.

The Publisher reports downstream subscription acquisition and release through the defined interface.

It must not directly modify source lifecycle state.

### 13.6 Last Subscriber Disconnects

When the final downstream subscriber disconnects:

    Subscriber Count = 0

The source stream may remain active.

The Stream Registry evaluates the remaining desired-state and dependency requirements.

The Publisher must not unilaterally deactivate the exchange subscription.

### 13.7 Subscriber Reconnection

When a subscriber reconnects, it may request a source stream that is already active.

The Publisher should reuse that source stream.

A subscriber reconnection must not trigger exchange resubscription unless the underlying source actually requires restoration.

---

## 14. Stream-to-Subscriber Routing

The Publisher maintains a routing index that maps logical source streams to interested subscriber sessions.

Routing occurs after canonical event acceptance.

### 14.1 Routing Model

Conceptually:

    StreamId
       |
       v
    Subscriber Set

Example:

    Stream 15
       |
       +-- Subscriber 1
       +-- Subscriber 3
       +-- Subscriber 8

The Publisher uses this index to identify the recipients of each accepted event.

### 14.2 RoutingIndex

Conceptual representation:

    RoutingIndex {
        stream_id -> subscriber_sessions
    }

The implementation may use:

- Concurrent hash maps.
- Immutable routing snapshots.
- Actor-owned routing state.
- Other bounded concurrent structures.

The selected design must minimize per-event lock contention.

### 14.3 Routing Requirements

Routing must:

- Use canonical `StreamId` values.
- Preserve per-stream event ordering.
- Reject obsolete subscriber sessions.
- Respect active subscription state.
- Avoid duplicate delivery-route entries.
- Support concurrent subscription mutations.
- Remain independent of exchange-native symbols.
- Avoid unbounded event copying.

### 14.4 Routing Pipeline

    Canonical Event
           |
           v
    Validate Source Stream
           |
           v
    Locate Subscriber Set
           |
           v
    Validate Subscriber Generations
           |
           v
    Apply Delivery Eligibility
           |
           v
    Enqueue for Each Subscriber

The Publisher must not perform expensive socket writes while holding a shared routing lock.

### 14.5 Routing Snapshot Consistency

Concurrent subscription changes must not create ambiguous delivery behavior.

For example:

    Event E accepted.

    Subscriber A subscribes.

The system must define whether Subscriber A is eligible to receive E.

Eligibility should be established using an explicit subscription activation boundary.

The Publisher must not rely on nondeterministic timing of concurrent map mutations.

### 14.6 Routing Activation Boundary

Each downstream subscription records the source-stream cursor from which delivery begins.

Conceptually:

    StartingCursor

The activation boundary must be established atomically with routing eligibility or through an equivalent ordered coordination mechanism.

This prevents events from being silently lost between:

    Source Cursor Inspection

and:

    Routing Activation

### 14.7 Routing Removal Boundary

Unsubscription establishes a corresponding terminal delivery boundary.

After the removal boundary becomes authoritative, new source events must not be enqueued for that downstream subscription.

Events already queued before removal follow the configured drain or cancellation policy.

### 14.8 Multiple Source Streams

A subscriber may receive events from several source streams.

Example:

    Subscriber A
         |
         +-- Stream 15
         +-- Stream 20
         +-- Stream 31

The Publisher preserves ordering within each source stream.

It does not automatically establish event-time ordering between these streams.

### 14.9 Routing Failure

If routing cannot safely determine subscriber eligibility:

- Do not silently discard an accepted event.
- Report the failure.
- Preserve the relevant publication boundary.
- Apply the configured backpressure or recovery policy.
- Avoid assigning events to obsolete sessions.

A routing failure is a publication-system problem, not automatically an exchange acquisition gap.

---

## 15. Subscriber Subscription Activation

A downstream subscription becomes active only when its delivery requirements can be satisfied.

### 15.1 Activation Sequence

    Subscribe Request
           |
           v
    Validate Session
           |
           v
    Resolve Source Stream
           |
           v
    Validate Delivery Mode
           |
           v
    Reserve Subscriber Resources
           |
           v
    Establish Delivery Boundary
           |
           v
    Activate Routing Membership
           |
           v
    Send SubscribeAck
           |
           v
    Begin Event Delivery

The ordering of acknowledgement and initial event frames must follow the negotiated IPC protocol.

The subscriber must not receive ordinary events before it can identify the subscription to which they belong.

### 15.2 Activation Result

A successful activation acknowledgement should identify:

    StreamId
    SubscriberSessionId
    DeliveryMode
    StartingCursor
    SubscriptionGeneration

The exact wire fields are defined by the IPC protocol schema.

### 15.3 Trade Subscription Activation

For trade streams, activation may begin from:

    LatestAvailable

or:

    ExplicitCursor

The selected starting position determines whether the subscriber receives only future events or also requests replay.

An ordinary live subscription does not guarantee historical execution completeness.

### 15.4 L2 Subscription Activation

A subscriber requiring a synchronized order book must receive a valid snapshot associated with the active book generation.

Conceptually:

    Subscribe to L2
           |
           v
    Resolve Current Book
           |
           v
    Obtain Consistent Snapshot
           |
           v
    Establish Delta Boundary
           |
           v
    Send Snapshot
           |
           v
    Send Subsequent Deltas

The snapshot and following deltas must belong to a compatible book generation.

A delta must not overtake the initialization snapshot.

### 15.5 Source Not Ready

If the source stream is registered but not operational:

    PendingSource

The Publisher may wait for the source to become ready within a configured deadline.

If readiness cannot be established, it returns an explicit pending or failed activation result.

The Publisher must not report active synchronized L2 delivery before the BookStore confirms synchronization.

### 15.6 Activation Cancellation

If the subscriber disconnects during activation:

- Cancel the pending downstream subscription.
- Release resource reservations.
- Remove provisional routing membership.
- Reject obsolete completion events.
- Preserve the underlying source stream.

Cancellation must not remove an exchange-native subscription that remains required by other consumers.

---

## 16. Subscriber Subscription Removal

Unsubscription must be explicit, idempotent, and generation-safe.

### 16.1 Removal Sequence

    Unsubscribe Request
           |
           v
    Validate Session
           |
           v
    Resolve Subscription
           |
           v
    Establish Removal Boundary
           |
           v
    Remove Routing Membership
           |
           v
    Resolve Queued Events
           |
           v
    Release Subscription Resources
           |
           v
    Send UnsubscribeAck

### 16.2 Queued Event Policy

Events already queued before unsubscription may be:

    Drained

or:

    Cancelled

The selected behavior must be explicit.

The Publisher must not silently claim that every queued event was delivered when cancellation discarded pending delivery obligations.

### 16.3 Session Disconnection

When the Unix socket closes unexpectedly:

- Fence the session generation.
- Remove downstream routing memberships.
- Release session queues.
- Record final delivery positions.
- Report interrupted delivery where required.
- Preserve shared source streams.

A subscriber disconnect must not automatically invalidate an otherwise healthy trade stream or L2 book.

### 16.4 Removal Idempotency

Repeated unsubscription requests must not cause double resource release.

An already removed downstream subscription may return a successful no-op response or a defined not-found result.

The IPC contract must specify the chosen behavior.

### 16.5 Source Reference Release

Removing a downstream subscription releases its reference to the source stream.

The Stream Registry evaluates whether the source remains necessary.

The Publisher must not independently decide to terminate the native exchange subscription.

---

## 17. Event Delivery Semantics

The Publisher must explicitly define what it means for a canonical event to be accepted, queued, transmitted, and consumed.

These stages represent different completion boundaries.

A successful Unix socket write does not establish that the receiving application has processed the event.

### 17.1 Delivery Stages

The initial delivery model distinguishes:

    ProducerAccepted
    PublisherCommitted
    SubscriberQueued
    SocketWritten
    SubscriberAcknowledged

| Stage | Meaning |
|---|---|
| ProducerAccepted | Trade Processor or BookStore accepted the canonical event |
| PublisherCommitted | Event entered the Publisher's authoritative distribution sequence |
| SubscriberQueued | Event was admitted to a subscriber's bounded delivery queue |
| SocketWritten | Complete serialized frame was written to the local socket |
| SubscriberAcknowledged | Consumer explicitly confirmed the event under a negotiated acknowledgement contract |

Not every subscriber uses application-level acknowledgements.

The system must not represent an earlier stage as proof of a later stage.

### 17.2 Delivery Guarantees

The initial Publisher supports two conceptual delivery modes:

    LiveBestEffort
    LosslessWithinRetention

**LiveBestEffort**

Prioritizes low-latency delivery.

When a subscriber cannot keep up, its session may be disconnected or its delivery state explicitly marked incomplete.

Events must not be silently discarded while the subscriber remains classified as uninterrupted.

**LosslessWithinRetention**

Attempts to preserve every accepted event within the configured replay and retention boundaries.

Delivery may use bounded queues, temporary retention, cursor replay, and explicit subscriber acknowledgements.

This mode is available only when the underlying retention and protocol capabilities support it.

### 17.3 Scope of Lossless Delivery

Lossless delivery must be defined relative to a specific boundary.

For example:

    Lossless Source Acquisition

    Lossless Publisher Admission

    Lossless Subscriber Queueing

    Lossless Subscriber Replay

These are not equivalent guarantees.

A Publisher cannot guarantee that every exchange execution was acquired.

It can guarantee only the behavior of canonical events admitted to its distribution pipeline.

### 17.4 Initial Delivery Policy

The initial recommended default is:

    LiveBestEffort

with:

- Bounded subscriber queues.
- Per-stream cursor tracking.
- Explicit delivery-gap reporting.
- Disconnection of subscribers that cannot maintain their delivery contract.
- Optional replay through the Live Retention Manager.

The system must not silently skip events and continue claiming an uninterrupted subscriber stream.

### 17.5 Delivery Acknowledgements

Application-level acknowledgements are optional and capability-negotiated.

When enabled, they identify which events the subscriber has processed according to the protocol contract.

The Publisher must distinguish:

    LastQueuedCursor

    LastWrittenCursor

    LastAcknowledgedCursor

These values may differ.

### 17.6 At-Least-Once Delivery

At-least-once delivery may be supported through retention and replay.

A consumer reconnecting after an uncertain delivery boundary may receive previously transmitted events again.

Therefore, downstream consumers requiring exactly-once processing must deduplicate by canonical event identity.

The Publisher must not promise exactly-once application processing merely because it supports acknowledgements.

### 17.7 Delivery Failure Classification

Possible failures include:

    SubscriberDisconnected
    SubscriberQueueFull
    SocketWriteFailure
    SocketWriteTimeout
    AcknowledgementTimeout
    ReplayUnavailable
    CursorExpired
    ProtocolViolation

Each failure must produce an explicit delivery outcome.

---

## 18. Per-Stream Event Ordering

The Publisher must preserve canonical publication order within each logical source stream.

It must not infer a global ordering across unrelated source streams.

### 18.1 Ordering Identity

Each accepted event carries:

    StreamId

    StreamCursor

The complete cursor identity may additionally include a runtime or cursor epoch when required by the canonical event contract.

### 18.2 Per-Stream Ordering Invariant

For a single source stream:

    Cursor 100
        |
        v
    Cursor 101
        |
        v
    Cursor 102

A subscriber must receive these events in publication order.

The Publisher must not deliver:

    100 -> 102 -> 101

unless the subscriber explicitly selected a separate protocol that does not guarantee ordered delivery.

The initial Unix stream transport preserves outbound frame order.

### 18.3 Multiple Source Streams

A subscriber may consume:

    Stream A
    Stream B
    Stream C

Each stream has an independent cursor domain.

Example:

    Stream A: Cursor 100
    Stream B: Cursor 300
    Stream A: Cursor 101
    Stream C: Cursor 700

The interleaving represents Publisher scheduling.

It does not establish global market-event ordering.

### 18.4 Event-Time Ordering

Event timestamps may arrive out of order.

This is particularly relevant when trade recovery produces late historical executions.

Example:

    Cursor 100:
        Event Time 12:00:05

    Cursor 101:
        Event Time 12:00:06

    Cursor 102:
        Event Time 12:00:03
        Source = Recovery

Publication order remains valid.

The event-time sequence is not monotonic.

Cross-stream event-time ordering and reorder-buffer semantics belong to the separate merge subsystem.

### 18.5 L2 Ordering

L2 updates require stricter generation-aware ordering.

For one book generation:

    Snapshot
        |
        v
    Delta 1
        |
        v
    Delta 2
        |
        v
    Delta 3

A subscriber reconstructing book state must receive the authoritative snapshot before subsequent deltas.

The Publisher must not reorder deltas within a generation.

### 18.6 Book Generation Changes

When the BookStore creates a new authoritative generation:

    Book Generation 5
        |
        +-- Delta 100
        +-- Delta 101

    Book Generation 6
        |
        +-- Snapshot
        +-- Delta 1

The Publisher must preserve the generation transition.

Deltas from generation 5 must not be delivered after the subscriber begins reconstructing generation 6 as though they belonged to the new book.

### 18.7 Ordering Across Publisher Restart

After restart, source cursors must remain unambiguous.

The system must either:

- Restore a validated persistent cursor frontier.
- Establish a new cursor epoch.
- Use another documented restart-safe publication identity.

The Publisher must not reuse cursor identities in a way that makes distinct accepted events indistinguishable.

---

## 19. Subscriber Delivery Queues

Each subscriber session owns a bounded outbound queue.

The queue decouples canonical event routing from potentially slow Unix socket writes.

### 19.1 Queue Architecture

Conceptually:

    Canonical Event Bus
             |
             v
          Publisher
             |
             v
       Routing Decision
             |
             v
    Subscriber Outbound Queue
             |
             v
       Session Writer
             |
             v
         Unix Socket
             |
             v
       Subscriber Process

The Publisher must not perform blocking socket writes while holding shared routing locks.

### 19.2 Queue Ownership

A subscriber queue belongs to one transport session generation.

When the session terminates, the queue is released according to its delivery contract.

A new session receives a new queue.

Retained event history remains owned by the Live Retention Manager.

### 19.3 Queue Limits

The initial implementation must support:

    MaximumQueueMessages

    MaximumQueueBytes

    MaximumQueuedEventAge

    MaximumSingleFrameSize

The effective queue capacity is constrained by all applicable limits.

Queue limits must be validated before admission.

### 19.4 Queue Representation

Conceptual structure:

    SubscriberQueue {
        session_id
        session_generation

        maximum_messages
        maximum_bytes

        queued_messages
        queued_bytes

        state
    }

The queue contains canonical event references or serialized IPC frames.

The exact representation is an implementation decision.

### 19.5 Avoiding Event Copies

When one canonical event is distributed to several subscribers, the Publisher should avoid unnecessary payload duplication.

Possible approaches include:

- Immutable reference-counted canonical events.
- Shared serialized frame buffers.
- Per-subscriber references to a shared event.
- Bounded serialization caches.

The optimization must not compromise event immutability or resource ownership.

### 19.6 Per-Subscriber Isolation

A slow subscriber must not indefinitely block unrelated subscribers.

Example:

    Canonical Event
           |
       +---+---+
       |       |
       v       v
    Fast A   Slow B
       |       |
       v       v
    Delivered Queue Fills

Subscriber A should continue receiving events while Subscriber B's queue remains within its configured capacity.

When Subscriber B exceeds its limits, the Publisher applies that subscriber's delivery failure policy.

### 19.7 Queue Admission

An event may enter a subscriber queue only when:

- The session generation is current.
- The downstream subscription is active.
- The event satisfies the subscription's cursor boundary.
- The event belongs to the requested source.
- Sufficient queue capacity exists.
- The event is compatible with the negotiated IPC protocol.

Queue admission and delivery cursor updates must remain consistent.

### 19.8 Queue Ordering

A subscriber queue must preserve the event order established by the Publisher.

For one source stream, later cursors must not overtake earlier cursors.

For multiple source streams, interleaving may follow the Publisher's scheduling policy.

The scheduler must not violate per-stream ordering.

### 19.9 Queue Drain

During graceful disconnection or unsubscription, queued events follow an explicit policy:

    DrainWithinDeadline

or:

    CancelWithDeliveryBoundary

The selected policy must be visible in the final delivery outcome.

The Publisher must not report successfully written events when their queued frames were cancelled.

---

## 20. Subscriber Backpressure

Subscriber backpressure occurs when a consumer cannot receive events as quickly as they are published.

The Publisher must manage this condition without unbounded memory growth or silent event loss.

### 20.1 Backpressure Detection

Backpressure signals include:

- Increasing outbound queue occupancy.
- Sustained queue-full conditions.
- Slow socket writes.
- Repeated write timeouts.
- Increasing delivery latency.
- Unacknowledged events exceeding configured limits.

The Publisher reports these measurements through the Metrics Registry.

### 20.2 Backpressure States

Supported states:

    Normal
    Elevated
    Saturated
    Failed

| State | Meaning |
|---|---|
| Normal | Queue operates within expected limits |
| Elevated | Queue utilization exceeds warning thresholds |
| Saturated | Queue cannot admit additional events |
| Failed | Required delivery guarantees can no longer be maintained |

Thresholds belong to runtime configuration.

### 20.3 Backpressure Policy

The initial Publisher supports:

    BoundedQueue

    DisconnectSlowSubscriber

    ReplayFromRetention

These mechanisms may be combined according to delivery mode.

The Publisher must not introduce an unbounded per-subscriber event backlog.

### 20.4 LiveBestEffort Overflow

For `LiveBestEffort`, a subscriber whose queue becomes saturated may be disconnected.

Required behavior:

    Queue Saturated
          |
          v
    Stop Subscriber Admission
          |
          v
    Establish Last Valid Delivery Boundary
          |
          v
    Report Delivery Interruption
          |
          v
    Disconnect Subscriber
          |
          v
    Release Queue Resources

The Publisher must not silently drop one event and continue presenting subsequent events as uninterrupted.

### 20.5 LosslessWithinRetention Overflow

For `LosslessWithinRetention`, the Publisher may use retained canonical events to recover from queue saturation.

Conceptually:

    Subscriber Falls Behind
             |
             v
    Record Last Delivery Cursor
             |
             v
    Suspend Direct Live Queueing
             |
             v
    Retention-Based Replay
             |
             v
    Rejoin Live Delivery

This procedure requires a valid replay boundary.

The Publisher must not assume that the requested events remain available indefinitely.

### 20.6 Replay Availability

Replay is possible only when:

- The requested source stream exists.
- The cursor identity is valid.
- The required retained interval is available.
- The retained events are compatible with the requested protocol.
- The subscriber has permission to access the stream.
- Replay can establish the required transition to live delivery.

If any requirement fails:

    ReplayUnavailable

The Publisher must report the failure explicitly.

### 20.7 Backpressure and Source Processing

A slow subscriber must not automatically force the Trade Processor or BookStore to stop.

However, this isolation is conditional.

If the configured contract requires lossless delivery to a subscriber and no replay or buffering capacity remains, the Publisher must report that the guarantee can no longer be satisfied.

The system must not claim universal lossless delivery while silently discarding subscriber events.

### 20.8 Shared Resource Pressure

Many slow subscribers may collectively exhaust Publisher memory or scheduling capacity.

The Publisher must enforce global limits in addition to per-subscriber limits.

Possible constraints:

    MaximumPublisherQueueBytes

    MaximumPendingFrames

    MaximumConcurrentWriters

    MaximumReplaySessions

The effective limits belong to the Configuration System.

### 20.9 Backpressure Escalation

When Publisher-wide backpressure becomes critical:

- Restrict new subscriber admission.
- Identify saturated sessions.
- Apply configured slow-consumer policies.
- Preserve accepted-event ordering.
- Report impaired delivery guarantees.
- Coordinate resource recovery.
- Avoid uncontrolled queue growth.

The Recovery Coordinator handles system-level escalation.

The Publisher owns subscriber-level delivery recovery.

---

## 21. Delivery Progress Tracking

The Publisher must track delivery progress independently for every subscriber and source stream.

### 21.1 Per-Stream Delivery State

Conceptually:

    SubscriberStreamDeliveryState {
        subscriber_session_id
        stream_id

        starting_cursor

        last_queued_cursor
        last_written_cursor
        last_acknowledged_cursor

        delivery_status
    }

The exact fields depend on the negotiated delivery mode.

### 21.2 Queued Cursor

`last_queued_cursor` identifies the latest event admitted to the subscriber's outbound queue.

It does not prove that the event was written to the socket.

### 21.3 Written Cursor

`last_written_cursor` identifies the latest event whose complete IPC frame was successfully written.

It does not prove that the receiving application decoded or processed the event.

### 21.4 Acknowledged Cursor

`last_acknowledged_cursor` identifies the latest event acknowledged according to the negotiated application-level acknowledgement contract.

An acknowledgement must include sufficient source identity to distinguish independent cursor domains.

### 21.5 Cursor Advancement

Delivery progress must advance monotonically within one cursor epoch.

For one stream:

    Cursor 100
    Cursor 101
    Cursor 102

A client acknowledgement for cursor 102 must not be interpreted as proof that every preceding event was processed unless the negotiated acknowledgement protocol explicitly defines cumulative acknowledgement semantics.

### 21.6 Acknowledgement Validation

Before accepting an acknowledgement:

- Validate the subscriber session generation.
- Validate the source stream.
- Validate the cursor epoch.
- Validate the acknowledged cursor.
- Confirm that it does not exceed the permitted delivery frontier.
- Apply the negotiated acknowledgement semantics.

Unknown or obsolete acknowledgements must not modify current delivery state.

### 21.7 Disconnection Boundary

When a subscriber disconnects, the Publisher records the latest known delivery positions.

Conceptually:

    LastQueued
    LastWritten
    LastAcknowledged

These positions must not be collapsed into one value.

The selected replay starting point depends on the delivery contract.

### 21.8 Delivery Uncertainty

If a socket disconnects after a frame was written but before an application acknowledgement arrives, the Publisher cannot know whether the application processed the event.

Therefore:

    SocketWritten = true

    SubscriberProcessed = unknown

Replay may produce duplicate application delivery.

Consumers requiring exactly-once effects must implement appropriate deduplication or transactional acknowledgement semantics.

---

## 22. Delivery Failure Isolation

Subscriber delivery failures must remain distinct from exchange acquisition and canonical normalization failures.

### 22.1 Failure Domains

| Failure | Affected domain |
|---|---|
| Exchange WebSocket disconnect | Source acquisition |
| Trade normalization error | Source processing |
| L2 sequence gap | Book integrity |
| Subscriber queue saturation | Subscriber delivery |
| Unix socket write failure | IPC transport |
| Subscriber acknowledgement timeout | Delivery confirmation |
| Retention segment unavailable | Replay availability |
| PostgreSQL outage | Optional recording |

The Publisher must not report a subscriber queue failure as an exchange sequence gap.

### 22.2 Subscriber Failure

A single subscriber failure should affect only that subscriber session whenever possible.

The Publisher must preserve other active subscriber routes.

### 22.3 Publisher Failure

A failure affecting the Publisher itself may impair multiple subscriber sessions.

The Publisher reports the condition through the runtime failure interface.

The Recovery Coordinator determines system-level recovery actions.

### 22.4 Delivery Gap Reporting

When a subscriber misses one or more accepted canonical events, the Publisher must identify the affected cursor interval when possible.

Conceptually:

    DeliveryGap {
        subscriber_id
        stream_id
        first_unavailable_cursor
        last_unavailable_cursor
        reason
    }

When exact boundaries cannot be established, the gap must remain explicitly unverified.

The Publisher must not fabricate missing cursor ranges.

### 22.5 Source Integrity Independence

A subscriber delivery gap does not invalidate an otherwise healthy source stream.

For example:

    Binance BTCUSDT Trades:
        Source Healthy

    Subscriber A:
        Delivery Healthy

    Subscriber B:
        Delivery Gap

This is a valid system state.

The runtime must preserve the distinction.

---

## 23. L2 Snapshot-to-Live Handoff

A subscriber joining an active L2 stream must receive a consistent initialization snapshot followed by the correct sequence of canonical L2 updates.

The Publisher must prevent race conditions between snapshot acquisition and live event delivery.

The procedure is separate from exchange-side L2 synchronization.

The BookStore has already reconstructed the authoritative current state.

The Publisher establishes a consistent starting point for a downstream consumer.

### 23.1 Fundamental Requirement

A subscriber must never receive:

    Snapshot at BookVersion 100
              |
              v
    Delta at BookVersion 103

without accounting for versions 101 and 102.

The required invariant is:

    Snapshot Version N
           |
           v
    Delta Version N+1
           |
           v
    Delta Version N+2

This assumes that every required committed transition is represented in the selected canonical delivery stream.

When that continuity cannot be established, the Publisher must provide a newer snapshot or report a delivery failure.

### 23.2 Snapshot Handoff Identity

The snapshot-to-live boundary must identify:

    StreamId
    BookGeneration
    BookVersion
    StreamCursor
    ConfigurationRevision
    MetadataRevision

These identifiers connect the BookStore's authoritative state to the Publisher's canonical publication sequence.

A BookVersion is not interchangeable with a StreamCursor.

The BookVersion identifies a committed book state.

The StreamCursor identifies a canonical publication position.

The relationship between them must be recorded explicitly.

### 23.3 Snapshot Handoff Procedure

The recommended procedure is:

    Subscriber Requests L2
              |
              v
    Validate Subscription
              |
              v
    Establish Ordered Event Capture
              |
              v
    Acquire Consistent Book Snapshot
              |
              v
    Record Snapshot Publication Boundary
              |
              v
    Buffer Subsequent Deltas
              |
              v
    Send Initialization Snapshot
              |
              v
    Replay Buffered Deltas
              |
              v
    Transition to Live Delivery

The event capture boundary must be established before a gap can occur between snapshot creation and delta delivery.

### 23.4 Atomic Boundary Establishment

The Publisher and BookStore must coordinate snapshot acquisition with event routing.

An implementation may use:

- A per-stream serialized command queue.
- A publication barrier.
- A versioned snapshot API.
- A retention-backed cursor boundary.

The exact mechanism is an implementation detail.

However, the operation must produce one consistent relationship between:

    Snapshot BookVersion

and:

    First Required Delta

The Publisher must not independently read a book snapshot and later subscribe to updates without accounting for events committed between those operations.

### 23.5 Snapshot Creation Race

Consider:

    BookVersion 100 committed.
              |
              v
    Subscriber requests snapshot.
              |
              v
    BookVersion 101 committed.
              |
              v
    Subscriber routing becomes active.

If the snapshot contains version 100 but routing begins after version 101, the subscriber may miss a required update.

Therefore:

    Read Snapshot
          +
    Activate Routing

cannot be treated as two unrelated operations.

The handoff protocol must establish a consistent boundary.

### 23.6 Handoff Buffer

During initialization, subsequent deltas may be retained in a bounded subscriber-specific handoff buffer.

Conceptually:

    L2HandoffBuffer {
        subscriber_session_id
        stream_id

        book_generation
        snapshot_book_version

        first_required_cursor
        buffered_events

        maximum_messages
        maximum_bytes
    }

The buffer must preserve publication order.

It must not silently discard events.

### 23.7 Handoff Buffer Overflow

If the buffer exceeds its configured capacity:

    Handoff Buffer Overflow
              |
              v
    Cancel Current Handoff
              |
              v
    Release Temporary State
              |
              v
    Retry With New Snapshot
              |
              v
    Establish New Boundary

Retry attempts must be bounded.

Repeated failure must produce an explicit subscription failure or degraded delivery status.

The Publisher must not continue with a snapshot whose required deltas are unavailable.

### 23.8 Book Generation Changes

A book generation may change during subscriber initialization.

Example:

    Snapshot acquired:
        BookGeneration 5

    Exchange sequence gap occurs.

    BookStore synchronizes:
        BookGeneration 6

The Publisher must not attach generation-6 deltas to a generation-5 snapshot.

The current handoff must be cancelled or restarted.

A new authoritative snapshot from generation 6 establishes the replacement baseline.

### 23.9 Initialization Snapshot Origin

The initialization snapshot may be generated from the current BookStore state.

It is not necessarily an exchange-native snapshot message.

The IPC protocol must distinguish:

    Exchange-Origin Snapshot

from:

    Subscriber Initialization Snapshot

Both may contain authoritative book state, but their provenance and publication identities differ.

The Publisher must not fabricate an exchange-native sequence event when constructing a subscriber initialization snapshot.

### 23.10 Handoff Completion

The handoff succeeds when:

- The subscriber receives a valid initialization snapshot.
- Required subsequent deltas are available.
- The book generation remains compatible.
- The cursor boundary is valid.
- Live routing begins without missing required updates.
- Subscriber queue capacity remains sufficient.

For acknowledgement-enabled delivery, application processing confirmation is a separate guarantee.

Successful handoff does not imply that every frame has been acknowledged by the consumer.

---

## 24. Cursor-Based Replay

Cursor-based replay allows subscribers to resume canonical event delivery from an earlier accepted publication position when the required events remain available.

Replay is provided through the Live Retention Manager.

The Publisher coordinates replay delivery but does not own historical event storage.

### 24.1 Replay Purpose

Replay supports:

- Subscriber reconnection.
- Recovery from temporary subscriber backpressure.
- Consumer process restarts.
- Reestablishing delivery after socket interruption.
- Joining an existing stream from a retained cursor.
- Recovering bounded publisher-side delivery gaps.

Replay does not automatically recover exchange events that MarketForge never acquired.

### 24.2 Replay Identity

A replay request must identify:

    StreamId
    CursorEpoch
    StartingCursor

The cursor epoch may be represented by a runtime-instance identity or another restart-safe namespace defined in document 02.

A numeric cursor without its applicable epoch may be ambiguous across process restarts.

### 24.3 Replay Request

Conceptual structure:

    ReplayRequest {
        request_id
        stream_id

        cursor_epoch
        starting_cursor

        delivery_mode
        maximum_events
    }

The exact wire representation belongs to the finalized IPC protocol.

The Publisher must validate the requested replay capability during protocol negotiation.

### 24.4 Replay Availability

A replay request may be accepted only when:

- The source stream is known.
- The cursor identity is valid.
- The requested interval is retained.
- The relevant event schema is compatible.
- The subscriber is authorized.
- Replay capacity is available.
- A valid transition to live delivery can be established.

A request outside available retention must fail explicitly.

### 24.5 Retention Lookup

Conceptually:

    Replay Request
           |
           v
    Validate Cursor Identity
           |
           v
    Query Retention Availability
           |
           v
    Locate Required Events
           |
           v
    Reserve Replay Resources
           |
           v
    Begin Ordered Replay

The Publisher must not implement a competing retention index.

### 24.6 Replay Ordering

Within one source stream, replay preserves canonical publication order.

Conceptually:

    Cursor 100
    Cursor 101
    Cursor 102
    Cursor 103

This does not necessarily imply monotonic event timestamps.

Recovered trades may have been published after newer live trades.

The replay mechanism must preserve the original canonical publication sequence.

### 24.7 Replay Inclusivity

The IPC contract must define whether a requested starting cursor is inclusive or exclusive.

The initial recommended contract is:

    ResumeAfterCursor

For example:

    Last acknowledged cursor = 100

The replay begins with:

    Cursor 101

A separate operation may support inclusive replay when explicitly required.

The protocol must not leave this distinction ambiguous.

### 24.8 Replay Deduplication

A subscriber may reconnect after receiving an event but before acknowledging it.

Consequently, replay may deliver that event again.

The Publisher must preserve stable event identities so the subscriber can deduplicate repeated delivery.

Replay must not create a new canonical execution identity for an existing retained event.

### 24.9 Replay Expiration

If the requested cursor has expired from retention:

    ReplayUnavailable

The error should identify the available replay frontier when the retention contract supports it.

Example:

    Requested:
        Cursor 100

    Earliest available:
        Cursor 150

The Publisher must not silently begin at cursor 150 while claiming uninterrupted delivery.

### 24.10 Replay Cancellation

Replay may be cancelled because of:

- Subscriber disconnection.
- Session replacement.
- Explicit unsubscription.
- Resource exhaustion.
- Runtime shutdown.
- Unsupported cursor transition.
- Source-stream removal.

Cancellation must release replay-specific resources.

It must not modify retained canonical events.

---

## 25. Replay-to-Live Handoff

A subscriber catching up from retention must eventually transition to the current live publication stream.

This transition must not lose events or publish the same canonical event twice within one delivery sequence.

### 25.1 Handoff Problem

Consider:

    Subscriber replays through cursor 100.

    Live publisher reaches cursor 105.

If replay stops at cursor 100 and normal routing begins at cursor 105, events 101–104 are missed.

The Publisher must coordinate the transition.

### 25.2 Recommended Procedure

    Subscriber Requests Replay
              |
              v
    Establish Live Publication Boundary
              |
              v
    Begin Ordered Retention Replay
              |
              v
    Track Newly Published Events
              |
              v
    Replay Through Boundary
              |
              v
    Reconcile Buffered Live Events
              |
              v
    Activate Direct Live Delivery

The transition must use the same canonical cursor namespace.

### 25.3 Replay Frontier

The Publisher maintains a replay frontier identifying the latest cursor already scheduled for the subscriber.

Conceptually:

    ReplayFrontier

Newly accepted live events must not be delivered before earlier required replay events.

### 25.4 Concurrent Live Publication

Live publication continues while replay is underway.

The subscriber's direct live route may be suspended while the Publisher accumulates a bounded handoff backlog or relies on a retained cursor frontier.

This must not block unrelated subscribers.

### 25.5 Duplicate Boundary Events

The same event may appear in both:

    Retention Replay

and:

    Live Handoff Buffer

The Publisher must compare canonical event identities and deliver the event once within the current subscription's authoritative delivery sequence.

This does not eliminate the possibility of duplicate application delivery after an ambiguous acknowledgement boundary.

### 25.6 Handoff Buffer Capacity

The replay-to-live handoff must remain bounded.

If the subscriber cannot catch up while new events continue arriving, possible outcomes include:

    ContinueReplayWithinRetention

    DisconnectSlowSubscriber

    FailStrictDelivery

The Publisher must not accumulate an unlimited backlog.

### 25.7 Replay Falling Behind Retention

If retention expires events before replay reaches them:

    ReplayContinuityLost

The Publisher must stop claiming uninterrupted delivery.

The subscriber may request a new starting position or, for L2, a fresh initialization snapshot.

For trades, a new live subscription does not reconstruct the expired canonical publication interval.

### 25.8 Handoff Completion

A handoff succeeds when:

    Last Replayed Cursor
            |
            v
    Next Delivered Live Cursor

forms a valid continuous publication sequence under the selected cursor contract.

The Publisher must validate this boundary before declaring replay complete.

---

## 26. Subscriber Reconnection

Subscriber reconnection is separate from exchange WebSocket reconnection.

A local consumer may disconnect while MarketForge continues receiving and processing market data normally.

### 26.1 Reconnection Responsibilities

The Publisher must:

- Detect session closure.
- Fence the old session generation.
- Release obsolete transport resources.
- Preserve required delivery diagnostics.
- Admit the replacement session.
- Negotiate IPC capabilities.
- Validate subscriber identity.
- Restore requested downstream subscriptions.
- Coordinate replay when available.

The Publisher must not automatically restart exchange subscriptions merely because one local subscriber reconnects.

### 26.2 Session Restoration

Conceptually:

    Subscriber Session Lost
              |
              v
    Old Session Generation Fenced
              |
              v
    Subscriber Reconnects
              |
              v
    New Unix Socket Session
              |
              v
    IPC Protocol Negotiation
              |
              v
    Subscribe / Resume Request
              |
              v
    Validate Delivery Boundary
              |
              v
    Replay or Live Handoff
              |
              v
    Delivery Restored

### 26.3 Persistent Subscriber Identity

The initial implementation may use ephemeral subscriber identities.

A reconnecting client then explicitly supplies its desired source subscriptions and replay cursors.

Persistent subscriber identities may be introduced later.

If persistent identities are supported, the Publisher must prevent simultaneous sessions from incorrectly sharing one authoritative delivery state.

### 26.4 Reconnection Cursor

A subscriber may provide its last successfully processed cursor.

The Publisher must validate that cursor against:

- Stream identity.
- Cursor epoch.
- Available retention.
- Negotiated delivery semantics.

The client must not claim arbitrary cursor ownership to bypass authorization.

### 26.5 Reconnection Without Replay

If replay is unavailable or not requested, the subscriber may resume from the current live frontier.

The new delivery interval must be identified as a fresh subscription boundary.

The Publisher must not claim continuity with the previous session.

### 26.6 L2 Reconnection

For a subscriber requiring synchronized L2 reconstruction, the preferred default is:

    Fresh BookStore Snapshot
              |
              v
    Snapshot-to-Live Handoff

This avoids relying on potentially expired subscriber-side book state.

Cursor replay may be used when its continuity and generation compatibility can be established.

### 26.7 Trade Reconnection

For trade subscribers:

    ResumeAfterCursor

may be used when the relevant retained events remain available.

Otherwise:

    ResumeFromCurrentFrontier

creates a new delivery boundary with an explicit potential delivery gap.

### 26.8 Obsolete Session Operations

Late operations from a disconnected session must not affect the replacement session.

Examples include:

- Delayed write completions.
- Delivery acknowledgements.
- Pending subscription acknowledgements.
- Replay completions.
- Timeout callbacks.

Every operation must validate its session generation.

---

## 27. IPC Transport Failure Recovery

The Publisher owns local Unix socket session recovery.

The Recovery Coordinator handles failures affecting the broader publication subsystem.

### 27.1 Failure Categories

Possible IPC failures include:

    SocketClosed
    BrokenPipe
    ReadFailure
    WriteFailure
    FramingError
    ProtocolViolation
    SessionTimeout
    SubscriberQueueOverflow
    ListenerFailure

These failures must be classified separately from exchange market-data failures.

### 27.2 Individual Session Failure

A failed subscriber socket should affect only that subscriber whenever possible.

Required behavior:

    Session Failure
          |
          v
    Fence Session Generation
          |
          v
    Stop Further Writes
          |
          v
    Record Delivery Boundary
          |
          v
    Release Session Resources
          |
          v
    Preserve Other Subscribers

The underlying source stream remains active when required by other consumers.

### 27.3 Listener Failure

A Unix socket listener failure may prevent new subscriber admission.

Existing accepted sessions may remain operational depending on the failure and transport implementation.

The Publisher must distinguish:

    ListenerUnavailable

from:

    ExistingSessionsUnavailable

The Recovery Coordinator may request listener restoration when appropriate.

### 27.4 Socket Path Recovery

After an unclean runtime shutdown, the socket filesystem entry may remain.

The server must verify endpoint ownership before removing a stale socket path.

An existing socket must not be deleted solely because a previous runtime instance is believed to have terminated.

### 27.5 Framing Failure

Malformed length prefixes or incomplete frames must not produce partially decoded canonical events.

The session may be terminated according to the IPC protocol policy.

Framing failures must not corrupt unrelated subscriber sessions.

### 27.6 Protocol Failure

Unsupported protocol versions or invalid control messages must result in explicit protocol errors.

Repeated protocol violations may cause session termination.

The Publisher must not attempt to interpret unknown bytes as canonical market data.

### 27.7 Delivery Failure vs Source Failure

The Publisher must preserve the distinction:

    IPC Delivery Failed

and:

    Exchange Acquisition Failed

A local subscriber disconnection does not invalidate a healthy source stream.

Likewise, source-stream recovery does not automatically restore an interrupted subscriber session.

### 27.8 Recovery Guarantees

After subscriber reconnection, the Publisher may establish:

    TransportRestored

    SubscriptionRestored

    DeliveryRestored

These guarantees must be evaluated independently.

For example:

    Unix Socket Connected = true

    IPC Subscription Active = true

    Missed Events Replayed = false

This represents restored current delivery with incomplete historical subscriber coverage.

### 27.9 Recovery Escalation

Repeated session failures remain local unless they indicate a broader Publisher or runtime problem.

Examples of broader failures include:

- Listener repeatedly failing.
- Widespread socket write errors.
- Publisher-wide queue exhaustion.
- Serialization incompatibility affecting many clients.
- Filesystem permissions changing unexpectedly.
- Runtime file-descriptor exhaustion.

Such conditions are reported to the Recovery Coordinator.

---

## 28. Replay and Handoff Invariants

The following invariants apply to snapshot initialization, cursor replay, and subscriber reconnection.

### 28.1 L2 Initialization

    A subscriber reconstructing L2 state
    must receive an authoritative snapshot
    before dependent deltas.

    Snapshot and deltas must belong to
    compatible book generations.

    No committed transition may disappear
    between snapshot and live delivery.

### 28.2 Cursor Replay

    Replay preserves original canonical
    publication identities.

    Replay does not create new native
    execution identities.

    Replay is limited by actual retention
    availability.

    Expired cursors produce explicit errors.

### 28.3 Replay Handoff

    Replay and live delivery must share
    one consistent cursor boundary.

    Overlapping replay and live events
    must not be delivered twice within
    one authoritative delivery sequence.

    No required cursor may be skipped.

### 28.4 Subscriber Reconnection

    A new Unix socket session receives
    a new session generation.

    Obsolete session operations cannot
    modify replacement session state.

    Reconnection alone does not establish
    historical delivery completeness.

### 28.5 Source Independence

    Subscriber failure does not
    automatically invalidate source data.

    Subscriber reconnection does not
    automatically trigger exchange resubscription.

### 28.6 Resource Safety

    Handoff buffers remain bounded.

    Replay buffers remain bounded.

    Failed handoffs release resources.

    Slow subscribers cannot cause
    unbounded Publisher memory growth.

---

## 29. Canonical Event Serialization

The Publisher must serialize accepted canonical events into a representation suitable for efficient local interprocess communication.

Serialization occurs after canonical normalization and integrity validation.

The serialization layer must not reinterpret exchange-native fields or modify the economic meaning of accepted events.

### 29.1 Serialization Responsibilities

The serialization subsystem must:

- Encode canonical market-data events.
- Preserve exact prices and quantities.
- Preserve event timestamps.
- Preserve source-stream identities.
- Preserve publication cursors.
- Preserve relevant integrity metadata.
- Preserve L2 book-generation information.
- Support protocol versioning.
- Validate outgoing message sizes.
- Minimize unnecessary allocations.
- Reject unsupported canonical representations.

Serialization must remain independent of Unix socket framing.

### 29.2 Serialization Architecture

The processing pipeline is:

    Canonical Event
          |
          v
    Event Serialization
          |
          v
    Serialized Payload
          |
          v
    IPC Message Envelope
          |
          v
    Length-Prefixed Frame
          |
          v
    Subscriber Outbound Queue
          |
          v
    Unix Socket Writer

The serialization stage produces a complete IPC message payload.

The framing stage adds the transport-specific length prefix.

### 29.3 Initial Serialization Strategy

The recommended production strategy is:

    Versioned Binary Serialization

The initial implementation should use a compact binary representation compatible with the canonical domain contracts defined in **02 — Domain Models and Contracts**.

The exact serialization library or schema system must be selected during implementation.

Potential requirements include:

- Rust support.
- Python decoder support where required.
- Explicit schema evolution.
- Deterministic encoding.
- Efficient decoding.
- Stable numerical representations.
- Bounded allocation behavior.

The architecture must not assume that Rust's internal memory layout is a stable IPC representation.

### 29.4 Binary Compatibility

Raw Rust structs must not be transmitted by directly copying their in-memory bytes unless an explicitly defined binary ABI guarantees:

- Field layout.
- Field alignment.
- Endianness.
- Padding.
- Numerical representation.
- Version compatibility.

The initial design should use explicit serialization.

This avoids accidental dependence on compiler-specific or architecture-specific memory layouts.

### 29.5 Canonical Event Types

The initial serialized event variants are:

    Trade
    L2Snapshot
    L2Update

Each variant must have an unambiguous type identifier.

The decoder must not infer event type from payload length or field presence alone.

### 29.6 SerializedEvent

Conceptual representation:

    SerializedEvent {
        schema_version
        event_type

        stream_id
        cursor_identity

        event_timestamp
        received_timestamp

        configuration_revision
        metadata_revision

        payload
    }

L2 events additionally require the applicable book-generation and transition identity.

The exact field placement must remain compatible with the canonical event envelope.

### 29.7 Canonical Event Identity

The serialized event must preserve the event identity assigned at canonical publication.

Conceptually:

    EventIdentity {
        stream_id
        cursor_epoch
        stream_cursor
    }

The Publisher must not generate a new canonical event identity for every subscriber.

The same accepted event distributed to multiple subscribers retains one canonical identity.

### 29.8 Serialization Failure

Serialization may fail because of:

- Unsupported schema variant.
- Invalid numerical representation.
- Missing required canonical fields.
- Frame-size overflow.
- Unsupported protocol version.
- Internal encoding failure.

A serialization failure must produce an explicit error.

The Publisher must not silently omit an accepted event and continue claiming uninterrupted delivery.

---

## 30. Exact Numerical Representation

Canonical prices and quantities must retain the precision established by the normalization subsystem.

IPC serialization must not introduce new rounding or floating-point conversion.

### 30.1 Numerical Requirements

The serializer must preserve:

- Price.
- Base quantity.
- Quote quantity.
- Contract-derived quantities where included.
- Book-level quantities.
- Native identifiers.
- Native sequence identifiers.
- Event timestamps.

The serialization format must not require conversion through `f32` or `f64`.

### 30.2 Decimal Encoding

The initial recommended binary decimal representation is:

    Decimal {
        coefficient
        scale
    }

Conceptually:

    value = coefficient × 10^(-scale)

For example:

    65000.25

may be represented as:

    coefficient = 6500025
    scale = 2

The actual coefficient width must support the canonical decimal contract.

Overflow and unsupported scales must be rejected.

### 30.3 Decimal Equivalence

Numerically equivalent decimal values may have different internal scales.

For example:

    1.0

and:

    1.00

The serializer must follow the canonical decimal representation policy.

If canonical decimals are normalized before serialization, equivalent values should produce equivalent numerical representations.

Preserving original exchange formatting belongs to source provenance, not necessarily the primary canonical quantity representation.

### 30.4 Timestamp Encoding

The initial canonical timestamp representation is:

    Unix Nanoseconds

The IPC schema must preserve signedness and numerical width consistently.

The decoder must not infer timestamp units from magnitude.

The schema version defines the unit.

### 30.5 Native Identifiers

Native trade and sequence identifiers may require:

    UnsignedInteger

    SignedInteger

    OpaqueString

The serializer must preserve their identity semantics.

A string identifier that contains digits must not automatically become an integer.

### 30.6 Endianness

All fixed-width binary numerical fields must have a documented byte order.

The initial recommended convention is:

    LittleEndian

for canonical binary payload fields, subject to the selected serialization format.

The Unix socket length prefix remains:

    BigEndian u32

as defined in Section 7.

Framing and payload endianness are independent protocol choices.

The finalized serialization schema must make both explicit.

### 30.7 Precision Testing

The serialization implementation must test:

- Very small decimal quantities.
- Large prices.
- Maximum supported coefficients.
- Positive and negative scales where supported.
- Fractional contract quantities.
- Nanosecond timestamps.
- Large native trade identifiers.
- Exact round-trip equivalence.

A serialized event must decode into the same canonical economic values.

---

## 31. IPC Schema Versioning

The IPC schema must support controlled evolution without silently breaking existing consumers.

Schema evolution applies to both control messages and canonical event payloads.

### 31.1 Version Dimensions

MarketForge distinguishes:

    IPC Protocol Version

    Canonical Event Schema Version

    Serialization Format Version

These versions may evolve independently.

The IPC protocol version defines message exchange and session behavior.

The canonical schema version defines market-data event structure.

The serialization version identifies the encoding contract where necessary.

### 31.2 Protocol Version

Conceptually:

    ProtocolVersion {
        major
        minor
    }

An incompatible major version requires explicit rejection.

Compatible minor versions may negotiate a shared capability set.

The initial implementation must not silently reinterpret unknown mandatory fields.

### 31.3 Canonical Schema Version

Canonical event schemas are defined by document 02.

The Publisher must advertise the canonical schema versions it can transmit.

A subscriber must not receive events encoded under a schema it cannot interpret.

### 31.4 Schema Compatibility

Potential compatible changes include:

- Adding optional fields.
- Adding explicitly ignorable metadata.
- Introducing new optional capabilities.

Potential breaking changes include:

- Changing numerical units.
- Changing identifier semantics.
- Removing required fields.
- Reinterpreting an existing event variant.
- Changing cursor identity semantics.
- Changing L2 generation semantics.

Compatibility must be evaluated semantically, not merely structurally.

### 31.5 Unknown Event Variants

A subscriber receiving an unsupported event variant must not interpret it as another known event type.

The negotiated protocol determines whether unknown optional variants may be skipped.

If the subscription requires an unsupported variant, activation must fail.

### 31.6 Version Negotiation

During IPC negotiation, the server and client establish:

    IPC Protocol Version

    Serialization Format

    Canonical Schema Version

    Supported Event Types

    Delivery Capabilities

A session must not begin canonical event delivery until the required compatibility checks succeed.

### 31.7 Runtime Schema Changes

A running subscriber session must not silently switch to an incompatible event schema.

A breaking schema change requires:

    New Negotiation

or:

    Session Replacement

The exact upgrade procedure is determined by the finalized IPC protocol.

### 31.8 Configuration Revisions

A canonical event's configuration revision identifies the processing rules used to produce it.

It is not a substitute for schema versioning.

For example:

    Configuration Revision 12

may produce events using:

    Canonical Schema Version 1

These identities serve different purposes.

---

## 32. Serialization and Fan-Out Efficiency

One canonical event may be delivered to many subscribers.

The Publisher should avoid serializing and copying that event independently for every subscriber when their negotiated representations are identical.

### 32.1 Shared Serialization

Conceptually:

    Canonical Trade
          |
          v
    Serialize Once
          |
          v
    Immutable Serialized Frame
          |
       +--+--+--+
       |  |  |  |
       v  v  v  v
       A  B  C  D

The frame may be shared through immutable reference-counted storage.

Each subscriber retains independent delivery state.

### 32.2 Serialization Cache Identity

A serialized representation may be reused only when the following properties match:

    Canonical Event Identity

    Serialization Format

    Canonical Schema Version

    Required Capabilities

Subscriber-specific message fields must not accidentally be embedded in a shared event payload.

### 32.3 Immutable Event Buffers

Shared serialized event buffers must be immutable.

Conceptually:

    Arc<SerializedEventBuffer>

The precise Rust representation is an implementation detail.

A subscriber must not modify a shared event buffer.

### 32.4 Per-Subscriber Framing

Even when serialized payload bytes are shared, each subscriber writer maintains its own:

- Queue membership.
- Write progress.
- Session generation.
- Delivery frontier.
- Error state.

One subscriber's partial socket write must not affect another subscriber's frame.

### 32.5 Serialization Variants

If subscribers negotiate different schema versions or serialization formats, the Publisher may need multiple encoded variants of one canonical event.

Conceptually:

    Canonical Event
           |
       +---+---+
       |       |
       v       v
    Binary V1 Binary V2
       |       |
       v       v
    Group A   Group B

The Publisher must bound the number and memory cost of retained serialization variants.

### 32.6 Serialization Cache Lifetime

Shared serialized buffers may remain alive while referenced by:

- Subscriber outbound queues.
- Pending socket writes.
- Replay operations.
- Other explicitly registered consumers.

They must be released after their final reference is removed.

The Publisher must not retain every serialized event indefinitely.

### 32.7 Copying and Allocation

The implementation should minimize:

- Repeated canonical serialization.
- Repeated heap allocation.
- Full payload cloning.
- Unnecessary string conversion.
- Per-subscriber decimal formatting.
- Large temporary frame buffers.

Performance optimization must not weaken delivery integrity or protocol correctness.

### 32.8 Slow Subscriber Isolation

A slow subscriber may retain references to serialized events for longer than other consumers.

Those references count toward configured queue and memory budgets.

Reference counting must not become a mechanism for bypassing bounded subscriber resource limits.

---

## 33. IPC Security Model

Unix domain sockets provide local communication but do not eliminate the need for access control.

The Publisher must treat connected clients as independent processes with potentially different trust levels.

### 33.1 Security Objectives

The IPC subsystem must:

- Restrict unauthorized local access.
- Validate peer identity where supported.
- Protect socket ownership.
- Enforce resource limits.
- Prevent malformed-frame memory exhaustion.
- Prevent unauthorized source access.
- Avoid exposing credentials.
- Prevent session impersonation.
- Isolate subscriber failures.
- Validate all control messages.

### 33.2 Local Trust Boundary

A process running on the same machine is not automatically trusted.

The default access policy should be:

    SameUser

More permissive access requires explicit configuration.

Filesystem permissions and operating-system peer credentials should be used where available.

### 33.3 Peer Credential Validation

On supported Unix platforms, peer credentials may include:

    UID
    GID
    PID

The server must obtain credentials from the operating system.

Client-supplied identity fields must not be treated as authoritative operating-system credentials.

The precise implementation is platform-dependent.

### 33.4 Socket Filesystem Permissions

The Publisher must:

- Validate its runtime directory.
- Use restrictive directory permissions.
- Bind only to an authorized socket path.
- Avoid following unsafe filesystem ownership assumptions.
- Protect against accidental deletion of unrelated files.
- Remove only socket entries it owns.

A stale socket entry must not be deleted without verifying ownership and liveness.

### 33.5 Socket Path Attacks

The server must consider:

- Preexisting filesystem entries.
- Symbolic links.
- Path substitution.
- Permission changes.
- Multiple publisher instances.
- Stale runtime files.

The initial implementation should use a controlled runtime directory and an ownership lock or equivalent mechanism.

### 33.6 Control Message Validation

Every client control message must be validated before execution.

Validation includes:

- Supported message type.
- Supported protocol version.
- Valid request identifier.
- Valid field types.
- Valid stream identity.
- Permitted operation.
- Resource limits.
- Session-generation compatibility.

Malformed messages must not trigger uncontrolled allocations or state mutations.

### 33.7 Source Authorization

A subscriber may be authorized to connect to the Publisher without being authorized to access every source stream.

The initial single-user deployment may use a simple access policy.

The architecture should preserve a distinct authorization decision for:

    ConnectToPublisher

and:

    SubscribeToSource

Future multi-user deployments may require more granular source permissions.

### 33.8 Session Impersonation

A reconnecting client must not take over another subscriber's delivery state merely by supplying the same client name.

Persistent subscriber identity requires an explicit authentication or authorization contract.

The initial ephemeral-session design avoids assuming that a client-provided name is a secure identity.

### 33.9 Denial-of-Service Protection

The Publisher must enforce:

- Maximum connected subscribers.
- Handshake timeouts.
- Maximum frame size.
- Maximum control-message rate.
- Maximum pending requests.
- Maximum subscription count.
- Maximum outbound queue size.
- Maximum replay concurrency.
- Bounded serialization resources.

A malicious or malfunctioning local client must not cause unbounded Publisher memory consumption.

### 33.10 Information Exposure

Subscriber-facing payloads must not expose:

- Exchange API keys.
- Authentication tokens.
- Database credentials.
- Internal secrets.
- Private configuration fields.
- Unrelated subscriber identities.

Canonical event provenance should contain only the information required by the event contract.

Operational diagnostics must not leak sensitive configuration values.

---

## 34. IPC Fault Isolation

The Publisher must isolate malformed or overloaded subscriber sessions from healthy consumers.

### 34.1 Session-Level Failures

Examples:

    MalformedFrame
    UnsupportedProtocol
    UnauthorizedRequest
    SubscriberQueueOverflow
    SocketWriteFailure
    AcknowledgementTimeout

These failures should remain scoped to the affected session whenever possible.

### 34.2 Publisher-Level Failures

Examples:

    ListenerUnavailable
    GlobalQueueExhaustion
    SerializationSubsystemFailure
    RuntimeFileDescriptorExhaustion
    InternalRoutingCorruption

These may affect multiple subscribers and require system-level recovery coordination.

### 34.3 Failure Escalation

The Publisher reports failures through the common runtime failure interface.

The Recovery Coordinator determines broader recovery and escalation.

The Publisher remains responsible for:

- Session disconnection.
- Session resource cleanup.
- Delivery boundary reporting.
- Subscriber reconnection handling.
- Replay coordination.

### 34.4 Security Failure Reporting

Security-related failures must be logged locally without exposing sensitive client data unnecessarily.

Examples:

    UnauthorizedPeerRejected
    InvalidControlMessage
    ProtocolNegotiationFailed
    SubscriberResourceLimitExceeded

Repeated security failures may trigger rate limiting or temporary admission restrictions according to runtime policy.

### 34.5 Security and Correctness Invariant

An unauthorized or malformed client must never be able to:

    Modify Canonical Event Contents

    Mutate BookStore State

    Bypass Stream Registry Admission

    Access Unrelated Subscriber Sessions

    Bypass Publisher Resource Limits

    Claim Unsupported Delivery Guarantees

---

## 35. Publisher Lifecycle

The Publisher owns the lifecycle of canonical-event distribution and local subscriber connectivity.

Its lifecycle is independent of exchange WebSocket connections, native subscriptions, trade processing, and L2 synchronization.

The Publisher may be operational while no exchange streams are active.

Likewise, exchange acquisition may remain operational while the Publisher is degraded.

### 35.1 Publisher States

Supported states:

    Created
    Starting
    Listening
    Degraded
    Stopping
    Stopped
    Failed

| State | Meaning |
|---|---|
| Created | Publisher constructed but not started |
| Starting | IPC transport and routing infrastructure initializing |
| Listening | Publisher accepting subscriber connections and distributing eligible events |
| Degraded | Some required delivery capabilities are impaired |
| Stopping | Graceful shutdown in progress |
| Stopped | Publisher resources released |
| Failed | Nonrecoverable Publisher failure |

Publisher lifecycle transitions must be explicit and validated.

### 35.2 Startup Sequence

    Runtime Initialization
              |
              v
    Load Publisher Configuration
              |
              v
    Validate IPC Protocol Settings
              |
              v
    Initialize Subscriber Registry
              |
              v
    Initialize Routing Infrastructure
              |
              v
    Validate Socket Endpoint
              |
              v
    Bind Unix Socket Listener
              |
              v
    Start Listener Supervisor
              |
              v
    Begin Subscriber Admission
              |
              v
           Listening

The Publisher must not advertise readiness before the IPC listener and required routing infrastructure are operational.

### 35.3 Startup Validation

Startup must validate:

- Unix socket path.
- Runtime directory ownership.
- Filesystem permissions.
- Supported IPC protocol versions.
- Supported serialization formats.
- Canonical event schema compatibility.
- Subscriber admission limits.
- Frame-size limits.
- Queue capacities.
- Required transport capabilities.

Invalid configuration must prevent activation.

### 35.4 Publisher Independence

The Publisher must not require active source streams to start.

For example:

    Publisher = Listening

    Active Source Streams = 0

This is a valid runtime condition.

Subscribers may connect and inspect available sources or wait for permitted source activation.

### 35.5 Source Processing Independence

Publisher failure does not automatically invalidate exchange-native market data or BookStore synchronization.

However, failure to distribute accepted canonical events may violate required publication or retention guarantees.

The affected guarantees must be reported independently.

For example:

    Source Acquisition = Operational

    Canonical Processing = Operational

    Subscriber Delivery = Unavailable

The Recovery Coordinator determines system-level escalation.

---

## 36. Graceful Shutdown

Publisher shutdown must release subscriber sessions, queues, routing memberships, and Unix socket resources deterministically.

Shutdown must have a finite deadline.

### 36.1 Shutdown Sequence

    Shutdown Requested
              |
              v
    Stop New Subscriber Admission
              |
              v
    Stop New Subscription Commands
              |
              v
    Establish Publication Shutdown Boundary
              |
              v
    Notify Active Subscribers
              |
              v
    Drain or Cancel Pending Deliveries
              |
              v
    Fence Subscriber Sessions
              |
              v
    Close Unix Socket Connections
              |
              v
    Stop Listener
              |
              v
    Release Subscriber Registry
              |
              v
    Remove Owned Socket Entry
              |
              v
            Stopped

The exact drain behavior follows the configured delivery policy.

### 36.2 Shutdown Boundary

The Publisher must establish a clear boundary between:

    Events Accepted Before Shutdown

and:

    Events Not Accepted After Shutdown

Events admitted before shutdown remain subject to their committed publication guarantees.

The Publisher must not silently abandon them while reporting a clean shutdown.

### 36.3 Subscriber Notification

Where supported by the negotiated IPC protocol, subscribers should receive a shutdown notification.

Conceptually:

    ServerClosing {
        reason
        last_available_cursor
        reconnect_policy
    }

The notification is advisory.

A process crash may prevent its delivery.

Subscribers must not depend on receiving a graceful shutdown message.

### 36.4 Queue Draining

The shutdown policy determines whether pending subscriber queues are drained.

Supported strategies:

    DrainWithinDeadline

    CancelWithExplicitBoundary

Draining must respect a configured deadline.

A subscriber that stops reading must not prevent the Publisher from terminating indefinitely.

### 36.5 Unacknowledged Deliveries

Application-level acknowledgements may remain outstanding during shutdown.

The Publisher must distinguish:

    Written

from:

    Acknowledged

Unacknowledged events must not be reported as confirmed application processing.

Where supported, the subscriber may resume from its last acknowledged cursor after reconnection.

### 36.6 Session Termination

Every subscriber session must:

- Stop accepting commands.
- Stop receiving new delivery assignments.
- Resolve pending writes.
- Fence its session generation.
- Release queued event references.
- Release routing memberships.
- Close the Unix socket.
- Report final delivery diagnostics.

Session cleanup must be idempotent.

### 36.7 Socket Cleanup

The Publisher removes only socket filesystem entries it owns.

The cleanup procedure must verify endpoint ownership.

It must not delete a socket belonging to another active process.

Unexpected shutdown may leave a stale entry, which is handled during subsequent startup.

### 36.8 Immediate Shutdown

An immediate shutdown may cancel pending deliveries.

The runtime must not claim that cancelled events were successfully transmitted or acknowledged.

Any resulting delivery uncertainty must remain explicit where the relevant state survives shutdown.

---

## 37. Publisher Restart Recovery

After process restart, the Publisher must reconstruct its runtime without treating previous subscriber transport sessions as active.

### 37.1 Restart Procedure

    Runtime Restart
          |
          v
    Load Publisher Configuration
          |
          v
    Validate Socket Ownership
          |
          v
    Restore IPC Listener
          |
          v
    Establish New Runtime Instance
          |
          v
    Accept New Subscriber Sessions
          |
          v
    Negotiate Protocol
          |
          v
    Resume Delivery Through New Sessions

Previous Unix socket connections cannot survive a Publisher process restart.

### 37.2 Session State

The initial implementation does not require persistent subscriber-session state.

Subscribers reconnect and explicitly request their streams.

Clients requiring replay provide valid cursor identities.

### 37.3 Cursor Epochs

Publisher restart must not create ambiguous publication identities.

The canonical cursor contract must either:

- Restore a validated persistent cursor frontier.
- Establish a new cursor epoch.
- Use another explicitly defined restart-safe identity.

A subscriber must not interpret a cursor from one epoch as belonging to another without validation.

### 37.4 Retention Availability

After restart, replay availability depends on the Live Retention Manager.

Some events may remain available in recovered Parquet segments.

Others may have existed only in memory and been lost during the crash.

The Publisher must query authoritative retention availability.

It must not assume complete replay merely because the previous runtime accepted the events.

### 37.5 Subscriber Reconnection

A reconnecting subscriber must:

- Establish a new Unix socket session.
- Negotiate a compatible protocol.
- Reestablish downstream subscriptions.
- Request replay or a fresh live boundary.
- Validate any required L2 snapshot initialization.

The Publisher must not silently claim continuity with the previous session.

---

## 38. Publisher Observability

The Publisher exposes operational metrics and diagnostics through MarketForge's internal Metrics Registry, runtime API, and TUI.

No external monitoring stack is required.

### 38.1 Publisher Health

Conceptual representation:

    PublisherHealth {
        runtime_instance_id
        publisher_state

        listener_state
        socket_path

        active_subscribers
        active_downstream_subscriptions

        pending_publication_events
        total_queued_bytes

        active_replay_sessions
        degraded_subscribers

        last_error
        evaluated_at
    }

The health snapshot must not expose sensitive operating-system information unnecessarily.

### 38.2 Publisher Metrics

| Metric | Description |
|---|---|
| `publisher_events_received_total` | Accepted canonical events received |
| `publisher_events_committed_total` | Events committed to Publisher distribution |
| `publisher_events_serialized_total` | Serialized canonical events |
| `publisher_serialization_errors_total` | Serialization failures |
| `publisher_active_subscribers` | Connected admitted subscribers |
| `publisher_active_subscriptions` | Active downstream subscriptions |
| `publisher_pending_events` | Events pending distribution |
| `publisher_queue_bytes` | Aggregate queued event memory |
| `publisher_backpressure_total` | Publisher backpressure incidents |
| `publisher_delivery_failures_total` | Failed delivery operations |
| `publisher_replay_sessions` | Active replay sessions |

Counters and gauges must use appropriate scopes.

### 38.3 Subscriber Metrics

| Metric | Description |
|---|---|
| `subscriber_connections_total` | Subscriber sessions established |
| `subscriber_disconnections_total` | Subscriber sessions terminated |
| `subscriber_admission_rejections_total` | Rejected connection attempts |
| `subscriber_subscriptions_active` | Active source subscriptions per subscriber |
| `subscriber_queue_messages` | Current queued message count |
| `subscriber_queue_bytes` | Current queue memory |
| `subscriber_frames_written_total` | Complete IPC frames written |
| `subscriber_bytes_written_total` | Bytes transmitted |
| `subscriber_write_failures_total` | Socket write failures |
| `subscriber_delivery_latency` | Queue-to-write latency |
| `subscriber_acknowledgement_latency` | Write-to-acknowledgement latency |
| `subscriber_delivery_gaps_total` | Detected delivery discontinuities |

Metric labels must avoid unbounded cardinality.

Highly detailed per-session information may be exposed through diagnostic snapshots instead.

### 38.4 Replay Metrics

| Metric | Description |
|---|---|
| `replay_requests_total` | Replay requests received |
| `replay_accepted_total` | Accepted replay requests |
| `replay_rejected_total` | Rejected replay requests |
| `replay_events_delivered_total` | Replayed events transmitted |
| `replay_duration` | Replay session duration |
| `replay_handoff_failures_total` | Replay-to-live handoff failures |
| `replay_cursor_expired_total` | Requests outside retention |
| `l2_handoff_retries_total` | L2 initialization retries |
| `l2_handoff_failures_total` | Failed L2 snapshot-to-live handoffs |

### 38.5 TUI Integration

The Ratatui dashboard may display:

    Publisher State
    Connected Subscribers
    Active IPC Subscriptions
    Events Per Second
    Outbound Throughput
    Queued Bytes
    Slow Subscribers
    Delivery Failures

A subscriber detail view may show:

    Subscriber Identity
    Session Generation
    Connected Since
    Subscribed Streams
    Delivery Mode
    Queue Usage
    Last Written Cursor
    Last Acknowledged Cursor
    Replay Status
    Last Error

The TUI must not directly modify Publisher internals.

Supported commands must pass through the Publisher's control interface.

### 38.6 Local Structured Logging

Relevant events include:

    PublisherStarted
    PublisherStopped
    SubscriberConnected
    SubscriberDisconnected
    SubscriberAdmissionRejected
    SubscriberSubscribed
    SubscriberUnsubscribed
    SubscriberQueueSaturated
    SubscriberDeliveryFailed
    ReplayStarted
    ReplayCompleted
    ReplayFailed
    IPCProtocolViolation

Operational logs remain local.

They must not include API credentials, database credentials, or unnecessary sensitive peer information.

---

## 39. IPC and Publisher Testing Strategy

The Publisher requires deterministic tests covering framing, serialization, subscriber lifecycle, event routing, ordering, backpressure, replay, and restart recovery.

Core correctness tests must run without exchange connectivity.

### 39.1 Testing Categories

| Category | Scope |
|---|---|
| Framing tests | Length prefixes, partial reads, oversized frames |
| Serialization tests | Canonical event encoding and decoding |
| Negotiation tests | Protocol and schema compatibility |
| Admission tests | Subscriber limits and local authorization |
| Routing tests | Stream-to-subscriber delivery |
| Ordering tests | Per-stream cursor preservation |
| Backpressure tests | Slow consumers and queue saturation |
| L2 handoff tests | Snapshot-to-live consistency |
| Replay tests | Cursor replay and live handoff |
| Reconnection tests | Session generations and delivery restoration |
| Security tests | Invalid clients and unauthorized operations |
| Fault-injection tests | Socket failures and Publisher resource exhaustion |
| Property-based tests | Arbitrary subscription and delivery sequences |
| Performance tests | Throughput, latency, memory, and fan-out |

### 39.2 Test Infrastructure

Tests should use:

- Local Unix socket clients.
- Mock canonical event producers.
- Mock Stream Registry interfaces.
- Mock Live Retention Manager.
- Deterministic clocks.
- Controlled event scheduling.
- Bounded test queues.
- Injected socket failures.
- Configurable slow subscribers.

Tests must not depend on arbitrary sleeps to establish correctness.

---

## 40. Framing and Serialization Tests

### 40.1 Framing Tests

Verify:

- Complete single-frame decoding.
- Multiple frames in one socket read.
- One frame split across multiple reads.
- Partial length-prefix reception.
- Partial payload reception.
- Zero-length frame rejection.
- Oversized frame rejection.
- Invalid length-prefix handling.
- Connection closure during frame reception.
- Frame ordering preservation.

### 40.2 Serialization Round Trips

Verify exact encode/decode equivalence for:

    Trade

    L2Snapshot

    L2Update

Include:

- Large native identifiers.
- Small decimal quantities.
- Large prices.
- Fractional contract quantities.
- Nanosecond timestamps.
- Optional provenance attributes.
- Book-generation identifiers.
- Cursor epochs.

No numerical precision may be lost during serialization.

### 40.3 Protocol Compatibility

Verify:

- Compatible protocol negotiation.
- Incompatible major-version rejection.
- Supported minor-version negotiation.
- Unsupported required capability rejection.
- Unsupported event-schema rejection.
- Unknown optional-field handling.
- Breaking schema-change rejection.

### 40.4 Serialization Failure

Inject:

- Unsupported event variant.
- Invalid canonical payload.
- Unrepresentable numeric value.
- Frame-size overflow.
- Encoding failure.

The Publisher must report the failure without silently omitting an accepted event.

---

## 41. Subscriber and Routing Tests

### 41.1 Subscriber Admission

Verify:

- Successful same-user admission.
- Unauthorized peer rejection.
- Maximum subscriber limit.
- Concurrent admission requests.
- Handshake timeout.
- Failed negotiation cleanup.
- Session resource reservation release.

### 41.2 Subscription Lifecycle

Verify:

- Subscribe to an active source.
- Subscribe to an unavailable source.
- Duplicate subscribe request.
- Conflicting subscription replacement.
- Unsubscribe.
- Repeated unsubscribe.
- Disconnect during activation.
- Session replacement.
- Source removal during subscription.

### 41.3 Source Deduplication

Simulate multiple subscribers requesting one source stream.

Verify:

    Subscriber A
    Subscriber B
    Subscriber C

all reference:

    One Logical Source Stream

The Publisher must not create redundant exchange-native subscriptions.

### 41.4 Routing Correctness

Verify:

- Correct StreamId routing.
- No delivery to unsubscribed sessions.
- No duplicate routing membership.
- Per-stream event ordering.
- Multi-stream interleaving.
- Generation fencing.
- Routing changes during publication.
- Activation and removal boundaries.

### 41.5 Subscriber Isolation

Simulate one subscriber that stops reading.

Verify that other healthy subscribers continue receiving events.

The slow subscriber's bounded queue must not grow indefinitely.

---

## 42. Delivery and Backpressure Tests

### 42.1 Delivery Boundaries

Verify that the Publisher distinguishes:

    PublisherCommitted
    SubscriberQueued
    SocketWritten
    SubscriberAcknowledged

A successful write must not automatically advance the acknowledged cursor.

### 42.2 Cursor Tracking

Verify:

- Unique canonical event identities.
- Monotonic per-stream cursors.
- Independent cursors for multiple streams.
- Correct queued cursor.
- Correct written cursor.
- Correct acknowledged cursor.
- Invalid acknowledgement rejection.
- Obsolete session acknowledgement rejection.

### 42.3 Queue Saturation

Fill a subscriber queue while preventing socket writes.

Verify:

- Queue limits are enforced.
- Memory remains bounded.
- The selected delivery policy is applied.
- No silent event skipping occurs.
- Delivery interruption is reported.
- Queue resources are released after disconnection.

### 42.4 Publisher-Wide Backpressure

Simulate many slow subscribers simultaneously.

Verify:

- Global queue budgets.
- Subscriber isolation.
- Bounded serialization buffers.
- Restricted new admission.
- Correct failure escalation.
- No uncontrolled memory growth.

### 42.5 Socket Write Failure

Simulate:

    Event queued.

    Partial socket write.

    Connection closes.

Expected:

- Delivery outcome remains explicit.
- The event is not incorrectly marked acknowledged.
- The session terminates safely.
- Other sessions remain unaffected.
- Replay may be offered when retention permits it.

---

## 43. L2 Handoff and Replay Tests

### 43.1 L2 Snapshot Handoff

Simulate:

    BookVersion 100 committed.

    Subscriber requests snapshot.

    BookVersion 101 committed.

    BookVersion 102 committed.

Verify that the subscriber receives:

    Snapshot at a consistent version.

    Every required subsequent delta.

No update may disappear between snapshot creation and routing activation.

### 43.2 Book Generation Change

Simulate a sequence gap during subscriber initialization.

Expected:

- Old-generation handoff cancelled.
- New book generation established.
- Fresh snapshot requested.
- No incompatible delta delivered.

### 43.3 Replay-to-Live Handoff

Simulate:

    Replay through Cursor 100.

    Live publication reaches Cursor 105.

Verify that the subscriber receives the required events between cursors 101 and 105 exactly once within the authoritative session delivery sequence.

### 43.4 Replay Expiration

Request a cursor older than the retained frontier.

Expected:

    ReplayUnavailable

The Publisher must not silently begin from a newer cursor.

### 43.5 Subscriber Reconnection

Simulate:

    Subscriber receives Cursor 100.

    Socket disconnects.

    Subscriber reconnects.

    Subscriber requests ResumeAfterCursor(100).

Verify correct replay and new session-generation handling.

### 43.6 Ambiguous Delivery

Simulate a socket write that succeeds, followed by disconnection before the application acknowledgement.

Verify that:

- The written cursor advances.
- The acknowledged cursor does not advance.
- Replay may repeat the event.
- Stable event identity permits downstream deduplication.

### 43.7 Retention Failure During Replay

Simulate retained data becoming unavailable during replay.

Expected:

- Replay fails explicitly.
- Continuity is not falsely reported.
- Subscriber state becomes degraded or disconnected according to policy.
- Unrelated subscribers remain operational.

---

## 44. Property-Based Testing

Property-based tests should generate arbitrary subscriber operations and event sequences.

### 44.1 Routing Invariants

    One downstream subscription per
    SubscriberSessionId and StreamId.

    No event delivered to an unauthorized subscriber.

    No event delivered to a removed subscription
    after its authoritative removal boundary.

    Source-stream deduplication is preserved.

### 44.2 Ordering Invariants

    Per-stream cursor order is preserved.

    L2 deltas do not precede their initialization snapshot.

    No obsolete book-generation delta enters
    a newer reconstructed book.

    Replay-to-live handoff does not skip
    required retained cursors.

### 44.3 Delivery Invariants

    Queued does not imply written.

    Written does not imply acknowledged.

    Exact delivery guarantees are not claimed
    without supporting protocol evidence.

    Failed deliveries produce explicit outcomes.

### 44.4 Resource Invariants

    Subscriber queues remain bounded.

    Publisher memory budgets remain bounded.

    Failed sessions release owned resources.

    Obsolete session generations cannot mutate
    current subscriber state.

    Shutdown terminates owned tasks.

### 44.5 Protocol Invariants

    Frames have valid boundaries.

    Unsupported protocol versions are rejected.

    Canonical numerical precision is preserved.

    Serialization does not change economic event semantics.

    Malformed clients cannot bypass admission limits.

---

## 45. Performance and Benchmarking

Publisher performance must be evaluated independently of exchange connectivity and market-data normalization.

### 45.1 Required Benchmarks

Benchmark:

- Canonical event serialization.
- IPC framing.
- Unix socket transmission.
- Subscriber admission.
- Routing lookup.
- Event fan-out.
- Shared serialization buffers.
- Per-subscriber queue operations.
- L2 snapshot handoff.
- Cursor replay.
- Replay-to-live transition.

### 45.2 Workload Variation

Benchmarks should vary:

- Number of subscribers.
- Number of source streams.
- Subscriptions per subscriber.
- Events per second.
- Event payload size.
- L2 snapshot depth.
- Subscriber read speed.
- Queue capacity.
- Replay backlog.
- Serialization format.
- Number of schema variants.

### 45.3 Fan-Out Benchmark

A representative benchmark should test:

    One Source Stream
           |
           v
        Publisher
           |
      +----+----+----+
      |    |    |    |
      v    v    v    v
      A    B    C    D

Measure:

- Serialization cost.
- Additional cost per subscriber.
- Total throughput.
- Queue memory.
- Delivery latency.
- Slow-subscriber isolation.

### 45.4 Performance Targets

Concrete throughput and latency targets must be based on measured workloads.

The architecture must not claim arbitrary performance guarantees.

Optimization must preserve ordering, precision, and delivery correctness.

---

## 46. Implementation Acceptance Criteria

The Event Distribution subsystem is complete when the following requirements are satisfied.

### 46.1 Architecture

- Unix domain sockets provide the initial IPC transport.
- Publisher and Subscriber Registry responsibilities are separated.
- Canonical events are distributed independently of exchange protocols.
- Multiple subscribers can share one logical source stream.
- Subscriber subscriptions do not create redundant native subscriptions.
- Publisher, retention, and recording completion semantics remain distinct.

### 46.2 IPC Protocol

- Length-prefixed framing is implemented.
- Partial reads and writes are handled correctly.
- Protocol negotiation is supported.
- Canonical schema versions are validated.
- Exact decimal values are preserved.
- Native identifiers remain lossless.
- Session-generation fencing is enforced.
- Unix socket access control is implemented.

### 46.3 Distribution

- Per-stream publication ordering is preserved.
- Subscriber routing is generation-safe.
- Subscription activation boundaries are explicit.
- Subscription removal boundaries are explicit.
- Subscriber queues are bounded.
- Slow consumers cannot cause unbounded memory growth.
- Delivery failures are reported explicitly.

### 46.4 L2 Delivery

- Synchronized subscribers receive an authoritative snapshot.
- Snapshot-to-live handoff is gap-free within the publication contract.
- Book generations are preserved.
- Obsolete deltas are rejected.
- Initialization buffer overflow is handled explicitly.

### 46.5 Replay

- Cursor-based replay is supported when retention permits it.
- Replay availability is validated.
- Expired cursors produce explicit errors.
- Replay-to-live handoff preserves cursor continuity.
- Replayed events retain original canonical identities.
- Subscriber reconnection establishes new session generations.

### 46.6 Resource Safety

- Subscriber admission limits are enforced.
- Frame sizes are bounded.
- Queue memory is bounded.
- Replay concurrency is bounded.
- Serialization buffers are released deterministically.
- Failed sessions release resources.
- Graceful shutdown has a finite deadline.

### 46.7 Observability

- Publisher health is exposed.
- Subscriber health is exposed.
- Delivery progress is measurable.
- Backpressure is measurable.
- Replay status is available.
- TUI and runtime API expose relevant diagnostics.
- Operational logs remain local.

### 46.8 Testing

- Framing tests pass.
- Serialization round-trip tests pass.
- Protocol negotiation tests pass.
- Subscriber admission tests pass.
- Routing tests pass.
- Ordering tests pass.
- Backpressure tests pass.
- L2 handoff tests pass.
- Replay tests pass.
- Reconnection tests pass.
- Security tests pass.
- Fault-injection tests pass.
- Property-based tests pass.
- Performance benchmarks establish acceptable operating characteristics.

---

## 47. Final Architectural Contract

The Event Distribution subsystem provides local access to accepted canonical market data through Unix domain sockets.

It guarantees:

- Exchange-independent canonical event distribution.
- Explicit versioned IPC framing.
- Stable canonical event identities.
- Per-stream publication ordering.
- Bounded subscriber queues.
- Explicit delivery-stage tracking.
- Generation-safe subscriber sessions.
- Source-stream reuse across multiple subscribers.
- Consistent L2 snapshot initialization.
- Replay where retained events are available.
- Explicit reporting of delivery gaps.
- Deterministic shutdown and resource cleanup.

It does not guarantee:

- That every exchange-native event was acquired.
- Global chronological ordering across independent streams.
- Unlimited replay availability.
- Exactly-once application processing.
- Subscriber delivery after retention expiration.
- Complete historical L2 reconstruction after exchange-side data loss.
- Durable persistence without an applicable retention or recording contract.

The ownership chain is:

    Canonical Trade Processor
              |
              +
    Canonical L2 BookStore
              |
              v
    Accepted Canonical Events
              |
              v
          Publisher
              |
              v
       Routing Registry
              |
              v
    Subscriber Delivery Queues
              |
              v
       IPC Serialization
              |
              v
    Unix Domain Socket Writers
              |
              v
       Local Subscribers

Replay follows a separate path:

    Subscriber Replay Request
              |
              v
          Publisher
              |
              v
      Live Retention Manager
              |
              v
       Retained Canonical Events
              |
              v
      Replay-to-Live Handoff
              |
              v
       Subscriber Delivery

The Publisher owns canonical distribution and subscriber delivery.

The Stream Registry owns logical source streams.

The Connection Manager owns physical exchange connectivity.

The BookStore owns synchronized L2 state.

The Trade Processor owns trade identity and continuity.

The Live Retention Manager owns temporary event retention.

The Recording Manager owns optional PostgreSQL persistence.

These ownership boundaries must remain explicit throughout implementation.

---

**End of Part 7 — Publisher Lifecycle, Shutdown, Observability, Testing, and Acceptance Criteria.**

**End of 08 — Event Distribution and Unix Sockets.**