# Canonical Stream Router

The Router is volatile, transport-independent distribution infrastructure under
`live::stream::router`. It consumes owned canonical publications and delivers
shared immutable payloads directly to Rust consumers. It requires Live's restored
PostgreSQL definition graph; it does not replace registration or persistence.

The public facade consists of `StreamRouter`, `RouterConfig`, `RouterContext`,
`AttachProducerRequest`, `SubscribeRequest`, `ProducerHandle`, and
`SubscriptionHandle`. `RouterPublication` owns canonical `Trade` records or the
owned equivalent of the established `DepthPublicationEvent` contract: shared
`BookChange` slices, original boundaries, baseline, book/acquisition generations,
and timestamp provenance. Trade callbacks preserve accepted integrity metadata
and distinguish recovered records.

Create the Router inside an existing Tokio runtime. Pass the application's
`Arc<StreamGraph>` and shared `Arc<Mutex<DemandRegistry>>` through `RouterContext`.
Subscribe using an independent `DemandOwner`. Subscribe before starting a source
if the initial publication is required. Start a prepared/attached Source through
`SourceRuntimeSupervisor::start_routed(stream_id, executor, &router)`. This obtains
its lease from the existing runtime registry and installs the synchronous Source
publication adapters. Existing `start` and callback-based Source use remain valid.

A trusted runtime owner can instead call `StreamRuntimeRegistry::begin_start`,
obtain `publication_authority`, and pass it to `attach_producer`. Each generation's
lease can be claimed exactly once, even across Router instances. Share one
`Arc<ProducerHandle>` when several in-process callers need the same producer.
Dropping the final producer detaches it; attaching again requires a new runtime
generation. A token from a different persisted definition is rejected even when
its textual stream ID matches.

## Admission and delivery

`ProducerHandle::try_publish` never waits for queue capacity or consumer
processing. It attempts the Router state and runtime-authority guards and returns
`Busy` on contention. The complete publication is validated and admitted, or the
call returns an error without advancing sequence, depth authority, or retained
capacity. Cloning `RouterPublication` is inexpensive (`Arc` payloads); retain a
clone when a direct caller intends to retry rejected admission.

Insertion into bounded ingress is the admission linearization point. Successful
acceptance means complete bounded **volatile Router ownership**. It does not mean
durability, complete fanout, consumer processing, or historical coverage. Source
commits accepted trade identities only after this synchronous acknowledgment.
Source callbacks use a separate bounded lock-acquisition path rather than turning
an immediate `Busy` into a handoff failure. `RouterConfig::source_lock_timeout`
defaults to 50 ms and must be greater than zero and at most one second. Both the
Router state lock and runtime-authority lock share one monotonic deadline. Only
these two private mutexes use timed `parking_lot` locking; the dependency is enabled
by the Live feature. There is no polling/retry loop or extra publication queue.
After both locks are acquired, the same validation and atomic admission body runs
exactly once. Capacity exhaustion, shutdown, and revoked authority remain distinct
errors and are not retried. A lock held beyond the budget returns the explicit
`ContentionTimeout` error without admission; Source still fails closed for this
prolonged stall. No integrity state is committed until the callback returns success.
A synchronous callback can occupy its Tokio worker for the configured acquisition
budget; operating-system scheduling is not a hard real-time guarantee.

One dispatch task processes ingress in order. Each publication is shared by
`Arc` among eligible subscribers. Subscriptions start at their creation boundary;
they do not receive data admitted earlier. Each stream has independent publication
sequences tagged with the runtime generation in acceptance and delivery. These
sequences are ephemeral and are **not** `(StreamId, LogEpoch, offset)` log cursors.
There is no cross-exchange event-time ordering guarantee.

Read `RouterDelivery` using `SubscriptionHandle::recv`. A saturated consumer loses
continuity: its queue is cleared, demand released, and exactly one terminal
`ContinuityLost(ConsumerCapacity)` is returned before EOF. Other consumers continue.
This policy deliberately does not promise end-to-end lossless delivery. Receivers
must handle terminal failures and decide how to rebuild their own state.
`recv` cancellation leaves queued data intact. Explicit `close` and handle drop
remove only that subscription's demand. Independent consumers and AlwaysOn roots
remain owned by the existing demand registry.

A late depth subscriber starts unsynchronized and receives subsequent deltas
marked unsynchronized until an authoritative snapshot is available. The Router
maintains only publication authority, not another order book or snapshot cache.
Invalidation removes book authority; stale acquisition/book deltas are rejected.
Consumers process the ordered depth events inside a batch, including invalidation
and initialization boundaries. The delivery's `synchronized` value describes the
continuity of that consumer's delivered chain after the entire publication. It
does not acknowledge consumer processing. `SubscriptionHandle::status` exposes
lifecycle, synchronization, and retained queue counts/bytes.

Runtime stop, failure, replacement, or registry drop revokes the producer lease.
Revocation serializes with admission and extraction; ingress and consumer queues
cannot deliver stale data across that fence. An idle dispatch task observes lease
revocation without polling. Subscribers receive terminal `ProducerRevoked` and
must subscribe again for a restarted runtime. Source acquisition reconnection
uses its existing independent fencing and depth invalidation/snapshot protocol.

## Resources and lifecycle

`RouterConfig` bounds ingress publications/bytes, each consumer queue's
publications/bytes, active producer bindings, and active subscriptions. Byte
charges conservatively include canonical record storage, allocated strings,
integrity observations, depth levels, shared headers, queue entries, and the
pending stream identity. Shared payloads are charged fully to each queue, so
sharing cannot weaken limits. Configuration and charges use checked arithmetic;
aggregate capacities that cannot be represented are rejected. Consumer identity
components must be nonempty and at most 4096 bytes each. Counts and ownership
state are bounded; no unbounded channels or per-event tasks are used.

Removing a queued delivery releases its Router byte reservation. Memory retained
by a consumer after `recv` is consumer-owned and outside Router queue budgets.
Status reports retained ingress/consumer counts and bytes, accepted publications,
terminal consumer failures, and cleanup errors. Counters saturate at `u64::MAX`.
No consumer callback or financial computation runs on dispatch, and no lock is
held across an await. The context's shared demand mutex must be used for short
registry operations; callers must not reenter the Router while holding it.

`shutdown` stops admissions, revokes Router bindings, cancels undelivered volatile
work, explicitly fails consumers with `RouterShutdown`, releases demand/capacity,
and joins the dispatch task. It reports cleanup or task failure. Router drop
performs the same resource cancellation with `RouterDropped` and aborts the task
as a fallback. Stop Sources before shutting down the Router when coordinating a
service lifetime. During Source stop, the terminal runtime fence supersedes the
final depth invalidation: the adapter acknowledges that cleanup without accepting
fresh market data from a revoked producer.

## Current boundary

There is no persistence/replay, consumer-processed acknowledgment, bootstrap
snapshot service, external serialization, adapter execution, or live exchange
certification. Producer bindings validate the currently implemented Source
definitions. Future Derived producers need their own approved output identity
and payload contract; this milestone implements no derived computation. All
consumers already use the same generic delivery API. The durable graph and queue
limits are fixed for a Router lifetime. No configuration control API is added.
