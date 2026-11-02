# Live derived processing

`ProcessingService` owns the existing Live CPU pool, graph-bound subscriptions,
publication authority and terminal cleanup. Production processing stays under
`live::stream::processing`; it has no Historical execution dependency.

## Public entry points and definitions

Restore a validated `StreamGraph`, create the existing `StreamRouter` and
`StreamRuntimeRegistry`, then create `ProcessingService` with them. To start an
existing Merge or Convert definition:

```rust,ignore
let handle = service.handle();
let stream = handle.start_derived("valuation").await?;
// Read an ordinary Router subscription to "valuation".
handle.stop(&stream).await?;
handle.wait_terminal("valuation", stream.generation).await?;
if let Some(report) = handle.take_report("valuation", stream.generation) {
    report.discard(); // explicit disposal of retained payload ownership
}
```

The general-purpose `StartProcessor` takes `inputs: Vec<ProcessingInputSpec>` and
`output_semantics: PublicationSemantics`. Production constructors select `Derived`.
`CanonicalCompatibility` is an explicit custom-processor migration capability,
restricted to one Source without references. It is not a production Convert result.
Obtain the specifications from `router.processing_inputs(id)` or the persisted
request's `input_specs()`. The complete ordered vector must match the graph,
including ranks and roles. One input is the same path with one vector element.
Factories still receive the managed input mailbox and output capability through
`ProcessorContext`; all turns execute on `marketforge-cpu-*` workers.

`CreateDerivedStreamRequest.inputs` contains data dependencies. The additive
`reference_inputs` field contains required, reference-only dependencies. Positions
are data inputs followed by references, each in declared order. The repository
persists both kinds of dependency rows in the same transaction as the JSON
request. Graph restoration validates the complete positional list and cycles.
Existing requests without references or conversion settings deserialize unchanged.
Convert has one data dependency, which may itself be a mixed Merge. Merge has two
or more data dependencies. References must be last-trade Source streams.
Production Merge rejects `target_quote_asset`: declare a separate Convert stream.

Conversion operation settings are persisted in `conversion`:

```json
{
  "name": "valuation",
  "operation": "convert",
  "inputs": ["merged"],
  "reference_inputs": ["btc_usdt"],
  "target_quote_asset": "USDT",
  "conversion": {
    "max_staleness_ns": 60000000000,
    "max_lateness_ns": 1000000000,
    "max_hops": 3
  }
}
```

The values shown are defaults for omitted settings. Hop limits are 1..=8.
Merge continues to use `ordering` and `max_lateness_ns`; event-time Merge requires
positive lateness. Live rejects lateness outside the signed timestamp range.
No symbol splitting, stablecoin peg assumptions or dynamic reference discovery
are performed. The plan is frozen before registration, using authoritative
persisted Source instrument metadata and Shared's deterministic path enumeration.
Re-converting already converted values is explicitly unsupported; use native or
merged native inputs.

## Transport, identity and readiness

The adapter owns one bounded subscription per declared dependency. Startup is
transactional across attachments. A failed final attachment releases all earlier
subscriptions and demand, producer authority and unregistered resources. Canceling
startup leaves cleanup owned by the service. Existing independent demand remains.

Each `ProcessingInput` preserves the immediate stream, Router delivery generation
and sequence, input position/role and a checked admission ordinal. Only the
Router subscription adapter can admit into a service registration. The lifecycle
handle exposes `admission_closed()` but no direct ingress capability. Custom
processors receive managed input through `ProcessorContext`; application producers
must publish through authoritative Source Router bindings. This avoids a second
sequence system. Rejected managed admission returns ownership and accounting is
recomputed from the complete delivery, including its custody evidence.

Polling rotates after each selected delivery, across associations and input ranks.
Other wakeups do not advance the cursor. This preserves each subscription's FIFO
while preventing a continuously ready input from displacing another ready input.
There are no per-event tasks or channels. Processor state remains exclusively
owned by one registration.

Every required upstream must be Running and synchronized before readiness is
committed. Custom readiness futures may additionally wait for controlled startup
processing. Depth needs an actual snapshot baseline. Mixed derived subscriptions
track native book baselines independently for each original depth source; trade
readiness cannot substitute for missing depth baselines. A late derived depth
subscriber needs a subsequent snapshot: the Router does not cache or invent one.
`RouterDelivery::Publication.event_valid` describes the local event/book validity;
`synchronized` describes overall subscriber readiness after that event. A trade
can be locally valid while one or more depth baselines are missing. Controlled
startup consumes valid snapshots and other valid events while overall readiness
is false; it commits Running only when every dependency is fully ready and no
queued invalid delivery remains. A complete batch that repairs a missing baseline
after an unusable delta can finish synchronized while `event_valid` remains false;
such a batch is rejected by the adapter. After
readiness, loss of overall synchronization fails closed.

Required-input failure, incompatible generation, invalid synchronization or
readiness timeout fences the derived generation. Registry watch notifications
provide lifecycle wakeups even without market events. There is no automatic
restart or partial/degraded-input operation.

## Typed derived publications

Source `Trades` and `Depth` publications retain their existing validation.
`RouterPublication::Derived` holds tagged `NativeMarketEvent::Trade` or complete
`Depth` operations. Source producers cannot submit derived publications.
The Router validates graph ancestry, immediate data binding, native instrument
identity, depth generations and per-source book authority.

`DerivedEvent::from_delivery` creates a record using opaque Router-issued
`EventEvidence`. The evidence binds the original admission, immediate upstream
admission and native record/control keys. Both public identities, ordinals and
input position must agree with it and the frozen dependency binding. A delayed
sequence may be older than the latest Router counter, but cannot be fabricated,
zero, future, or from a replaced binding/generation. Evidence retains one original
record plus one immediate identity, never a recursively growing lineage.
Instrument envelopes and depth acquisition/book/sequence metadata are validated
without weakening Source publication validation. Financial results remain the
responsibility of the trusted authorized processor; receipts are not a proof of
arithmetic or a subscriber-completion acknowledgement.

Production bindings accept only typed derived events (plus explicit completion).
Convert requires valuation metadata for timed data and permits unvalued untimed
snapshot barriers only. Legacy `attach_derived_producer` bindings accept canonical
publications explicitly labelled `CanonicalCompatibility` in their delivery
receipt. They cannot yield typed evidence or feed production derived processors.
Consumers of custom compatibility streams must inspect this mode.

`DerivedEvent.origin` preserves the original source runtime generation,
publication sequence and record ordinal. `upstream` preserves the immediate
upstream delivery. Each stage replaces only the immediate hop; metadata never
accumulates a lineage list. The native trade envelope, quantities and price, or
complete native depth operation, remain unchanged. Instrument metadata remains
resolvable through the original Source definition. `input_position` and
`admission_ordinal` describe this stage's observed admission sequence.

An optional immutable `EventValuation` distinguishes converted values from native
merged events. Untimed snapshots have `baseline_barrier = true` and no valuation;
they establish native book state and are explicitly outside the ordered data-event
subsequence. Untimed data deltas fail ordered processing/conversion. Snapshots
with a canonical exchange timestamp are ordered and valued as one atomic event.
Receive-time fallback is not promoted to an exchange-event-time guarantee.

## Merge ordering and progress

Arrival means successful managed ingress admission. Arrival Merge emits in that
order, traversing trade records and complete depth operations in publication order.
It never waits for an idle input after readiness. A recorded admission sequence
reproduces this order; independent live runs need not observe the same sequence.

Event-time Merge uses Shared `MergeKey` comparison:

1. Canonical exchange event timestamp in nanoseconds.
2. Declared immediate dependency position.
3. That publication's admission ordinal (Live's source-local task rank).
4. Record/event ordinal within the publication.

Trades are individual canonical events. A snapshot or delta, including all its
levels, is one indivisible event. Nested derived events retain native provenance
and use the current dependency's rank. Timestamp regression within an original
source is a terminal sequence conflict; native depth sequence/book checks also
apply independently of timestamps. Publication record order is preserved during
traversal; a later record revealing a key below the emitted frontier causes an
explicit terminal failure before that record is emitted. Production processors
publish one derived event per publication. Cross-origin events from an arrival Merge may
be reordered only within the configured lateness contract.

For each input, maximum observed timestamp `M` establishes `W = M - lateness`.
This is an explicit bounded-lateness input assumption: future data timestamps
must be at least `W`. Violations are checked against prior progress, including
within a multi-event publication. The boundary is exclusive for emission: a
candidate at `T` is released only when **every** required input has `W > T`.
This conservative rule also protects all equal-time tie keys. A temporary empty
queue establishes no progress. No wall clock or heartbeat advances event time.

An idle input holds the frontier only when work is pending. Empty healthy
processors have no computation deadline, including arrival Merge, identity
Convert and event-time Merge. A computation deadline is armed only while accepted
event-time work has no eligible head; draining or renewed eligibility clears it.
Retained count/byte limits or expiry of that monotonic deadline fail the generation. Deadlines are anchored to retained work, so newer
reference/output activity cannot indefinitely extend an older data event's wait.
Startup readiness has its own bounded deadline. Scheduling remains cooperative,
not a hard real-time execution guarantee.

Only a Source explicitly attached with `attach_replay_producer` may submit
`ReplayComplete`. Ordinary live producers reject it. Completion is FIFO Router
custody, prohibits subsequent source publications, and permits the completed
input's final deterministic ordering flush. Production derived processors
propagate completion after all required inputs explicitly finish and their data
is consumed. Disconnect, revocation, silence, failure and user stop never imply
completion. A normal live stop retains the unproven tail.

## Conversion alignment and units

References use Shared's positive **last trade** basis. Shared's planner, graph,
`ReferenceState`, `evaluate_conversion_path`, `convert_trade_values` and
`convert_depth_level` supply path choice, failure categories and arithmetic.
Direct, inverse, identity and multi-hop paths use Decimal256(76,28), exact rational
intermediates and Shared half-even rounding. Canonical Decimal inputs enter this
representation through an exact mantissa/scale ratio; no float or narrowing is
introduced. Historical execution code is unchanged. Additive Shared `find_paths_limited` and
`plan_conversion_limited` APIs bound search work and stored paths in the same
existing traversal; valid plans keep exactly the previous preference order.
Planning runs on the existing blocking executor before acquiring subscriptions.

With references, data and reference events traverse the same ordered frontier.
`ReferenceState` is updated only when a reference is selected in global key order.
Thus its latest observation remains sufficient for every subsequently evaluated
data event: a later reference cannot overwrite history still needed by earlier
pending data. Every selected latest reference retains its original input batch
charge until replaced or terminal cleanup. Reference-only events produce no data
output. Equal timestamp eligibility uses Shared's strict `reference_key < data_key`;
data ranks precede reference ranks in this API, so equal-time references are
ineligible for that data event. Future observations are never substituted.

Shared checks every edge's event-time age against the data event, identity and
positive price. Deterministic Shared path order supplies fallback. A proven
unavailable conversion fails with the Shared missing/stale/future/identity/
arithmetic category; an absent future frontier fails at the bounded waiting
limit. A withheld future reference with no prior eligible observation is reported
as missing, rather than used. Event-time reference age is separate from feed
activity and readiness.

A valuation stores the exact evaluation, selected edge keys/prices/directions,
reference runtime generations, currency path and target values. Only prices and
existing canonical quote quantities receive target-currency values. Native base
quantities, contract counts, source metadata and level keys remain unchanged.
The complete depth operation uses one immutable evaluation; partial arithmetic
failure publishes no partial update. A deletion is still keyed by its native
side/price and book generation even if its target-currency price differs from an
earlier insertion. This is event valuation, not a continuously repriced book.

## Ownership, resources and failure handling

`runtime.processing.derived` holds runtime defaults: `max_inputs = 128`,
`state_events = 4096`, `state_bytes = 16777216`, `stall_timeout_ms = 5000`,
`retry_delay_ms = 1`, and `planning_steps = 100000` per conversion source. Fan-in, source ancestry, state records, state accounted bytes,
input/output mailboxes and aggregate buffering are bounded. A retained whole
publication is charged in full while partly traversed, including consumed prefixes.
The input charge includes fixed cursor/reference metadata, cached head/map nodes
and identity/evidence storage (a conservative per-publication metadata charge);
reference observations do not create an uncharged payload-history queue.
These limits concern accounted retained data, not arbitrary processor allocations
or total RSS. General validation rejects zero limits and excessive fan-in/deadlines.

Tracked input `WorkBatch` reservations remain alive in processor-owned pending
state and latest-reference holds. They become terminally retained batches in
`ProcessingReport.processor_retained`, with the next record ordinal and a
reference-hold marker. Original partially consumed publications must not be
blindly replayed. Output admitted locally remains charged independently until
Router acceptance or terminal retention. Reports retain their association slots
until taken; payload charges survive taking a report and release on disposal/drop.

A Router `Busy` result has accepted no custody. The service keeps the same pending
output at the FIFO head and retries after a bounded monotonic timer wakeup.
There is no busy spin or financial recomputation. Capacity, invalid publication,
revocation, shutdown and exhausted identity remain terminal, with unpublished
output retained in the report. Subscriber completion and persistence are separate
from Router custody.

The admission ordinal lock precedes pool state, generation fence, mailbox state
and aggregate budget. Output admission and retirement share one generation fence;
guarded mailbox operations never reacquire it. Router custody holds the output
fence, then Router state and publication authorities. Under Router state,
publication validates frozen dependency binding/runtime epochs and Source book
epochs, then holds each distinct existing authority guard through custody insertion.
A required upstream revocation therefore serializes with downstream admission
even before the service observes registry changes. Dispatch and extraction recheck
the same epochs. Ancestors are deduplicated; publication uses try-locks and Busy
retains custody locally. Registry revocation releases
authority before processing fencing. Processor callbacks/destructors run outside
pool/registry/Router locks. Production pending-state synchronization never acquires
pool state or the registry. No synchronous guard crosses an await.

Existing association status exposes per-input rank, role, generation, readiness,
last delivery activity, active ordering, watermarks and lag, retained buffering,
last reference timestamps/ages, worker thread and terminal reasons. The processing
end-to-end regression includes terminal status in unexpected-delivery diagnostics.

## Local selection and throughput measurement

The opt-in Linux unit measurement `selector_cpu_benchmark` prepares charged input
batches before timing selection and disposal. It uses thread CPU time, 4,096
selections per case, 1/4/16 inputs and backlogs of 32/512/2,048 publications.
In the unoptimized test profile, the original scan reconstructed candidate events
(including cloned native records and identity Strings) on every selection; draining
N heads performed N(N+1)/2 candidate reconstructions. At backlog 2,048 it cost
382–403 microseconds of CPU per event; arrival selection cost 0.49–0.63 microseconds.

The focused replacement borrows timestamp keys at admission, caches one head per
retained publication in a binary heap, and keeps original charged WorkBatches in
an admission-ordinal map. Head removal/replacement is logarithmic, frontier checks
remain linear in input count, and no candidate payload is allocated during head
selection. It cost 1.35–1.68 microseconds per event at backlog 2,048 in the same
profile. Arrival selection now costs 0.73–0.79 microseconds, reflecting map-node
removal overhead. This is a selector measurement, not production capacity or a
claim about total RSS. The generic scheduler and CPU pool are unchanged.

The separate opt-in `real_router_merge_throughput` integration measurement runs
actual Source Router → Merge CPU → Router → subscriber pipelines with finite
completion, 2/4/16 inputs and 32/512/2,048 total events. It measures full pipeline
wall throughput; output and terminal accounting assertions remain enabled. Both
measurements use local synthetic data and intentionally have no timing assertions.

On this local debug run, the full pipeline measured 4,102–4,682 arrival events/s
and 3,987–4,567 event-time events/s for 2,048 events (2/4/16 inputs). At 512
events the ranges were 4,013–4,659 and 3,797–4,732 respectively; at 32 events,
startup/scheduling amortization dominated (2,579–4,446 and 2,525–3,836 events/s).
These single-run results are diagnostic, not optimized-build service guarantees.
