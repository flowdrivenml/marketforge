# WP-07 — Service, management and observation contract implementation

Date: 2026-10-21. Disposition: **contract implementation review pending; C12 production adapter decision OPEN.
Not Accepted and not production integration certification**.
The cross-package C06 database gates remain blocked as documented separately.

WP-07 began after C06 mapping and independent recording contracts passed their
own relevant checks. Sources: Document 17 WP-07 and C07–C12, Documents 01–03,
05/05.1, 12–14, existing WP-05/WP-06 models and accepted decisions. No production
server, configuration persistence engine, exchange processor or event router was
implemented under this contract package.

## Requirement and consumer ledger

| Contract/acceptance requirement | Concrete implementation and witness |
|---|---|
| Narrow service boundaries | Public `ports/` transport, protocol, registry, processing, recovery, configuration, publication, retention, recording, observability, context and lifecycle modules; existing routing contracts reused |
| One versioned control vocabulary | `model/protocol.rs`: queries, configuration validate/apply/history/diff/restore, transient demand, recording flush, retention protection/progress/renew/release, recovery/restart/resync, operation cancel and shutdown |
| Request/operation identity and completion | Runtime-scoped operation/session/connection IDs; RequestId; explicit desired-persisted/runtime-applied/acquisition-ready/removed/committed/protection/recovery/stopped objectives; checked operation transitions and evidence |
| Validation, auth, errors and version behavior | Stable typed ErrorCode/ControlError, trusted server-side AuthenticatedPeer, resource-category permission checks, invalid semantic objective rejection, bounded codec, version-first rejection, typed response correlation |
| Idempotency | Bounded entry/serialized-byte ledger reserves before effects; identical pending requests stay Pending; completed results replay; changed command conflicts; no silent eviction; transient entries session scoped and removed only after fencing |
| Effective configuration | Separate desired/resolved models, explicit Inherit/Set, global budgets retained once, optimistic revision inputs, pure compiler/plan/impact ports, store CAS/history/revision ports, persistence-before-apply contract; failure-injected store witness |
| Updates/shutdown | Owner settings with explicit adoption progress/revision/boundary, per-call deadline/cancellation/generation checks, task spawn/join ports, queue count/byte/in-flight/deadline limits and explicit unfinished work |
| Five TUI views | Owned Dashboard/Streams/Derived/Recording/Settings DTOs; presentation consumer round trips all five; populated source, derived, recording and retention fixtures |
| State distinctions | Lifecycle, each demand, persistent intent, effective acquisition need, connection/subscription generation, processor/book readiness, integrity/recovery/coverage, desired/applied/adopted revisions remain separate |
| Availability/health | Available zero, missing, disabled and stale observations are distinct; typed capability health/reasons; health is observational and never substitutes for canonical integrity |
| Native metrics | Closed metric vocabulary, typed units/value kinds, sums/latest/bounded distributions, reset/generation identity, cardinality-bounded prebound scope, checked quantiles/count/sum/extrema, no arbitrary runtime labels |
| Exchange strategy consumer needs | Raw frame origin/generation/receipt sequence, protocol classification, typed heartbeat and depth synchronization/sequence/checksum capability descriptors; three contrasting synthetic strategy fixtures; unverified/stale evidence blocks activation |
| Transport pooling | Exact endpoint/protocol/authentication/handshake/heartbeat/class compatibility key; immutable descriptor and separate connection writer/control ports; no secret material in configuration or pool keys |
| Real/fake interface compatibility | Shared std Future/object-safe Send+Sync ports; fake management server/client and failure-injected config store implement those public traits; real SystemMonotonicClock and test clock share the existing clock trait |
| Feature/storage compatibility | No manifest/lockfile/dependency changes; v1 JSON control framing and EventRecordV1 remain explicit; required feature builds and historical regressions run |

## Wire and execution behavior

The control protocol is v1 JSON with a four-byte big-endian payload length. Codec
limits cover frame bytes, collection items, string bytes and nesting. Partial
frames return no request; oversized lengths are rejected before payload decode.
The caller retains trailing frames. Owners must bound their input buffers before
reading. Serialization itself is byte bounded. The original bytes are typed-
decoded after inspection, preserving serde duplicate-field detection.

Unknown protocol versions are rejected before interpreting their command payload.
Responses must correlate to the expected RequestId. Error messages generated by
these helpers use static public descriptions, not raw payloads or credentials.
Peer UID/grants are supplied by a trusted session owner and are not request data.
Version 1 is a proposed concrete freeze for user review, not a claim of an already
released backwards-compatible wire protocol.

Idempotency is process scoped. Runtime identity fences obsolete sessions; a
transient demand/protection result is scoped to its authenticated session.
Persistent desired state is protected separately by the durable revision/CAS
contract. A full ledger rejects admission. A pending entry is not evidence that
it is safe to repeat effects. Capacity failure while caching a result leaves it
pending rather than reporting guessed success. The session/server owner must
serialize admission/reservation and maintain bounded in-flight request IDs.
A process-local ledger is not durable cross-restart command history.

Recording enable/default policy changes use a stream transition. Only the
separate batch-size/flush-timing command has a next-batch boundary. Logging is
immediate; metrics use the next measurement; subscriber limits affect future
admission; derived semantic changes require a new generation; profile/global
resource changes require service restart. History restoration compiles historical
contents but persists a NEW revision, never rewinds IDs or revives native handles.
Semantic/capability/DAG compilation and atomic persistent implementation remain
WP-09; these are required ports and witnesses, not fake-success implementations.

## Constructor contracts

`runtime/dependencies.rs` contains typed bundles. Every instrumented owner gets a
monotonic clock, cancellation, explicit limits, task-spawn handle, prebound metrics
whose owner generation must match, and diagnostic handle. Metrics bootstrap has
its own context/diagnostic inputs and finite series limit, without a circular
requirement for an already-created metrics registry.

| Planned owner | Typed dependency bundle and substantive inputs |
|---|---|
| Connection Manager | ConnectionManagerDependencies: immutable descriptor, raw connector, frame router, compiled protocol, recovery policy |
| REST client/rate scheduler | RestSchedulerDependencies: REST transport, explicit weight/window/concurrency rate policy, recovery policy, owner resource limits |
| Subscription Manager | SubscriptionManagerDependencies: generation-fenced connection writer, compiled protocol, recovery policy |
| Source Registry | SourceRegistryDependencies: processor factory, subscription owner, profile compiler, idle grace |
| Source Processor / BookStore | SourceProcessorDependencies: acquisition plan, exclusive EventAcceptor, normalizer, existing event output, recovery |
| Recovery Coordinator | RecoveryCoordinatorDependencies: registry, REST, processor, connection control, recovery policy |
| Derived Registry | DerivedRegistryDependencies: source registry, derived-owner factory |
| Individual Merger | MergerDependencies: definition, owned accepted-event inputs, own cursor acceptor, canonical output |
| Reference Registry | ReferenceRegistryDependencies: source demands and accepted-event inputs |
| Converter | ConverterDependencies: definition, input, typed exact reference reader, own cursor acceptor, output |
| Unix Publisher | PublisherDependencies: listener, registry, protected reader/protection control, atomic book view, input |
| PostgreSQL Recorder | RecorderDependencies: registry, recording backend, input, recovery, recording settings |
| Retention Manager | RetentionDependencies: exact segment storage, accepted-event input |
| Metrics Registry | MetricsDependencies: context, diagnostics, measurement settings, series budget |
| Configuration Manager | ConfigurationManagerDependencies: pure compiler, CAS/history store, reconciler |
| Runtime Reconciler | RuntimeReconcilerDependencies: source/derived query and configuration execution, publication/recovery ports and explicit recorder/retention/metrics update handles |
| Runtime Supervisor | SupervisorDependencies: finite registered child lifecycle handles |
| Management owner | ManagementDependencies: management service, frame/session/operation bounds |

The bundles contain typed owner-specific capabilities, not a global lookup API.
Ports do not import a database client, TUI, concrete executor or runtime container
into pure normalization. Actual owner construction/startup implementations remain
in their designated production packages. The constructor witness tests scope and
budget rejection and an explicit task-admission failure.

## Tests and verification

`tests/live/wp07_contracts.rs` adds 28 deterministic tests, including a fake
management server/client wire round trip and presentation fixtures. No arbitrary
sleeps or new dependencies were introduced. Independent fixture expectations
include exact known revision/generation/cursor values and deliberately invalid
states. Synthetic depth strategy evidence is explicitly a contract fixture; it
is not real exchange capability certification.

Final command logs and exact pass/fail counts:
[results-final.json](wp06-c06-wp07-verification/results-final.json).
Historical baseline failure:
[08.log](wp06-c06-wp07-verification/08.log).
Exact source/evidence manifest and hashes:
[files.json](wp06-c06-wp07-verification/files.json).

## Limits and review disposition

- C06 real transaction/commit/retry/protected-read/cleanup database witnesses are
  not run under the current read-only restriction. Negative SQL header timestamp
  representability needs an explicit decision. See [C06 evidence](wp06-c06-connected.md).
- C10 fixtures establish field/strategy sufficiency, not real profile validation.
  Real profile compilation/verification belongs to WP-12/WP-28.
- C12 production transport/TLS/SQL adapter selection and real adapter execution
  are not certified by these contracts. The ports expose no dependency-specific
  types and add no driver stack. Binding an adapter later must preserve the
  contracts and the accepted Cargo isolation; this is not authorization to adopt
  both SQLx and tokio-postgres or replace custom infrastructure.
- Metrics collection/histograms/health evaluation, persistent configuration/DAG
  processing, task runtime, actual Unix sessions, actual network transport,
  publisher, recorder and retention services remain their planned owners. The
  public contracts are executable; the private runtime scaffolds are not claimed
  implemented. No Prometheus/Grafana is added (DEC-032).
- No accepted decision, historical behavior, PostgreSQL schema, existing DB
  record, WP-10 router, credentials, Cargo dependency, commit or push changed.

This is submitted for independent contract review. If the reviewer requires the
C12 production adapter decision as a prerequisite for WP-07 acceptance rather
than before its first real adapter consumer, that gate remains open; this report
does not infer an approved driver/runtime choice from illustrative library lists.
The user retains final acceptance.
