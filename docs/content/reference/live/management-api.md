# Live management API

MarketForge's management service controls the existing Live application. The HTTP API and standalone TUI are adapters to that service. Market data continues over the existing Unix streaming interface; management SSE is an administrative observation feed.

## Modules and ownership

- `live::management::protocol`: portable requests, definitions, plans, operations, views and errors. Existing Source/Derived validation was moved here and reexported at the original Live paths.
- `live::application::LiveApplication`: existing Source, Derived, Router, recovery and metrics ownership and reconciliation.
- `live::application::ManagedLiveApplication`: owns that application, `ManagementService` and the HTTP listener. `start`, `handle`, `management`, `management_address` and retryable asynchronous `shutdown` support embedding.
- `live::management::service`: transport-independent `ManagementService`, cloneable `ManagementHandle`, trusted `Principal`, credential loading and definition storage. HTTP never obtains direct processor/registry/database handles.
- `live::management::http`: Axum authentication, bounded connections/requests, JSON errors and SSE.
- `live::management::client::MarketForgeManagementClient`: standalone HTTP client, independent of Live and PostgreSQL.
- `live::management::tui`: UI state/rendering and an asynchronous HTTP backend. The canonical executable is `marketforge-live-tui`. The documented `marketforge-tui` name remains an alias.

Features: `management-api` enables Live plus Axum; `management-client` enables the portable HTTP client; `management-tui` adds existing Ratatui/Crossterm dependencies. Neither client feature requires Live, SQLx or PostgreSQL. Existing `stream-client`, `live`, and `historical` remain separate.

## Desired state, graph and persistence

Enabled is permission to run. OnDemand/AlwaysOn is durable policy. Demand is owned ephemeral intent. Runtime generation/readiness and data integrity are separate observations. Merely reading a view or opening SSE does not acquire market demand. Releasing a hold can leave a stream running because another owner, AlwaysOn, or a dependent still needs it; release returns that explanation.

Definitions use the existing validated Source/Merge/Convert requests. Source identity changes are unsupported: create a new definition. Derived edits are explicit generation transitions. Graph updates install a validated immutable snapshot at the Router/application boundary, using the same runtime registry and dependency resolver. New admissions see one graph revision; old execution retains its original bindings. Unrelated additions do not recreate the Router or processing pool. Retired stream identities retain bounded generation tombstones, preventing an old completion from reviving a reused ID.

Preview does not persist or mutate execution. Plans contain graph/config/pool revisions, affected definitions, transition/continuity impact, persistence behavior, expiry and warnings. Disruptive plans also capture demand revision. Apply validates again and reserves affected admission before asynchronous persistence. Dynamic consumer changes invalidate unsafe previews; graph revision alone is insufficient. Definitions still referenced by other definitions cannot be deleted. Default unsafe cascades are rejected. Explicit runtime cascade scope retires dependents first; persistent dependent-policy changes require their own reviewed plans. Manual/recording owners must explicitly release their holds rather than having those holds silently removed.

`PostgresDefinitionStore` reuses existing tables, Source registration and Derived edges. Updates/deletes compare the locked previous revision/definition and commit definition plus dependency changes transactionally. Database work never runs while holding graph/Router/scheduler locks. Deleting a definition does not delete recorded market data. This is a single authoritative daemon contract, not a distributed multi-writer catalogue.

Durable commit and runtime activation are distinct. Operations expose `saved`, `activation_requested`, optional `runtime_ready`, `state`, `detail`, and `cleanup_complete`. A saved definition can remain visible with activation failure. OnDemand creation without demand does not promise a running stream. An accepted operation remains owned after its caller disconnects or times out. Incomplete cleanup records are retained until cleanup actually completes. Shutdown stops new mutation/lifecycle admissions and retires services in dependency order.

Administrative leases have an identity, explicit owner and monotonic expiry. Renewal/release require that owner; administrator forced release is explicit and audited. Acquisitions and renewals have idempotency keys. Duplicate renewal keys do not extend expiry again. The last 32 renewal keys per active lease are retained; acquisition/release outcomes are retained within the bounded lease-key history. Temporary holds, operations and idempotency records are process-local, not durable across restart. Daemon instance IDs identify the boundary. Clients must retain a mutation key when resolving unknown outcomes.

## CPU pool

`ProcessingHandle::set_workers(target, expected_revision)` changes the dedicated CPU pool only. Management exposes the same absolute target through a reviewed `resize_workers` action. Default upper bound is 64; configurable upper bound cannot exceed 1024; minimum is one. Existing processor registrations, state, execution leases, stream generations and buffering budgets remain in place. Tokio threads are not resized.

Growth stages workers before activating them under the shutdown boundary. Partial thread creation failure retires/joins additions and preserves the original pool. Shrink marks workers for retirement under the scheduling lock, wakes idle workers, and lets each current turn return state and its exclusive execution lease before exit. The supervisor joins only finished threads and remains responsive to shutdown. It does not kill a computation. A non-returning turn leaves resizing/cleanup incomplete and owned. Conflicting resizes are serialized/rejected, and unexpected worker failures retain the existing fail-closed policy.

Views distinguish requested target, actual live workers, starting, retiring, executing turns, ready processors, retained payload budget, resize revision/error and saved startup default. Apply-now changes no file; apply-and-save atomically saves the startup default before live application. Saving and applying can have different outcomes. Neither operation resizes queues or restarts streams.

## API and security

All `/api/v1` requests require `Authorization: Bearer TOKEN`. Viewer reads observations; operator applies non-destructive administrative actions and owns temporary leases; administrator additionally deletes definitions, forces lease release and shuts down. Authorization is enforced inside management too. Trusted Rust embedders explicitly construct `Principal::trusted`; this is not a network authentication shortcut. HTTP is loopback-only with no permissive CORS. Credentials are startup settings; no OAuth, remote deployment or live token rotation is implemented.

| Method | Path | Contract |
|---|---|---|
| GET | `/api/v1/overview`, `/capabilities` | Bounded observations and implemented capabilities |
| GET | `/api/v1/streams`, `/streams/{id}`, `/streams/{id}/integrity` | Definitions/runtime/dependencies and attributed bounded integrity |
| GET | `/api/v1/runtime`, `/clients`, `/activity`, `/config` | Actual pool, consumers, operations and setting mutability |
| GET | `/api/v1/instruments` | Bounded authoritative Source templates; unavailable metadata is explicit |
| POST | `/api/v1/change-plans` | Typed action preview |
| GET | `/api/v1/change-plans/{id}` | Retained preview |
| POST | `/api/v1/change-plans/{id}/apply` | `{ "idempotency_key": "owner-scoped-key" }`; returns operation |
| POST | `/api/v1/demand-leases` | Stream, seconds, idempotency key |
| POST | `/api/v1/demand-leases/{id}/renew` | Seconds and idempotency key |
| DELETE | `/api/v1/demand-leases/{id}` | Own hold; `?force=true` requires administrator |
| GET | `/api/v1/operations`, `/operations/{id}` | Owned asynchronous outcomes |
| GET | `/api/v1/events` | Bounded SSE with daemon/cursor resynchronization |

Collection pagination is `after`, `limit` (1–128), and bounded `filter`. Errors have stable `code`, safe `detail`, and `correlation_id`. Extractor/route errors use the same envelope. Actions are serde-tagged by `action`, including `create_source`, `create_derived`, `edit_derived`, `set_desired`, `restart`, `delete`, `resize_workers`, `set_observation_interval`, `shutdown`; they are not scripts. Expected definition revisions are strings. Rust clients reuse these exact DTOs.

Generation/revision IDs, observation timestamps and integrity integer positions are decimal strings in views; unavailable values remain null/absent. Native request contracts preserve existing exact JSON integers and decimal strings (`serde_json` arbitrary precision); cross-language consumers must parse contract integer fields with an exact integer representation. Financial values are not converted through floating point. Management does not perform currency conversion.

See the [Live configuration reference](configuration.md) for management defaults. A reserved shutdown admission slot prevents ordinary work from exhausting administrator shutdown capacity. SSE output has a bounded queue with reserved resync signaling. Slow network readers hit socket progress deadlines; independent clients continue. Oversized views fail explicitly rather than growing buffers indefinitely.

The initial overview captures its feed cursor before sampling. SSE begins with a snapshot and subsequent cursor-tagged events. Expired cursors, daemon replacement or slow observers explicitly require resynchronization. Lifecycle/integrity and operation events remain inspectable within bounded history; observations can be coalesced. This is not a durable incident log or market replay. Metrics use the collector's latest immutable observation: API readers do not consume/reset windows. Independently sampled services are not a globally atomic snapshot. Source input and Derived output throughput are separately labeled.

## Limits and unsupported behavior

No bootstrap, historical replay, new market transport, automatic scaling, affinity or Tokio resizing is implemented. Recording has no faithful Router recording adapter in the existing application: enabling it remains an explicit startup error, and management reports it unsupported rather than presenting a fake recorder. Last-data age is unavailable where no authoritative measurement exists. Detailed metrics/configuration views use bounded structured text rather than fabricated measurements. Very large catalogues may exceed the configured full-overview response budget; paginated stream queries remain available, while this TUI requires a fitting overview.

Configuration saves use the existing general file, preserve unrelated TOML/comments, compare expected content, and replace atomically with sync. External conflicts reject saving; external edits are not automatically hot-reloaded. Only processing workers and management observation interval are live-editable. Other general settings require engine restart or are read-only/unsupported. Operation/idempotency history and leases are not persisted. See the [historical verification report](https://github.com/flowdrivenml/marketforge/blob/main/.planning/engine/live/evidence/management-v1-verification.md) for executed coverage and persistence-environment limitations.

See the [administration workflow](../../workflows/live/management.md).
