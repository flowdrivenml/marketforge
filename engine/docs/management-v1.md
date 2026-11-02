# Live management v1

MarketForge's management service controls the existing Live application. The HTTP API and standalone TUI are adapters to that service. Market data continues over the existing Unix streaming interface; management SSE is an administrative observation feed.

## Modules and ownership

- `management_protocol`: portable requests, definitions, plans, operations, views and errors. Existing Source/Derived validation was moved here and reexported at the original Live paths.
- `live::application::LiveApplication`: existing Source, Derived, Router, recovery and metrics ownership and reconciliation.
- `live::application::ManagedLiveApplication`: owns that application, `ManagementService` and the HTTP listener. `start`, `handle`, `management`, `management_address` and retryable asynchronous `shutdown` support embedding.
- `live::management`: transport-independent `ManagementService`, cloneable `ManagementHandle`, trusted `Principal`, credential loading and definition storage. HTTP never obtains direct processor/registry/database handles.
- `live::control::http`: Axum authentication, bounded connections/requests, JSON errors and SSE.
- `client::management::MarketForgeManagementClient`: standalone HTTP client, independent of Live and PostgreSQL.
- `tui::management`: UI state/rendering and an asynchronous HTTP backend. Both `marketforge-tui` and the compatibility name `marketforge-live-tui` run this standalone client.

Features: `management-api` enables Live plus Axum; `management-client` enables the portable HTTP client; `management-tui` adds existing Ratatui/Crossterm dependencies. Neither client feature requires Live, SQLx or PostgreSQL. Existing `stream-client`, `live`, and `historical` remain separate.

## Running

Configure the existing general file and exchange/format directory. Normal daemon startup requires `MARKETFORGE_DATABASE_URL` for durable definitions even when recording is disabled. It restores definitions and validates the whole graph before accepting management work. Missing persistence fails startup; it never substitutes an empty in-memory catalogue. `--empty-definitions` is an explicit service-diagnostics mode: definition writes are unavailable.

Create a private credential file (mode 0600), with a JSON array shaped as follows. Replace the placeholder with a randomly generated secret of 24–256 bytes; do not use the displayed placeholder as a credential:

```json
[{"identity":"workstation-admin","role":"administrator","token":"REPLACE_WITH_A_RANDOM_PRIVATE_SECRET"}]
```

Set `control.management.credentials_file` in the existing general file, or supply the entire JSON array through `MARKETFORGE_MANAGEMENT_CREDENTIALS`. The TUI token file contains just that identity's token, not the JSON array. Do not pass tokens on the command line. Credentials are never returned by configuration queries. Files must be private regular files, not symlinks.

```sh
cargo run --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features management-api --bin marketforge-live -- \
  --config engine/examples/live/general.toml --config-dir data/.config/live \
  --profiles binance_spot --rest-weight binance_spot=6000/60

cargo run --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features management-tui --bin marketforge-tui -- \
  --endpoint http://127.0.0.1:8080 --token-file /private/path/operator-token
```

The quota is an example: provide the verified quota for the actual profile/egress contract. Select profiles that exist in the configuration directory. The socket's parent directory must satisfy the existing Unix server's private-directory checks. `--validate-only` validates static configuration without listeners, database access or exchange connections. `--general` and `--config` are aliases. Control port zero selects an ephemeral owned listener; non-loopback addresses remain rejected.

The application runs without the TUI. `q` exits only the TUI and best-effort releases that TUI's temporary holds; crashes are handled by lease expiry. Use AlwaysOn for durable independent running. Shutdown is a separate administrator-only reviewed plan. Signals also initiate owned shutdown. If a CPU turn does not return, the daemon retains its owners, reports incomplete cleanup and continues waiting; it does not claim success or discard the thread handle.

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

Default bounds in `[control.management]`: command queue 32, active operations 16, completed history 256, plans 128, leases 128, event history 256, HTTP connections 128, concurrent requests 64, SSE clients 16, request body 64 KiB, response 4 MiB, request/socket-progress deadline 10 s, plan TTL 60 s, maximum lease 300 s, observation interval 1 s. A reserved shutdown admission slot prevents ordinary work from exhausting administrator shutdown capacity. SSE output has a bounded queue with reserved resync signaling. Slow network readers hit socket progress deadlines; independent clients continue. Oversized views fail explicitly rather than growing buffers indefinitely.

The initial overview captures its feed cursor before sampling. SSE begins with a snapshot and subsequent cursor-tagged events. Expired cursors, daemon replacement or slow observers explicitly require resynchronization. Lifecycle/integrity and operation events remain inspectable within bounded history; observations can be coalesced. This is not a durable incident log or market replay. Metrics use the collector's latest immutable observation: API readers do not consume/reset windows. Independently sampled services are not a globally atomic snapshot. Source input and Derived output throughput are separately labeled.

## Client and TUI

Construct `MarketForgeManagementClient` from a token file/environment (or an explicit secret for trusted embedding), then use `overview`, `streams`, `stream`, `integrity`, `config`, `preview`, `apply`, `operation`, `wait_operation`, lease methods and `notifications`. Client concurrency, responses and deadlines are bounded. Observation reconnects resynchronize; mutations are never silently retried. Lost mutation responses return `UnknownMutation` with a key. `wait_operation` waits for owned cleanup completion, not just command acceptance. Notification framing belongs to the background reader, so cancelled `recv` does not lose a partially decoded SSE event. Close the notification handle and client explicitly for deterministic task cleanup.

Streams is the default TUI screen. Arrows/j/k select, Enter opens details, Tab changes screens/detail tabs, Esc backs out, `/` filters, `:` opens actions, `?` shows contextual help and PageUp/Down scroll. Stable stream IDs preserve selection on refresh. Source creation selects real metadata via F2 lookup; Merge creation preserves input order and supports existing ordering; Convert uses declared data/references/target. Management remains the validation authority. Derived editing uses the existing request JSON form; it is not an arbitrary script. Missing metadata is a visible limitation. Disruptive actions require plan review and explicit confirmation. Runtime controls show actual resize progress; settings expose live worker count/observation interval separately from restart-only/unsupported settings.

UI requests run outside rendering. Narrow terminals get a compact fallback. Names/diagnostics are sanitized for terminal controls. Disconnection labels cached observations stale, disables unsafe mutations, and reconnects with bounded backoff. Normal exit/error/panic paths restore terminal state.

## Limits and unsupported behavior

No bootstrap, historical replay, new market transport, automatic scaling, affinity or Tokio resizing is implemented. Recording has no faithful Router recording adapter in the existing application: enabling it remains an explicit startup error, and management reports it unsupported rather than presenting a fake recorder. Last-data age is unavailable where no authoritative measurement exists. Detailed metrics/configuration views use bounded structured text rather than fabricated measurements. Very large catalogues may exceed the configured full-overview response budget; paginated stream queries remain available, while this TUI requires a fitting overview.

Configuration saves use the existing general file, preserve unrelated TOML/comments, compare expected content, and replace atomically with sync. External conflicts reject saving; external edits are not automatically hot-reloaded. Only processing workers and management observation interval are live-editable. Other general settings require engine restart or are read-only/unsupported. Operation/idempotency history and leases are not persisted. See `management-v1-verification.md` for executed coverage and persistence-environment limitations.

## Operator walkthrough

1. Prepare the existing exchange/format configuration and instrument catalogue, a private Unix socket directory, the general file, database environment and private management credentials. Run static validation first, then start the headless daemon using the commands above.
2. Connect the TUI with an operator token. Viewing Streams creates no demand. Use `c`, select Source, and F2 to look up an authoritative instrument template. Review each create plan. If metadata is unavailable, correct the catalogue rather than inventing quantity specifications.
3. Create a Merge with ordered existing Source IDs, then a Convert with its data input, target and declared references. Review plans and inspect Definitions/Dependencies. Choose Run temporarily for an expiring UI-owned hold, or AlwaysOn for durable running intent. Inspect readiness reasons separately from coverage/integrity.
4. In Runtime press `r`, enter the absolute worker target, and choose apply-now or save-and-apply. Review the plan. Watch requested/live/starting/retiring/executing fields and the operation result; acceptance alone is not completion. The integrated test exercises 4 → 8 → 4 while actual financial publications continue under the same stream generations.
5. Preview a restart. If dependents are affected, use the explicit cascade review or reject the plan. Existing Unix consumers receive their continuity boundary and must subscribe anew; a restart is not transparent continuity.
6. Press `q` and reconnect later. The daemon remains owned independently. UI-only holds are best-effort released/expire; AlwaysOn and other consumers remain. Application shutdown is a separate reviewed administrator action.

A Rust operator can use the same client contracts:

```rust,no_run
use marketforge_engine::client::management::{ClientOptions, MarketForgeManagementClient};
use marketforge_engine::management_protocol::Action;
use std::time::Duration;

async fn operate() -> Result<(), Box<dyn std::error::Error>> {
let client = MarketForgeManagementClient::from_token_file(
    "http://127.0.0.1:8080", "/private/path/operator-token", ClientOptions::default(),
)?;
let _view = client.overview().await?; // read-only: no market demand
let plan = client.preview(Action::ResizeWorkers { workers: 8, save: false }).await?;
println!("Review impact: {}", plan.continuity_impact);
// Apply only after the operator/application has reviewed this concrete plan.
let operation = client.apply(&plan.id, "workstation-resize-request-001").await?;
let completed = client.wait_operation(&operation.id, Duration::from_secs(30)).await?;
println!("{}; cleanup complete={}", completed.state, completed.cleanup_complete);
client.close().await;
Ok(())
}
```
