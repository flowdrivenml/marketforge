# Administer MarketForge Live

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
  --config examples/engine/live/general.toml --config-dir data/.config/live \
  --profiles binance_spot --rest-weight binance_spot=6000/60

cargo run --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features management-tui --bin marketforge-live-tui -- \
  --endpoint http://127.0.0.1:8080 --token-file /private/path/operator-token
```

The quota is an example: provide the verified quota for the actual profile/egress contract. Select profiles that exist in the configuration directory. The socket's parent directory must satisfy the existing Unix server's private-directory checks. `--validate-only` validates static configuration without listeners, database access or exchange connections. `--general` and `--config` are aliases. Control port zero selects an ephemeral owned listener; non-loopback addresses remain rejected.

The application runs without the TUI. `q` exits only the TUI and best-effort releases that TUI's temporary holds; crashes are handled by lease expiry. Use AlwaysOn for durable independent running. Shutdown is a separate administrator-only reviewed plan. Signals also initiate owned shutdown. If a CPU turn does not return, the daemon retains its owners, reports incomplete cleanup and continues waiting; it does not claim success or discard the thread handle.

## Client and TUI

Construct `MarketForgeManagementClient` from a token file/environment (or an explicit secret for trusted embedding), then use `overview`, `streams`, `stream`, `integrity`, `config`, `preview`, `apply`, `operation`, `wait_operation`, lease methods and `notifications`. Client concurrency, responses and deadlines are bounded. Observation reconnects resynchronize; mutations are never silently retried. Lost mutation responses return `UnknownMutation` with a key. `wait_operation` waits for owned cleanup completion, not just command acceptance. Notification framing belongs to the background reader, so cancelled `recv` does not lose a partially decoded SSE event. Close the notification handle and client explicitly for deterministic task cleanup.

Streams is the default TUI screen. Arrows/j/k select, Enter opens details, Tab changes screens/detail tabs, Esc backs out, `/` filters, `:` opens actions, `?` shows contextual help and PageUp/Down scroll. Stable stream IDs preserve selection on refresh. Source creation selects real metadata via F2 lookup; Merge creation preserves input order and supports existing ordering; Convert uses declared data/references/target. Management remains the validation authority. Derived editing uses the existing request JSON form; it is not an arbitrary script. Missing metadata is a visible limitation. Disruptive actions require plan review and explicit confirmation. Runtime controls show actual resize progress; settings expose live worker count/observation interval separately from restart-only/unsupported settings.

UI requests run outside rendering. Narrow terminals get a compact fallback. Names/diagnostics are sanitized for terminal controls. Disconnection labels cached observations stale, disables unsafe mutations, and reconnects with bounded backoff. Normal exit/error/panic paths restore terminal state.

## Operator walkthrough

1. Prepare the existing exchange/format configuration and instrument catalogue, a private Unix socket directory, the general file, database environment and private management credentials. Run static validation first, then start the headless daemon using the commands in [Running](#running).
2. Connect the TUI with an operator token. Viewing Streams creates no demand. Use `c`, select Source, and F2 to look up an authoritative instrument template. Review each create plan. If metadata is unavailable, correct the catalogue rather than inventing quantity specifications.
3. Create a Merge with ordered existing Source IDs, then a Convert with its data input, target and declared references. Review plans and inspect Definitions/Dependencies. Choose Run temporarily for an expiring UI-owned hold, or AlwaysOn for durable running intent. Inspect readiness reasons separately from coverage/integrity.
4. In Runtime press `r`, enter the absolute worker target, and choose apply-now or save-and-apply. Review the plan. Watch requested/live/starting/retiring/executing fields and the operation result; acceptance alone is not completion. The integrated test exercises 4 → 8 → 4 while actual financial publications continue under the same stream generations.
5. Preview a restart. If dependents are affected, use the explicit cascade review or reject the plan. Existing Unix consumers receive their continuity boundary and must subscribe anew; a restart is not transparent continuity.
6. Press `q` and reconnect later. The daemon remains owned independently. UI-only holds are best-effort released/expire; AlwaysOn and other consumers remain. Application shutdown is a separate reviewed administrator action.

A Rust operator can use the same client contracts:

```rust,no_run
use marketforge_engine::live::management::client::{ClientOptions, MarketForgeManagementClient};
use marketforge_engine::live::management::protocol::Action;
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

See the [management API and runtime limits](../../reference/live/management-api.md).
