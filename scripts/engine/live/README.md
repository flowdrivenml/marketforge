# Live exchange test fixtures

Offline Live tests read native exchange captures from the worktree's ignored
`data/tests/live/` directory. Prepare this directory after cloning or creating
a worktree:

```bash
python3 scripts/engine/live/fixtures.py import --from data/.cache/live
python3 scripts/engine/live/fixtures.py check
```

Pass an absolute source path to import from another worktree. Relative source
paths resolve from the script's workspace, even when invoked from elsewhere.
Legacy captures are only consulted by explicit import. Use `import --from engine/target`
for captures produced under the old `live-rest-fixtures` and
`live-rest-service-fixtures` directories. Import preserves source files and copies native JSON bytes unchanged. Existing valid destination files
are kept unless `--replace` is supplied. Missing and invalid fixtures make
`check` exit nonzero. Import also exits nonzero for requested files that could
not be prepared.

## Required inventory

All 15 BTC profiles are covered: spot, linear and inverse for Binance, Bitget,
Bybit, Gate.io and OKX.

| Directory relative to `data/tests/live/` | Files | Native representation |
|---|---:|---|
| `websocket-fixtures/trades/` | 15 | `<profile>.json`, an array containing one or more complete native trade frames |
| `websocket-fixtures/depth/` | 15 | `<profile>.json`, an array of exactly 12 native depth frames |
| `rest-fixtures/` | 30 | `<profile>__<endpoint>.json`, standalone response bodies for 15 depth snapshots and 15 trade responses |
| `rest-service-fixtures/` | 35 | The same filename convention, with native response bodies observed through the production REST service |

Both REST families require `depth_snapshot` for every profile and
`recent_trades` for every profile except Gate.io, which uses `historical_trades`
for all three markets. The production service family additionally requires
Binance Inverse `aggregate_trades`, Binance Spot `historical_trades`, and
`historical_trades` for all three OKX markets. Root arrays and exchange-specific
envelopes retain their native shapes. Standalone trade tests assert the full
population returned by the configured endpoint defaults, namely 60 for Bybit
Spot, 500 for Binance and Bybit derivatives, and 100 for the other profiles.
Capture reports and surplus endpoints are outside the required inventory.

Tracked synthetic inputs, configuration fixtures, and instrument specifications
remain under `engine/tests/live/support/data/` and their existing test modules.
Capture preparation makes no changes to financial normalization or to documented
provisional instrument assumptions.

## Collect from public exchanges

```bash
python3 scripts/engine/live/fixtures.py collect --help
python3 scripts/engine/live/fixtures.py collect \
  --rest-weight "binance_spot=${SPOT_WEIGHT}/${SPOT_WINDOW_SECONDS}" \
  --rest-weight "binance_linear=${LINEAR_WEIGHT}/${LINEAR_WINDOW_SECONDS}" \
  --rest-weight "binance_inverse=${INVERSE_WEIGHT}/${INVERSE_WINDOW_SECONDS}"
python3 scripts/engine/live/fixtures.py check
```

Use verified request-weight capacity and window values for the current Binance
API families and egress. A weight binding is needed only when that profile has
REST files to capture. The runner reports any files blocked by missing bindings
and can still collect the other requested files. REST requests from both paths
charge the same compiled budgets within the run. Standalone requests stop
collection on a detected throttle or ban, and the production REST client
applies exchange feedback through its existing limiter. Coordinate with other
traffic sharing the same IP. The process-local limiter has no visibility into
other clients' usage.

Collection requires Cargo, Python 3 on Unix, and access to the configured public
HTTPS and secure WebSocket endpoints. It runs exactly the two opted-in Rust
capture tests needed by the missing inventory. Existing files are retained by
default. `--replace` requests fresh captures, but the existing file is replaced
only when a complete, structurally valid candidate is ready. Each file stages
independently, so successful captures remain usable after a partial failure.
Run `collect` again without `--replace` to retry missing or invalid files.

The WebSocket collector waits for subscription confirmation, captures a native
trade frame or 12 depth frames within 60 seconds, and bounds each capture to
16 MiB. It uses one connection generation per file. Bybit, Bitget, and OKX depth
capture starts at a native snapshot followed by incrementals. Binance and
Gate.io depth capture contains native deltas only. A disrupted capture can be
retried as a new capture. The collector leaves sequence, timestamp, side and
decimal values untouched.

Regional restrictions, unavailable routes or markets, throttling, and changing
exchange response shapes can prevent collection. The runner sends no credentials.
An endpoint declared authenticated is skipped. If a configured public endpoint
requires authentication or is denied on the current egress, import its existing
fixture from the corresponding family. The inventory includes Binance Spot's
[public old trade lookup](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market#old-trade-lookup).
A shape check cannot establish financial correctness or prove provenance for
imported data. The offline Rust contracts still verify that the observations meet
their financial and continuity assertions.

## Run offline verification

Ordinary tests read local files and do not collect from exchanges.

```bash
cargo test --locked --manifest-path engine/Cargo.toml --no-default-features \
  --features management-api,management-tui,stream-client \
  --test live_exchange_contracts --test live_normalization --test live_rest --no-fail-fast

cargo test --locked --manifest-path engine/Cargo.toml --no-default-features \
  --features management-api,management-tui,stream-client --lib \
  live::infrastructure::normalization::depth::

cargo test --locked --manifest-path engine/Cargo.toml --no-default-features \
  --features management-api,management-tui,stream-client --lib \
  normalizes_all_15_captured_rest_trade_fixtures

python3 -m unittest discover -s scripts/engine/live -p 'test_*.py' -v
```
