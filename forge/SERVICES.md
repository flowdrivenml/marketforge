# Independent Forge services

The public command remains `marketforge`. Use repository-root `.venv/bin/marketforge`
or `uv run --project forge --locked marketforge`. Global `--json` goes **before**
the command. Database operations require `--testing` and the existing safe
`MARKETFORGE_DATABASE_URL_TEST` loader. Production has no fallback.

## Acquisition scheduling

```console
marketforge acquisition map NAME --testing --refresh --requests-per-second 1
marketforge acquisition download NAME --testing --silent
marketforge acquisition plan NAME --cache-only --verify-local --testing --limit 100
```

One workspace/data-root owner encloses the historical operation. Three request
workers have separate connections; each download request has at most two file
workers, sharing a global transfer semaphore. Thus at most nine acquisition DB
connections are open (three request connections plus six file-worker connections).
Each of the five supported exchanges has one pre-initialized HTTP client, with
its existing bounded pool, limiter, retries, redirects, and cooldown. Four Gate
probe workers overlap independent candidates without creating a task per hour.
Pagination remains sequential. Scope locks coalesce identical overlapping requests;
striped acquisition locks serialize acquisition of the same archive. Reports keep
configuration indices despite completion order. Completion of the last **index**
does not finalize totals: all discovery work must have finished.

Progress lists multiple active requests and distinct probe/found/absent/failed/
registered activities. Per-request diagnostics retain aggregate counts and eight
recent probe examples, not an unbounded candidate history. Missing objects do not
mean unsupported products. The Rich view evaluates elapsed time on every bounded
refresh, even without new operation events. Rendering failure remains independent
of registry correctness.

Bitget queries use UTC+8 source dates, including the source day overlapping an
exclusive UTC end. Windows contain at most seven source calendar days, and objects
are filtered against the original half-open UTC request. Numbered partitions are
not merged. Gate's explicit selection is retained:

```toml
[[requests]]
exchange = "gateio"
instrument_type = "perpetual"
market_category = "linear"
data_type = "order_book_l2"
scope = { kind = "instrument", identifier = "BTC_USDT" }
source_options = { feed = "snapshot" } # or "incremental"
```

Incremental `orderbooks/*.csv.gz` identities are unchanged. Snapshot
`orderbooks_slice/*.gz` has a separate scope/cache identity. The current adapter
version includes the source-calendar fix; old discovery snapshots are not reused.
Only the four sanitized `bitget_*.json` fixture files are unignored. A regenerated
Forge Repomix export at `/tmp/marketforge-forge-services.xml` included all four.

## Markets and retained statistics

```console
marketforge markets sync --exchange bybit --market spot --instrument BTCUSDT --testing
marketforge markets refresh --exchange bybit --market spot --instrument BTCUSDT --testing
marketforge markets list --exchange bybit --market spot --sort freshness --limit 50 --testing
marketforge markets show bybit.spot.BTCUSDT --testing
```

Without filters, metadata/statistics cover Binance, Bybit, OKX, and Gate spot,
linear perpetual, and inverse perpetual; Bitget covers spot and linear perpetual.
No options or dated futures are added. Metadata pages validate before persistence;
partial observations do not delete absent instruments. Metadata sync completion is
retained, and statistics refresh requires successfully synchronized metadata for
its requested scope. Existing catalogue IDs/specification/raw history are retained.
Current metadata is not substituted for historical recording specifications.

Collection has three workers with separately owned connections and one shared
client per exchange. The market collector uses a conservative 0.2 requests/second
budget, including weighted bulk endpoints; it does not issue per-instrument ticker
requests. Browsing never syncs. Its `null` fields mean missing, not zero; `stale`
means more than one hour since collection. Policy freshness may be stricter.
Native units remain explicit. USDT/USDC never silently become USD. OKX linear
base-volume times latest-price turnover is explicitly an **estimate**. COIN-M
fixed quote-value contracts preserve contract and base volumes separately. Gate
uses native base/quote fields rather than deprecated `volume_24h_usd`.
Funding/open interest are retained where the chosen bulk endpoint supplies them;
otherwise they remain missing. Source observation time, fetch time, window, and
fetch-time-based window assumptions remain distinct. The latest cache derives
from persisted observations and orders by window end **and** source fetch time.

Official sources audited: [Binance COIN-M ticker](https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics),
[Bybit ticker](https://bybit-exchange.github.io/docs/v5/market/tickers),
[OKX ticker](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-tickers),
[Gate futures](https://www.gate.com/docs/developers/apiv4/en/futures/), and
[Bitget market data](https://www.bitget.com/docs/catalog/market/market-data).

## Immutable ranking and standalone streams

```console
marketforge markets rank new default
marketforge markets rank validate default
marketforge markets rank build default --testing
marketforge markets rank list --limit 50 --testing
marketforge markets rank show RANKING_UUID --testing
```

Policies resolve under `data/.config/markets/rankings/`. Example:

```toml
version = 1
metric = "turnover_24h"
exchanges = ["bybit"]
markets = ["spot"]
instruments = ["bybit.spot.BTCUSDT"]
comparison_currency = "USDT"
max_age_seconds = 3600
max_window_skew_seconds = 300
estimates = "exclude"
allow_partial_universe = false
manual_preferences = ["bybit.spot.BTCUSDT"]
# Conversion is optional, explicit and retained in the snapshot:
# conversion_as_of = 2026-11-04T00:00:00Z
# conversion_source = "explicit user approximation"
# [conversion_rates]
# USD = "0.999"
```

The default refuses incomplete, stale, estimated, or incomparable inputs. An
explicit partial policy records excluded entries and missing collection scopes;
it cannot masquerade as full exchange coverage. Explicit instrument selections
must all resolve. Manual preferences move eligible instruments ahead of numeric
ordering, in listed order; they never rescue excluded observations. Remaining
scores sort descending, with canonical instrument reference breaking ties.
Conversion uses 50-digit Decimal arithmetic and retained policy/provenance.
Atomic publication stores every selected observation and protects published
snapshots, entries, and referenced observations from mutation. A later refresh or
policy build produces a new snapshot. These are **current priorities**, not
historical backtest-valid observations.

`forge.markets.ranking.service.resolve_snapshot(connection, UUID, streams)` returns
`ResolvedStream(name, stream_rank, ranking_position)` for a list of `ConcreteStream`
objects. Ranks start at one and are consecutive. Names include instrument, data
type, and an explicit Gate depth feed, e.g.
`gateio.linear.BTC_USDT.order_book_l2.snapshot`. Missing/excluded instruments and
duplicate streams are rejected. No Rust job schema is assumed.

## External import walkthrough

```console
marketforge acquisition import new my-recording --from /path/to/recording
# Edit data/.config/acquisition/import/my-recording.toml explicitly.
marketforge acquisition import inspect my-recording
marketforge acquisition import validate my-recording
marketforge acquisition import plan my-recording
marketforge acquisition import format new my-recording --profile my-recorder-trades-v1 --input trades.csv --header
marketforge acquisition import run my-recording --testing
# The next invocation verifies and reuses the same registered copy.
marketforge acquisition import run my-recording --testing
```

`new` deliberately leaves identity unresolved instead of inferring an exchange
from filenames. A complete recording configuration can be:

```toml
version = 1
source_key = "my-independent-recording"
source_kind = "recording"
root = "/path/to/recording"
files = ["trades.csv"]
max_files = 1000
data_type = "trades"
identity = { exchange = "binance", market_category = "spot", instrument_type = "spot", symbol = "BTCUSDT" }
specifications = { as_of = 2026-01-01T00:00:00Z, base_asset = "BTC", quote_asset = "USDT", quantity_type = "base" }
# Optional declared interval; never inferred from a filename:
# interval = { start = 2026-01-01T00:00:00Z, end = 2026-01-02T00:00:00Z }
# Optional active Rust profile ID after approval, not a draft path:
# format_profile = "binance.spot.spot.binance-t1.v1"
```

Use `patterns = ["**/*.csv"]` instead of `files` for bounded expansion. `*` stays
within one directory; `**` is recursive. Expansion stops at `max_files` and at
100,000 inspected entries. A run freezes its input list/stat identity into the
resolved plan. Later files do not join it. Source size, inode/device, mtime/ctime
are checked around hashing and copying, and the copied hash must match. These
checks cannot prove immutability against a privileged adversarial writer; reliable
imports require stable source files. Cancellation is checked between bounded I/O
chunks. Originals are never changed or removed.

The database distinguishes source identity, file identity, content revision, and
market identity. Equal bytes from unrelated source keys/objects are not deduplicated.
Changed bytes get a new revision/acquisition. Declared identity/specification
conflicts are rejected rather than silently changing earlier provenance. Copies
use temporary files and the downloader's atomic publication/common reservation
recovery. Final-registration failure can be reconciled from a fresh connection
without copying again. Arbitrary raw formats, including binary/Parquet, may be
imported; this does not adopt them as normalized datasets.

The format helper samples at most 64 KiB. It supports CSV/JSONL evidence and
plain/gzip/selected ZIP members; select `--member` for multi-layout ZIPs. Unsupported
layouts still produce an incomplete template and diagnostics where an installed
physical template exists. Tar member sampling and arbitrary binary/depth parsing
are not promised. Generated `physical.toml`, `profile.toml`, and `diagnostics.json`
are under `data/.config/historical/format-drafts/PROFILE/`, outside the active
recursive `formats/` loader. `unsupported_reason` marks the profile as DRAFT.
Timestamp units, quantity interpretation, ordering, side/depth/snapshot/sequence
semantics and contract multipliers remain unresolved unless explicitly declared.
Drafts do not alter import configuration or active profiles, and refuse overwrite
without `--overwrite`.

The inspected engine has no stable profile-only validation CLI: its executable
accepts normalization jobs, which this helper does not run. Structural TOML
validation is therefore explicit and engine validation remains pending. Unknown
venues can be raw-imported, but their admission to Rust's canonical exchange
model is not invented by Forge. Final job generation/normalization integration
waits for the stable engine job and validation contracts.

The task's real tiny walkthrough is retained under
`/tmp/marketforge-import-walkthrough-uj5_rngh`: one CSV was imported, its second run
reused one copy, and a real physical/profile draft was created with unresolved
mappings. Originals and managed bytes remain present. A one-instrument current
ranking smoke snapshot is `5941f501-0e6d-414d-a3f4-a61500f6fa96` in testing only.

## Inventory, runs, doctor, backup

```console
marketforge storage list --origin external --limit 100 --testing
marketforge storage show local:123 --testing
marketforge storage verify --archive local:123 --testing
marketforge storage reconcile --archive local:123 --testing
marketforge runs list --limit 50
marketforge runs show RUN_UUID
marketforge runs logs RUN_UUID --limit 100
marketforge runs report RUN_UUID
marketforge doctor --testing
marketforge database backup /new/backup/directory --testing
marketforge database restore /existing/backup/directory --testing --disposable-testing --destination marketforge_restore_0123456789ab
```

Cache-only planning does no HTTP, writes no registry rows, and distinguishes
registered-but-unverified copies from read-only verified samples. Inventory is
bounded and scoped to the configured raw root. Reconciliation uses common
reservation recovery, retains unverified partials, and never redownloads or deletes
data files. Run inspection reports missing/interrupted reports honestly. Doctor
checks Python, database identity/history, storage and PostgreSQL client tools;
engine readiness remains pending, not falsely ready.

Backups contain `forge_meta`, `catalog`, `historical`, and `ranking`, verified TOML
configuration snapshots, checksums, and bounded artifact-reference audits. They
exclude `.secrets`, raw/dataset bytes, and run artifact bytes. Retain those bytes
separately. Configuration references are returned for explicit review/activation;
restore does not overwrite a workspace's active configuration. Backups may retain
upstream locations already stored in the registry; protect the backup accordingly.

Restore refuses `marketforge_test`, production, and existing databases. Only a
new explicitly named `marketforge_restore_<8..32 hex>` destination is eligible.
Checksums are checked before creation, restore is transactional, and restored
identity must be testing. Failed disposable destinations are retained for inspection.
A real backup succeeded at
`/tmp/marketforge-services-backup-06c4f9547b54475fa4b0e0d67a6d55ac`.
**Live restore verification was blocked:** the authorized testing role has
`CREATEDB = false`. No role privileges were changed and no database was reset,
replaced, or dropped. Deterministic restore-guard tests passed; a successful live
round trip is not claimed.

## Migrations and changed boundaries

New numbered migrations (existing 0001–0003 were not edited):

- 0004: retained metric refreshes/observations and observation-derived latest cache.
- 0005: dedicated immutable ranking snapshots and entries.
- 0006: external sources/revisions; exactly one remote/external local origin;
  protection for observations referenced by published rankings.
- 0007: retained metadata-sync scope completion, preventing partial catalogue
  collection from masquerading as a complete statistics/ranking universe.

This task adds `forge/markets/{models,repository,service}.py`,
`forge/markets/providers/public.py`, `forge/markets/ranking/`,
`forge/historical/imports/`, `forge/storage/service.py`,
`forge/orchestration/inspection.py`, `forge/shared/async_work.py`,
`forge/shared/config/toml.py`, `forge/shared/database/backup.py`, CLI handlers
`markets.py`, `rankings.py`, `imports.py`, `maintenance.py`, and corresponding
responsibility-grouped tests. It extends existing acquisition scheduling,
progress, discovery, common storage recovery, CLI registration and presentation,
and narrowly updates `.gitignore` for sanitized fixtures. Pre-existing Gate feed
models/identities and the user's Rust refactor are preserved. Rust source,
engine migrations/formats, normalization/merge/conversion semantics, unrelated
notes, TUI/live orchestration, benchmarks and ML tooling were not modified by
this task.

## Verification for this implementation

Final checks used repository-root `.venv/`: Ruff lint/format, strict Pyright, and
325 maintained Python tests, including real verified testing-database tests, with
no skips. The testing URL was loaded with `testing_dsn` and passed only through
the subprocess environment, never sourced as shell code. Earlier per-part checks
passed before proceeding to the next service.

Bounded live checks succeeded for Gate inverse BTC_USD metadata/tickers, Bitget
linear BTCUSDT metadata/tickers, and Bybit spot BTCUSDT metadata/tickers. Real
`markets sync` and `refresh` persisted Bybit spot BTCUSDT; stored browsing and a
one-instrument ranking were exercised. A final Bitget instrument probe confirmed
`category=USDT-FUTURES`, `type=perpetual`, `symbolType=crypto`: only the contract
`type` distinguishes deliveries. [Official instrument documentation](https://www.bitget.com/docs/catalog/market-market-data/market-instruments)
also specifies this distinction. This is not exhaustive live verification of all
14 exchange/market combinations. No bulk archive download, exhaustive public
scan, Rust normalization, or benchmark was run for this services task.

The requested root acquisition configuration `test` was absent at the final
cache-only CLI smoke; the command reported that missing configuration rather
than inventing a plan. Use an existing saved configuration NAME for map/download/
plan commands. JSON CLI output, silent/progress cleanup, bounded concurrency,
cooldowns, source calendars, external publication recovery, and immutable ranking
behavior are covered by deterministic tests. A complete live restore round trip
still requires a testing role with permission to create the disposable destination.
