# Source-local trade identities and verified chronological append

The Bybit spot `BYBIT-T1` profile now declares `source.trade_id_scope = "source_local"`. Its `id` column is preserved verbatim in canonical `trade_id`. Identity scope belongs to the fingerprinted source profile; no identifier is prefixed with a filename, path, timestamp, date, or task ID.

The retained source contract is documented in [BYBIT-T1 — Spot Trades](../tofixlater/raw-formats/BYBIT-T1%20%E2%80%94%20Spot%20Trades.md): `id` is an integer sequential within the archive. The user-confirmed three-archive workload also restarts this column at 1 in every archive. This declaration applies to that profile only; the other Bybit profiles do not inherit it.

## Identity contracts

Schema-2 profiles accept optional source metadata:

```toml
[source]
# Other existing source fields stay unchanged.
trade_id_scope = "source_local"
```

Scopes are `global`, `instrument_scoped`, `source_local`, and `unreliable`. `instrument_scoped` additionally requires a nonempty `trade_id_reset_domain`: an explicitly established, stable domain identifier. Other scopes reject that field. Dynamic date/reset expressions and automatic archive-date domains are not implemented. Missing IDs remain unreliable even under a reliable profile contract.

The compiler carries the contract in `raw_schema.marketforge_trade_identity`, includes it in the compiled profile fingerprint, and rejects trade identity metadata on depth profiles. It is also recorded in each reconciled source observation. Existing profiles without this metadata retain their existing identity rules; unknown Bybit numeric identities are not silently promoted to source-local or global identities. A source-local declaration overrides UUID-shape recognition too.

The tracked profile file under `data/.config/historical/profiles/trades/bybit.spot.spot.trades.toml` has been updated in place, preserving its existing comments. Old resolved jobs with a previous fingerprint must be resolved again from the authoritative profile; stale fingerprints are still rejected. No new runtime resource controls or job JSON fields were added.

## Execution decision and correctness

Multi-source normalization still decodes each physical source once. Each accepted canonical trade is written once to a bounded owned spool; the same sink incrementally observes first/last/minimum/maximum accepted timestamps and detects every accepted timestamp regression. Source read, normalization, integrity checks, source ordinals and spool completion must succeed before publication is considered. Contradictory native identity declarations within otherwise compatible sources fail explicitly rather than splitting the stream to bypass reconciliation. Family routing continues to decode once and keeps observations independent for each concrete instrument.

`chronological_append` is selected only when every nonredundant source has an explicit source-local contract, complete consistent timestamp evidence and validated internal chronology. Sources are ordered by their observed accepted minimum timestamps, and every adjacent pair must satisfy `previous_max < next_min`. Empty accepted sources have no interval but their spool completion markers are still consumed and checked. Rejected rows remain governed by the existing immediate and final integrity policy; a permitted rejection is neither an accepted trade nor a suppressed trade.

The append cursor opens one source spool reader at a time, checks source ownership and chronology again while consuming it, validates checksums/footers/trailing data, and sends trades to the existing single long-lived Parquet writer. Equal timestamps within a source, repeated IDs within a source, original source ordinals and native ID strings are preserved. A shared admitted memory quota owns buffers and the source inventory. The single-source direct-streaming algorithm is unchanged.

`identity_reconciliation` remains mandatory for global/scoped identities, missing/unestablished contracts, and overlapping or touching accepted intervals. A global ID can identify conflicting versions of one event at different timestamps, so disjoint timestamp ranges alone must not bypass exact identity resolution. Existing precedence, later-timestamp winners, exact duplicate/conflict counts, same-source repeated-ID semantics and deterministic chronological ordering remain enforced. Source-local, missing or unreliable identities in overlapping sources fail explicitly. No timestamp-based deduplication was introduced.

All output stays in staging until the normal final integrity evaluation, manifest validation and transactional publication succeed. Source, scratch, memory, decoder, writer, completion-marker and final-integrity failures cannot commit a dataset. Depth replay, reconstruction and boundary rules are unchanged.

## Work eliminated and diagnostics

Eligible streams eliminate both external trade sorts, their initial runs and merge passes, the identity-winner spool, and all rewrites of canonical trades between those stages. They retain one canonical spool write during normalization and one read during publication; there is no additional physical archive scan and no archive-sized memory accumulation.

The synthetic three-source matrix verifies seven complete expected canonical rows and exact source ownership across 2 MiB/4 MiB reconciliation quotas, JSON/typed trade payloads, and zlib/uncompressed scratch. In every case diagnostics assert seven spool frames written, seven read, **zero sort runs and zero merge passes**. This is deterministic work evidence, not a real-workload speedup measurement.

The existing diagnostics record `trade_execution_path`, its reason, source count, accepted timestamp boundaries and source ordering. The performance snapshot includes physical source records, processed records, canonical rows written, rejected/suppressed rows, scratch reads/writes and workspace peak, peak accounted memory, processing/publication wall times and throughput, Parquet files and actual flushed row groups. Processing timing covers stream normalization through Parquet finalization; it excludes resource admission, final manifest validation and dataset commit. Family-group timings can overlap across concrete streams. Accounted memory is not measured RSS, and workspace scratch peaks must not be summed as independent simultaneous run peaks.

Decision/publication snapshots are explicitly uncommitted. The existing dataset-publication diagnostic and final report status become committed only after the dataset rename succeeds. A short path/reason line is emitted to stderr even with optional diagnostics disabled; structured CLI stdout remains unchanged.

## Validation and real execution

Tests executed during implementation:

- 114 synthetic normalization unit tests passed, including source-local append, strict global/scoped identity resolution, source-local/missing/unreliable overlap rejection, equal boundaries, internal regressions, repeated IDs, reverse task order, complete Parquet fields, source ownership/manifest validation, rejected-row policies, empty accepted sources, failures and resource exhaustion.
- 39 configuration/profile tests passed, validating scope metadata, reset-domain rules, fingerprint changes, authoritative runtime settings and other Bybit profiles remaining unchanged.
- 9 tiny CLI contract tests passed, including diagnostics enabled with clean machine-readable stdout.
- The real integration target was compiled in release mode with `historical,historical-real-tests`; no large real archive workload or benchmark sweep was launched.

An existing family admission test still expected the pre-refactor fixed eight handles. Its assertion now derives the expected reservation from configured fan-in and existing headroom, matching the already implemented resource estimator; no resource limit was raised.

Run the real workload from the repository root:

```bash
cargo test \
  --release \
  --manifest-path engine/Cargo.toml \
  --features "historical,historical-real-tests" \
  --test historical_normalize \
  process_executor::processes_multiple_trade_tasks_into_staging \
  -- --exact --nocapture --test-threads=1
```

For retained live diagnostics, set `MARKETFORGE_TEST_DIAGNOSTICS=detailed` before that command, or enable diagnostics in authoritative `data/.config/historical/general.toml`. The environment override affects diagnostic mode only; the real test continues loading production resource settings and retains its isolated scratch/output directories.

```sh
report=$(rg --files --hidden --no-ignore data/tests/historical/normalize/execution/real_multi_archive_trades -g performance.json | sort | tail -n 1)
jq '{status, resources, process_wide}' "$report"
```

The expected accepted/output count for the user-confirmed three archives is **3,822,020 only if every physical record passes normalization and integrity checks**. This implementation does not force that count or relax those checks. Actual full-workload runtime, peak process memory and throughput remain unmeasured here. Conservatively, profiles without a source-local contract still use strict reconciliation even if their timestamp intervals appear disjoint.
