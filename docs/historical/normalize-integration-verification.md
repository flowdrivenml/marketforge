# Historical normalization final integration verification

Status: verified for the supported bounded scope; full-workload readiness is conditional. See [final summary](../../.agents/reports/final-summary.md) and [Task12 report](../../.agents/reports/task-12.md). No actual performance benchmarks were executed.

## Production architecture and contracts

`general.toml` → prepared batch3/offline authority → unified profile resolver → per-job compatible stream plan → shared resource scheduler → sequential source workers and long-lived trade/depth writers → persisted row/event/source validation and integrity evaluation → manifest3 → same-filesystem dataset rename → CLI response1. Depth reconstruction remains independent per source. See [output contract](normalize-stream-output.md) and [CLI contract](normalization-cli.md).

Task12 corrected production Degrade handling, resource-failure isolation, committed provenance discovery for conversion, generic canonical source-slice resolution, equal-time depth checkpoint precedence, manifest stream/path validation, duplicate new-test registration and persistent direct-test outcomes. No canonical financial schema, quantity algorithm, source archive, production runtime budget or integrity threshold was changed.

## Final verification results

| Check | Result |
|---|---|
|Library / normalize binary unit|204 / 1 passed|
|Source-only normalization / support|158 / 54 passed|
|Synthetic benchmark infrastructure/analysis|36 passed; no trial execution|
|26 shared book/depth/Parquet/merge/conversion targets|211 passed|
|Synthetic materialization/readers|192 passed|
|Bounded Bybit trade / depth|2 tests passed; 2 trades / 420 depth levels|
|Bounded Binance linear perpetual trade|1 test passed; independently checked first trade|
|Historical, minimal and monitor-tui compilation|passed|
|Merge/convert test-target compilation|passed|
|Clippy lib/binaries|passed with existing warnings|
|Changed-file rustfmt / diff checks|passed|
|Whole-repository cargo fmt --check|fails on pre-existing unrelated live formatting|
|Full-workload nonexecuting plan|blocked: Task88 BTC-USD family lacks authoritative members/specifications|
|Complete corpus/monthly archives, macOS, real performance|not executed|

Final focused correctness failures: none. Earlier implementation failures were fixed and rerun; prerequisite diagnostics and baseline formatting failures are not reclassified as passing. Checkpoint-focused11 and earlier focused subsets overlap broader results. New stream cases are registered only in the support target. Artifact-producing normalization checks retain unique outputs and outcomes under data/tests/historical/normalize; originals and production outputs are untouched.

Trade checks include timestamps/IDs/side/exact quantities through33 profile cases, shared writers/spans, equal timestamps/partitions, rotation, no missing/duplicated rows, job boundaries and physical source-slice decoding. Depth checks include independent initialization, changes/deletions, gaps/recovery, local/global event identity, zero-row events, segment offsets, full source boundaries and independent intermediate replay states. Integrity/source checks cover eligible denominators, windows, unattributed errors, fatal compressed I/O and Fail/Degrade behavior. Metadata/writer/resource/commit failures retain failed status and cannot publish success.

Merge uses safe manifest ownership plus source-scoped depth indices and physical slices. Conversion provenance resolution works without private job directories and rejects checksum mismatch; algorithms were not rewritten. Legacy manifest2/boundary1 depth is verified. Benchmark correctness verification accepts new stream metadata and produces layout-independent semantic evidence in unmeasured correctness tests. This is not benchmark execution or a throughput claim.

## Supported profile inventory

Every row below passed its named synthetic execution case. A real-prefix entry proves only that explicitly bounded source sample. Other archive oracles and feeds are unverified, even when files exist. Six OKX profile classifications contain unresolved family selections; see Task06's detailed matrix and generated catalog coverage artifacts.

|Profile|Synthetic execution|Real archive verification during final integration|
|---|---|---|
|`bitget.perpetual.linear.depth`|passed|not executed |
|`bitget.spot.spot.depth`|passed|not executed |
|`bybit.future.linear.depth`|passed|not executed |
|`bybit.perpetual.inverse.depth`|passed|not executed |
|`bybit.perpetual.linear.depth`|passed|not executed |
|`bybit.spot.spot.depth`|passed|snapshot + next event: 420 levels |
|`gateio.perpetual.inverse.depth`|passed|not executed |
|`gateio.perpetual.linear.depth`|passed|not executed |
|`gateio.spot.spot.depth`|passed|not executed |
|`okx.future.inverse.depth`|passed|not executed |
|`okx.future.linear.depth`|passed|not executed |
|`okx.perpetual.inverse.depth`|passed|not executed |
|`okx.perpetual.linear.depth`|passed|not executed |
|`okx.spot.spot.depth`|passed|not executed |
|`binance.future.inverse.trades`|passed|not executed |
|`binance.future.linear.trades`|passed|not executed |
|`binance.perpetual.inverse.trades`|passed|not executed |
|`binance.perpetual.linear.trades`|passed|independent first-trade oracle |
|`binance.spot.spot.trades`|passed|not executed |
|`bitget.perpetual.linear.trades`|passed|not executed |
|`bitget.spot.spot.trades`|passed|not executed |
|`bybit.future.linear.trades`|passed|not executed |
|`bybit.perpetual.inverse.trades`|passed|not executed |
|`bybit.perpetual.linear.trades`|passed|not executed |
|`bybit.spot.spot.trades`|passed|bounded prefix: 2 trades |
|`gateio.perpetual.inverse.trades`|passed|not executed |
|`gateio.perpetual.linear.trades`|passed|not executed |
|`gateio.spot.spot.trades`|passed|not executed |
|`okx.future.inverse.trades`|passed|not executed |
|`okx.future.linear.trades`|passed|not executed |
|`okx.perpetual.inverse.trades`|passed|not executed |
|`okx.perpetual.linear.trades`|passed|not executed |
|`okx.spot.spot.trades`|passed|not executed |

## Commands actually executed

```sh
cargo test --manifest-path engine/Cargo.toml --features historical --lib --bin marketforge-normalize --test historical_normalize --test historical_normalize_support --test historical_normalize_benchmark
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge materialize_
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge checkpoint
cargo test --manifest-path engine/Cargo.toml --features historical-real-tests --test historical_normalize_support representative_
cargo test --manifest-path engine/Cargo.toml --features historical-real-tests --test historical_normalize exchanges::binance::perpetual_linear_archive_prefix
cargo check --manifest-path engine/Cargo.toml --no-default-features
cargo check --manifest-path engine/Cargo.toml --features historical,monitor-tui --bins
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_convert --no-run
cargo clippy --manifest-path engine/Cargo.toml --features historical --lib --bins
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
engine/target/debug/marketforge-normalize-benchmark --input engine/tests/historical/fixtures/normalize.json --project-root /home/pasha/projects/marketforge --dry-run
```

Also executed the following shared targets in one `cargo test --manifest-path engine/Cargo.toml --features historical` invocation, each selected with `--test`:

```
book_batch book_emit book_segment book_sequence book_store
depth_csv_adapter depth_json depth_outcome depth_processor depth_relative
depth_scalar depth_snapshot_assembler depth_snapshot_differencing
depth_source_metadata depth_spec depth_transforms depth_xlsx_decoder
parquet_decimal parquet_depth_batch parquet_depth_events parquet_depth_segments
parquet_depth_writer parquet_trade_batch parquet_writer shared_merge shared_conversion
```

Historical cargo check and merge test-target compilation also passed during Tasks10/11. Changed Rust files were formatted/checked with rustfmt edition2024 and skip_children=true, leaving unrelated files intact. Real-prefix tests were rerun after final integration changes. No complete-real feature inventory was executed.

## Readiness limits and deferred work

Full-corpus planning cannot finish until authoritative family identities/specifications are supplied. Unsupported overlap/state continuation, stateful sortable feeds, mixed-instrument unified-merge inputs and conservative profile grouping remain explicit. Resource admission is estimated, with bounded metadata limits; realistic memory/performance behavior and macOS are unverified. Degraded timestamp output is retained in source order and is not promised chronological to downstream readers. Conversion needs its instrument/currency catalog and reachable canonical input provenance.

Task12 completes integration verification of implemented supported behavior; it does not certify the whole archive corpus. Python Forge must integrate the offline authority, ordering declarations, prepared batch3, exact response1 IDs/status, manifest3 spans/boundary2 and transactional registry lifecycle. No Python/DB implementation or performance experiment was started.
