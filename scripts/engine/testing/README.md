# Bounded Rust verification

Run the portable integration suite from the repository root. Run different feature
configurations in sequence when sharing a Cargo target directory, since CLI tests
launch the executable at Cargo's stable binary path.

```sh
set -o pipefail
cargo test --manifest-path engine/Cargo.toml --features historical,monitor-tui --tests --no-run 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical,monitor-tui --tests 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical --tests 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical,monitor-tui,normalization-test-hooks --test normalization_dashboard 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical,monitor-tui,normalization-test-hooks --lib tui::processing::tests:: 2>&1 | cpout
```

Cargo honors each target's required features. Use the following to compile and
run the additional live, stream, and management targets. Ignored network,
database, and performance cases require explicit opt-in.

```sh
set -o pipefail
cargo test --manifest-path engine/Cargo.toml --features management-api,management-tui,stream-client --tests --no-run 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features management-api,management-tui,stream-client --no-fail-fast \
  --test live_application --test live_config --test live_derived --test live_exchange_contracts \
  --test live_integrity --test live_metrics --test live_network --test live_normalization \
  --test live_processing --test live_recovery --test live_rest --test live_router \
  --test live_source --test live_websocket --test management --test management_client \
  --test management_tui --test stream_client --test stream_performance --test stream_protocol \
  --test stream_transport 2>&1 | cpout
```

## Per-case deadlines

Use the supervisor from the repository root for a 30-second deadline per selected test,
including setup. It stops the test's process group on expiry. Use a fresh
temporary directory for each execution.

```sh
set -o pipefail
run_dir=$(mktemp -d)
export CARGO_TARGET_DIR="$run_dir/target"
(
  for features in historical,monitor-tui historical management-api,management-tui,stream-client; do
    artifacts="$run_dir/$features.jsonl"
    cargo test --manifest-path engine/Cargo.toml --locked --offline --features "$features" --tests --no-run \
      --message-format=json > "$artifacts" || exit 1
    python3 scripts/engine/testing/bounded_tests.py "$artifacts" "$run_dir/$features" --run 2>&1 | cpout || exit 1
  done
)
```

Without `--run` the supervisor only lists tests. `--targets` and `--cases` are
full-match regular expressions. It runs exactly one case per invocation, records
filtered and ignored cases, and exits nonzero on failures, timeouts, discovery
errors, or an empty runnable selection. It requires successful Cargo artifact
output for execution. The benchmark integration target's synthetic and dry-run
contract cases run normally. Retained test diagnostics from generated
normalization workloads live under the system temporary directory. Recorded selections use the exact Cargo commands
below.

## Recorded smoke tests

Run these commands from the repository root. Keep source publications and raw
archives read-only. The conversion fixture declares its source publication and
prepared evidence. Set `MARKETFORGE_RECORDED_CONVERT_INPUT` to the absolute
location of an exact copy only if the source publication was relocated.

```sh
set -o pipefail
cargo test --manifest-path engine/Cargo.toml --features historical-real-tests \
  --test historical_recorded_convert validates_canonical_recorded_prefix_with_causal_prehistory \
  -- --exact --nocapture 2>&1 | cpout

(
set -e
for case in representative_trade representative_depth; do
  cargo test --manifest-path engine/Cargo.toml --features historical-real-tests \
    --test historical_normalize_support "$case" -- --exact --nocapture 2>&1 | cpout || exit $?
done
for case in exchanges::binance::perpetual_linear_archive_prefix trade_real_archives::normalize_real_trade_archives; do
  cargo test --manifest-path engine/Cargo.toml --features historical-real-tests \
    --test historical_normalize "$case" -- --exact --nocapture 2>&1 | cpout || exit $?
done

for case in \
  bounded_recorded::bounded_v5_trade_matches_normalized_rows_and_commits_with_one_or_two_workers \
  bounded_recorded::bounded_v5_depth_commits_real_regression \
  bounded_recorded::bounded_v5_combined_commits_real_regression \
  bounded_recorded::bounded_v5_trade_streams_from_one_publication_commit \
  bounded_recorded::adjacent_depth_parts_preserve_stream_rows_without_opening_primary_files \
  merge_trade_kway_real::merges_two_bounded_recorded_trade_streams \
  merge_unified_reference_integrity::validates_real_trade_stream \
  merge_unified_reference_integrity::validates_real_depth_stream; do
  cargo test --manifest-path engine/Cargo.toml --features historical \
    --test historical_merge "$case" -- --ignored --exact --nocapture 2>&1 | cpout || exit $?
done
)
```

The conversion smoke checks source inventory footers and committed provenance,
then reads a causal 4,096-event prefix for the timestamp regression prepass
and materialization. Internal reference observations in that prefix remain in
timeline order. Parquet readers can decode more data at page boundaries. The
test inspects the staged streams and routes and never publishes the prefix.

Recorded normalization checks use whole logical events from selected archives
through the canonical batch and profiles. Recorded merge windows retain depth
initialization and reconstruction context, including events before the window.
Merge still verifies the selected normalized files and sidecars against their
committed metadata. Set `MARKETFORGE_MERGE_FIXTURE_FROM` and
`MARKETFORGE_MERGE_FIXTURE_TO` to absolute prefix paths when relocating those
publications.

Full archives and the large parallel workload require `historical-full-tests`.
The full conversion CLI publication additionally requires an explicit exact
ignored-test invocation of `publishes_canonical_recorded_conversion` in
`historical_recorded_convert`. For compile-only coverage of these cases use:

```sh
set -o pipefail
cargo test --manifest-path engine/Cargo.toml --features historical-full-tests --no-run \
  --test historical_normalize --test historical_normalize_support --test historical_recorded_convert 2>&1 | cpout
```
