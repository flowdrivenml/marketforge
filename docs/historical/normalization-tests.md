# Normalization correctness tests

The authoritative normalization job fixture is `engine/tests/historical/fixtures/normalize.json`. The authoritative merge job fixture is `engine/tests/historical/fixtures/merge.json`. Shared test support resolves them from the crate manifest directory and uses the production loaders. Normalization subsets keep the namespace, instrument catalog, family bindings, and job metadata. Small source samples are chosen by semantic selection and stable task identity. Tests generate source records and apply scenario-specific policy overrides in memory. Temporary job files used by path-based APIs belong to the individual test run.

Merge selectors keep the original per-file identities, content types, ranks, and publication pins. Ordinary execution tests generate their own small publications. Recorded merge checks remain opt-in. The legacy v3 normalization example is a documentation wire-contract case exercised by its protocol and CLI compatibility tests.

Run source-only harness tests:

```sh
set -o pipefail
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_normalize_support 2>&1 | cpout
```

Run only the two representative, bounded real-source tests:

```sh
set -o pipefail
for case in representative_trade representative_depth; do
  cargo test --manifest-path engine/Cargo.toml --features historical-real-tests \
    --test historical_normalize_support "$case" -- --exact 2>&1 | cpout || exit $?
done
```

Each samples the source beginning through the production archive reader, then creates a complete small compressed wrapper. The depth sample retains its initialization snapshot and next event. No complete archive normalization is implied. Missing prerequisites fail with the intended path; no archive is substituted.

Prepared-batch runs retain `test.json`, selected batch, isolated scratch/staging and datasets under `${TMPDIR:-/tmp}/marketforge-historical-tests/<test>/<exclusive-run>/`. Metadata records running/passed/failed states, timestamps, original source selection, effective configuration, profile provenance and results. An interrupted process may leave `running`. No Drop cleanup deletes artifacts. Concurrent and repeated tests create distinct directories. Ordinary correctness defaults use one worker and 1 GiB memory/scratch, with small Parquet targets; specialized tests can override resources explicitly.

Exchange cases now use production profile resolution and explicit synthetic sources; retained archive-specific prerequisites remain explicit. The full workload is never selected by default. Real OKX family execution requires explicit authoritative member metadata.

Task 06 profile selection uses `historical_normalize`. Source-only tests require
`--features historical`; select an exchange with `exchanges::bybit` or a named
case such as `exchanges::bybit::perpetual_linear_depth_profile`.

Bounded recorded archive cases require `--features historical-real-tests`. A bounded current
archive oracle is `exchanges::binance::perpetual_linear_archive_prefix`.
The support target's `representative_trade` and `representative_depth` select
bounded Bybit prefixes. Prefixes are copied into complete isolated containers;
original compressed archives are never truncated or rewritten.

Retained September-specific oracles require that interval in the authoritative
prepared workload; the maintained October workload cannot satisfy those
expectations. Explicit selection fails with a prerequisite diagnostic.
`historical-full-tests` registers complete-archive correctness
cases. Listing them with `-- --list` does not execute workloads. Complete
workloads and performance benchmarks require an explicit opt-in.

Each of the 35 supported profiles has a named synthetic execution case with
fixed decimal, timestamp, identity and side expectations. These cases prove
interpretation of their supplied source records, not coverage of every real
archive. The catalog coverage report separately records fixture availability,
missing family metadata and feed gaps. Synthetic instrument identities belong
to an explicit test namespace and are never treated as real family metadata.

Stream tests are registered once in `historical_normalize_support` as `streams`
and `stream_depth`. Direct execution cases use a persistent outcome guard that
records success/failure without deleting artifacts. They check shared files,
source spans, local/global depth identities, zero-change events, recovery,
intermediate replay states, worker determinism, failures, bounded admission,
legacy readers and merge/conversion metadata. Integrity policy regression tests
cover production writer execution, including task-local Degrade behavior.

See [bounded Rust verification](../../engine/scripts/README.md) for current commands and feature gates.

The final verification report is [normalize-integration-verification.md](normalize-integration-verification.md).
