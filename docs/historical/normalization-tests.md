# Normalization correctness tests

The maintained archive workload is `engine/tests/historical/fixtures/normalize.json`. Shared test support selects by classified identity, source feed and exact filename and rejects missing/ambiguous selections. It serializes valid subsets through the production schema-4 loader and profile resolver. Runtime overrides are isolated in memory; policies remain unchanged.

Run source-only harness tests:

```sh
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_normalize_support
```

Run only the two representative, bounded real-source tests:

```sh
cargo test --manifest-path engine/Cargo.toml --features historical-real-tests --test historical_normalize_support representative
```

Each samples the source beginning through the production archive reader, then creates a complete small compressed wrapper. The depth sample retains its initialization snapshot and next event. No complete archive normalization is implied. Missing prerequisites fail with the intended path; no archive is substituted.

Prepared-batch runs retain `test.json`, selected batch, isolated scratch/staging and datasets under `data/tests/historical/normalize/<test>/<exclusive-run>/`. Metadata records running/passed/failed states, timestamps, original source selection, effective configuration, profile provenance and results. An interrupted process may leave `running`. No Drop cleanup deletes artifacts. Concurrent and repeated tests create distinct directories. Ordinary correctness defaults use one worker and 1 GiB memory/scratch, with small Parquet targets; specialized tests can override resources explicitly.

Exchange cases now use production profile resolution and explicit synthetic sources; retained archive-specific prerequisites remain explicit. The full workload is never selected by default. Real OKX family execution requires explicit authoritative member metadata.

Task 06 profile selection uses `historical_normalize`. Source-only tests require
`--features historical`; select an exchange with `exchanges::bybit` or a named
case such as `exchanges::bybit::perpetual_linear_depth_profile`.

Archive cases require `--features historical-real-tests`. A bounded current
archive oracle is `exchanges::binance::perpetual_linear_archive_prefix`.
The support target's `representative_trade` and `representative_depth` select
bounded Bybit prefixes. Prefixes are copied into complete isolated containers;
original compressed archives are never truncated or rewritten.

Retained September-specific oracles require that interval in the authoritative
prepared workload; the maintained October workload cannot satisfy those
expectations. Explicit selection fails with a prerequisite diagnostic.
`historical-full-tests` additionally registers complete-archive correctness
cases. Listing them with `-- --list` does not execute workloads. Complete
workloads were not run during this refactor. The obsolete
`historical_slow_normalize` target has been removed. Merge/convert slow targets
are unchanged. No performance benchmarks are authorized.

Each of the 33 supported profiles has a named synthetic execution case with
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

The final verification report is [normalize-integration-verification.md](normalize-integration-verification.md).
