# Historical normalization configuration catalog

## Organization and identities

```text
data/.config/historical/
├── raw_schemas/
│   ├── trades/   # 11 shared physical definitions
│   └── depth/    # 8 shared physical definitions
└── profiles/
    ├── trades/   # 20 normalization profiles
    └── depth/    # 16 normalization profiles
```

The 55 available TOMLs were moved; no removed exchange configuration, dataset,
archive, job, or historical fixture was recreated. The two existing registry text
exports (`raw formats.txt`, `normalization rules.txt`) remain unchanged reference
material, not runtime input or authoritative corrections to observed sources.

Raw IDs and filenames use
`exchange.{trades|depth}.format-code[.structural-variant].vN`. Tokens are lowercase
letters/digits/hyphens; revisions are positive integers without leading zeroes.
The variant is present only when the same format code has distinct physical
metadata: OKX option trades and Bitget inverse depth. It is not a requirement to
duplicate schemas for every market. Multiple profiles retain the same `Arc` to a
shared definition. For example, Bybit spot/linear/inverse depth share one raw
schema; their quantity interpretation belongs to their respective profiles.

Raw definitions retain `kind = "physical"`, `PhysicalFormat`, `container_format`,
`record_format`, `supported_containers`, and `raw_schema`: the established engine
serialization/decoder contract. Compression/archive structure, source fields,
header/position/type metadata and observed ordering stay here. Physical content
is unchanged except for its human-readable `id`.

Profiles retain `kind = "profile"`, the `physical` foreign-reference field, and
**all** existing profile IDs:
`exchange.market_category.instrument_type.format_code.version`.
`bybit.spot.spot.bybit-t1.v1` deliberately contains both `spot` dimensions.
Format code is retained in the ID and the explicit `format_code` metadata.
Market/instrument compatibility, timestamp/quantity interpretation, side mapping,
source ordering requirements, and distinct snapshot/update rules remain profiles'
responsibility. No rules, encodings, ordering, container lists or raw metadata were
changed by the migration.

## Loading and validation

```rust
use marketforge_engine::historical::config::{FormatCatalog, load_document_with_catalog};
let catalog = FormatCatalog::load("/absolute/workspace/data/.config/historical")?;
// This resolves a job document; execution remains a separate responsibility.
let job = load_document_with_catalog("/absolute/jobs/request.json", &catalog)?;
```

`FormatCatalog::load` takes an **absolute historical root**, not the former
`formats/` tree. Both `raw_schemas/` and `profiles/` must exist. Only their sorted
TOML descendants are discovered, under `trades/` or `depth/`. Nested grouping is
allowed within those categories. Arbitrary filenames/comments/whitespace do not
define identities. Drafts, runtime settings, registry exports, jobs and sibling
TOMLs are not loaded. Symlink entries are rejected to avoid recursive cycles and
implicit out-of-tree catalog inputs.

The existing loader and shared `TradeSpec` / `DepthSpec` compilers remain the
single implementation. It validates schema versions, readable raw IDs, duplicate
IDs, profile identities and resolution uniqueness, references, raw fields/types/
positions/header behavior, unique supported containers, compatible categories,
normalization targets, transforms and required rule metadata. It additionally
rejects documents in the wrong section, mismatched trade/depth directories, and
invalid source-field references even for inventory-only unsupported profiles.
Unsupported rules are never compiled or enabled just to load the inventory.
Errors identify paths and profiles. Schema version remains **1**.

Default loading is process-cached. Configure before first use:

- `MARKETFORGE_HISTORICAL_CONFIG=/absolute/.../historical` is the preferred root.
- The existing `MARKETFORGE_HISTORICAL_FORMATS` variable remains a compatibility
  alias. An absolute value ending in `/formats` selects its parent historical
  root after migration; otherwise it denotes the historical root itself. Old
  mixed directories are **not** loaded or silently substituted.
- Setting both overrides to different roots fails. Relative roots fail rather
  than depending on the working directory.
- With no override, the absolute source-tree default is resolved from
  `CARGO_MANIFEST_DIR`, joining `../data/.config/historical`.

No PostgreSQL dependency or configuration fetch is introduced.

## Compatibility and fingerprints

The SHA-256 calculation is unchanged:
`serde_json::to_vec((&profile, &physical_definition))`, then SHA-256. Comments,
file paths and whitespace remain irrelevant; serialized IDs remain significant.
All 19 raw IDs and all 36 `physical` references change, so affected profile
fingerprints change even though canonical transformations remain identical.
Cryptographic fingerprints are provenance, not human-facing schema names.

Profile IDs and wire protocol versions are unchanged. Regenerate pinned v2
`profile_fingerprint` values and resolved v1 `raw_schema.marketforge_profile`
provenance from the new catalog. Stale fingerprints/identities continue to fail;
no bypass, alias fingerprint, or silent acceptance was added. Unpinned v2 jobs
still resolve by their unchanged profile ID. Legacy jobs without pinned provenance
retain the existing schema/rule compatibility checks. Published datasets, their
metadata and existing generated job files were **not** rewritten.

Future Python work must update physical-template discovery from
`historical/formats/{trades,depth}` to `historical/raw_schemas/{trades,depth}` and
profile discovery to `historical/profiles/{trades,depth}`, and emit new catalog
fingerprints. In particular the external-import draft helper's old template path
needs that separate change. No `forge/` code was touched in this task.
Runtime ownership has since moved to `normalize.toml`, with Protocol v3 job
requests. See [runtime configuration and orchestration contract](normalization-runtime.md).
Profile semantics and fingerprints remain independent of runtime settings.

## Raw-schema migration ledger

All previous files were `formats/<type>/physical-<hash>.toml`. New filenames are
the new ID plus `.toml`, under `raw_schemas/<type>/`. Profile files moved unchanged
in name from `formats/<type>/` to `profiles/<type>/`.

| Old raw ID | New raw ID | Referencing profiles |
| --- | --- | ---: |
| `trades-fb8a8cd3be764571` | `binance.trades.binance-t1.v1` | 1 |
| `trades-8e67686feb09e160` | `binance.trades.binance-t2.v1` | 2 |
| `trades-bfe8e251a21dcfc4` | `binance.trades.binance-t3.v1` | 2 |
| `depth-b056fca0cb1f113d` | `bitget.depth.bitget-b1.inverse.v1` | 1 |
| `depth-27f3e7c2cf493bf6` | `bitget.depth.bitget-b1.v1` | 2 |
| `trades-27795f74ebf55560` | `bitget.trades.bitget-t1.v1` | 2 |
| `depth-ffc80404cf5abb13` | `bybit.depth.bybit-b1.v1` | 3 |
| `depth-0da18a2459405ace` | `bybit.depth.bybit-b2.v1` | 1 |
| `trades-8f03e8ad9adedc8e` | `bybit.trades.bybit-t1.v1` | 1 |
| `trades-38cb71e2f2d3b0c3` | `bybit.trades.bybit-t2.v1` | 2 |
| `trades-d2a778e788ff4fda` | `bybit.trades.bybit-t3.v1` | 1 |
| `depth-fc599eca882889a6` | `gateio.depth.gate-b1.v1` | 1 |
| `depth-47e7da4aee7e68ad` | `gateio.depth.gate-b2.v1` | 2 |
| `trades-72767b3278c7a2cf` | `gateio.trades.gate-t1.v1` | 1 |
| `trades-d6e392b67b0dc5e3` | `gateio.trades.gate-t2.v1` | 2 |
| `depth-34de574f546f04eb` | `okx.depth.okx-b1.v1` | 5 |
| `depth-8f9b705d40e16cca` | `okx.depth.okx-b2.v1` | 1 |
| `trades-9757d1280c728b84` | `okx.trades.okx-t1.option.v1` | 1 |
| `trades-f76fece6f379b1f9` | `okx.trades.okx-t1.v1` | 5 |

## Configuration audit

Every available raw schema and profile was parsed and inspected. All 36 profiles
resolve; 31 supported profiles compile 42 rules; five unsupported profiles retain
seven inventory-only rules. No invalid reference, duplicate identity, missing
supported encoding, incompatible declared source field, or unsupported transform
in a supported profile was found. A structural comparison of all 55 documents
before/after confirmed only raw IDs and profile references changed. This checks
configuration consistency, not upstream data correctness or execution results.

A separate read-only comparison parsed the currently retained PostgreSQL text
exports directly, without connecting to PostgreSQL: all **36 exported raw-format
identities** match their shared TOML raw-schema content, physical/record format
and wrapper exactly; **47 of 49 normalization rules** match exactly. The only
differences are Gate Spot L2 side maps in rules 24/25, explained below. No missing
JSON export was reconstructed and no export content was changed.

The layout/identity defects corrected are the mixed catalog tree, opaque raw IDs,
and the loader's lack of section/type validation. Structural source references
are now checked for unsupported profiles too. No new semantic correction was
justified by this audit, so no trade/depth rule was changed.

### Gate Spot L2 discrepancy

The retained `normalization rules.txt` export (records 24 and 25) maps `0=bid`,
`1=ask`. The current profile instead maps `1=ask`, `2=bid`.
[Existing source inspection](../tofixlater/raw-formats/GATE-B1%20%E2%80%94%20Spot%20L2.md)
explicitly documents source sides 1 and 2, the corresponding ask/bid meanings,
and representative rows from `BTC_USDT-2026090100.csv.gz`, including a `2,set`
price of `0.01`. The existing modernization report also records that correction.
The older TODO only establishes observed values 1/2 and calls for verification;
the registry export alone cannot override the more specific source inspection.

**Retain `1=ask`, `2=bid` in both snapshot and update rules.** The focused test
parses both compiled depth specifications and asserts that exact map, including
the absence of a `0` mapping. It does not process rows or reconstruct a book.
The underlying archive/evidence fixtures are no longer in the checkout. Independent
publisher verification now confirms the map: [Gate's historical-data announcement](https://www.gate.com/uk/announcements/article/21688/fiat%20500e%20rose%20gold)
specifies spot order-book sides as 1=sell and 2=buy, with `set`, `make`, and `take`
examples. The scalar parser applies the configured map without rebasing numeric
sides. Both rules retain 1=ask and 2=bid; no profile availability changes. This
establishes the documented archive encoding, not runtime normalization correctness.

### Unsupported functionality retained

- `bybit.option.option.bybit-t3.v1`: option instrument lookup/IV/index/mark fields.
- `bybit.option.option.bybit-b2.v1`: per-member option-chain instrument/book scope.
- `okx.option.option.okx-t1.v1`: composite instrument/trade identity and chain sorting.
- `okx.option.option.okx-b2.v1`: per-member option-chain instrument/book scope.
- `bitget.inverse.perpetual.bitget-b1.v1`: unestablished inverse quantity semantics.

These still load for inspection with their original reasons and fail execution
profile resolution. No encodings or missing semantics are guessed.

### Per-profile consistency ledger

Each row retains its existing metadata identity. Encodings are listed as
`timestamp / quantity`; `—` means intentionally omitted on an unsupported profile.
The referenced raw schema's namespace carries the data type; wrapper and physical
layout are audited separately in the raw ledger above. This is not a statement
that every historical upstream revision matches these profiles.

| Profile ID | Raw schema | Encodings | Status |
| --- | --- | --- | --- |
| `bitget.inverse.perpetual.bitget-b1.v1` | `bitget.depth.bitget-b1.inverse.v1` | — / — | unsupported |
| `bitget.linear.perpetual.bitget-b1.v1` | `bitget.depth.bitget-b1.v1` | seconds / base | validated |
| `bitget.spot.spot.bitget-b1.v1` | `bitget.depth.bitget-b1.v1` | seconds / base | validated |
| `bybit.inverse.perpetual.bybit-b1.v1` | `bybit.depth.bybit-b1.v1` | milliseconds / quote | validated |
| `bybit.linear.perpetual.bybit-b1.v1` | `bybit.depth.bybit-b1.v1` | milliseconds / base | validated |
| `bybit.option.option.bybit-b2.v1` | `bybit.depth.bybit-b2.v1` | — / — | unsupported |
| `bybit.spot.spot.bybit-b1.v1` | `bybit.depth.bybit-b1.v1` | milliseconds / base | validated |
| `gateio.inverse.perpetual.gate-b2.v1` | `gateio.depth.gate-b2.v1` | seconds / contracts | validated |
| `gateio.linear.perpetual.gate-b2.v1` | `gateio.depth.gate-b2.v1` | seconds / contracts | validated |
| `gateio.spot.spot.gate-b1.v1` | `gateio.depth.gate-b1.v1` | seconds / base | validated |
| `okx.inverse.future.okx-b1.v1` | `okx.depth.okx-b1.v1` | milliseconds / contracts | validated |
| `okx.inverse.perpetual.okx-b1.v1` | `okx.depth.okx-b1.v1` | milliseconds / contracts | validated |
| `okx.linear.future.okx-b1.v1` | `okx.depth.okx-b1.v1` | milliseconds / contracts | validated |
| `okx.linear.perpetual.okx-b1.v1` | `okx.depth.okx-b1.v1` | milliseconds / contracts | validated |
| `okx.option.option.okx-b2.v1` | `okx.depth.okx-b2.v1` | — / — | unsupported |
| `okx.spot.spot.okx-b1.v1` | `okx.depth.okx-b1.v1` | milliseconds / base | validated |
| `binance.inverse.future.binance-t3.v1` | `binance.trades.binance-t3.v1` | milliseconds / contracts | validated |
| `binance.inverse.perpetual.binance-t3.v1` | `binance.trades.binance-t3.v1` | milliseconds / contracts | validated |
| `binance.linear.future.binance-t2.v1` | `binance.trades.binance-t2.v1` | milliseconds / base | validated |
| `binance.linear.perpetual.binance-t2.v1` | `binance.trades.binance-t2.v1` | milliseconds / base | validated |
| `binance.spot.spot.binance-t1.v1` | `binance.trades.binance-t1.v1` | microseconds / base | validated |
| `bitget.linear.perpetual.bitget-t1.v1` | `bitget.trades.bitget-t1.v1` | milliseconds / base | validated |
| `bitget.spot.spot.bitget-t1.v1` | `bitget.trades.bitget-t1.v1` | milliseconds / base | validated |
| `bybit.inverse.perpetual.bybit-t2.v1` | `bybit.trades.bybit-t2.v1` | seconds / quote | validated |
| `bybit.linear.perpetual.bybit-t2.v1` | `bybit.trades.bybit-t2.v1` | seconds / base | validated |
| `bybit.option.option.bybit-t3.v1` | `bybit.trades.bybit-t3.v1` | — / — | unsupported |
| `bybit.spot.spot.bybit-t1.v1` | `bybit.trades.bybit-t1.v1` | milliseconds / base | validated |
| `gateio.inverse.perpetual.gate-t2.v1` | `gateio.trades.gate-t2.v1` | seconds / contracts | validated |
| `gateio.linear.perpetual.gate-t2.v1` | `gateio.trades.gate-t2.v1` | seconds / contracts | validated |
| `gateio.spot.spot.gate-t1.v1` | `gateio.trades.gate-t1.v1` | seconds / base | validated |
| `okx.inverse.future.okx-t1.v1` | `okx.trades.okx-t1.v1` | milliseconds / contracts | validated |
| `okx.inverse.perpetual.okx-t1.v1` | `okx.trades.okx-t1.v1` | milliseconds / contracts | validated |
| `okx.linear.future.okx-t1.v1` | `okx.trades.okx-t1.v1` | milliseconds / contracts | validated |
| `okx.linear.perpetual.okx-t1.v1` | `okx.trades.okx-t1.v1` | milliseconds / contracts | validated |
| `okx.option.option.okx-t1.v1` | `okx.trades.okx-t1.option.v1` | — / — | unsupported |
| `okx.spot.spot.okx-t1.v1` | `okx.trades.okx-t1.v1` | milliseconds / base | validated |

## Verification boundary

Only inspected configuration tests are executed. The selected library test
module creates small temporary TOML catalogs and checks parsing, reference
resolution, shared definitions, identity/metadata validation, wrong sections,
versions, source fields, duplicates, container/header/position validation,
deterministic fingerprints, stale provenance and unsupported handling. Two tests
read only the currently available configuration catalog; they never open inputs.
No test depends on a removed job, registry JSON export, downloaded archive, or DB.

The old grouped `historical_normalize` test target is deliberately not executed.
Its legacy job, archive, and database-export fixture requirements were not
restored. Its directly related catalog helpers were updated to the new layout,
but the missing-fixture historical tests remain outside this verification.

Selected commands:

```sh
cargo test --manifest-path engine/Cargo.toml --features "historical,monitor-tui" \
  --lib historical::config:: -- --nocapture
cargo check --manifest-path engine/Cargo.toml --features "historical,monitor-tui"
cargo build --manifest-path engine/Cargo.toml --release \
  --features "historical,monitor-tui" --bin marketforge-normalize
cargo fmt --manifest-path engine/Cargo.toml --check
```

No normalization binary, historical suite, pipeline test, benchmark, PostgreSQL
operation, network dataset download, or real dataset processing is run. Build
success is not end-to-end normalization verification.

### Recorded results

- Selected configuration-only library tests: **20 passed**, 0 failed, 0 ignored;
  167 unrelated library tests filtered out. No grouped Historical test binary ran.
- Requested `cargo check --features "historical,monitor-tui"`: passed.
- Requested release build of `marketforge-normalize` with the same features: passed.
- `rustfmt --edition 2024 --check` on the four changed/new Rust files: passed.
- Repository-wide `cargo fmt --check`: failed on existing unrelated Live/client/
  other formatting differences and the absent `src/bin/marketforge-tui.rs` target.
  Those files/targets were left untouched.
- Scope-local `git diff --check`: passed.

Exact selected tests (all under `historical::config::`):

- `catalog::tests::valid_documents_resolve_in_separate_trees_preserving_spot_spot`
- `catalog::tests::shared_raw_schema_is_not_duplicated_for_different_markets`
- `catalog::tests::duplicate_raw_and_profile_ids_are_rejected`
- `catalog::tests::unresolved_raw_reference_is_rejected`
- `catalog::tests::unsupported_document_versions_fail_with_path`
- `catalog::tests::profile_identity_keeps_all_metadata_dimensions_and_format_code`
- `catalog::tests::raw_identity_is_descriptive_and_versioned`
- `catalog::tests::wrong_document_section_or_data_type_is_rejected`
- `catalog::tests::missing_tree_is_not_replaced_with_legacy_or_working_directory_fallback`
- `catalog::tests::source_field_errors_and_unsupported_rules_fail_closed`
- `catalog::tests::raw_field_and_container_validation_is_preserved`
- `catalog::tests::fingerprint_ignores_locations_and_comments_but_includes_raw_identity`
- `catalog::tests::unsupported_profiles_remain_inventory_only_with_valid_references`
- `catalog::tests::root_resolution_is_explicit_absolute_and_does_not_depend_on_cwd`
- `catalog::tests::available_catalog_compiles_without_opening_archives`
- `catalog::tests::gate_spot_side_rule_matches_retained_source_inspection_not_stale_export`
- `catalog::tests::pinned_job_documents_reject_stale_fingerprints_without_opening_inputs`
- `catalog::tests::inline_depth_catalog_resolves_snapshot_and_update_rules`
- `catalog::tests::supported_profiles_require_encodings_ordering_and_matching_targets`
- `job::catalog_fingerprint_tests::resolved_profile_provenance_rejects_stale_id_or_fingerprint`

### File changes

- Moved 19 raw TOMLs to `data/.config/historical/raw_schemas/`, with descriptive
  filenames/IDs as listed in the migration ledger.
- Moved 36 profile TOMLs to `data/.config/historical/profiles/`, retaining their
  filenames/IDs and updating only `physical` references.
- Removed the now-empty mixed `formats/{trades,depth}` directories; no independent
  configuration definition was deleted.
- Modified `engine/src/historical/config/catalog.rs` for root/tree discovery and
  validation. Added `engine/src/historical/config/catalog/tests.rs`.
- Modified `engine/src/historical/config/job.rs` only to add a pure provenance test;
  executable fingerprint checking/resolution logic is unchanged.
- Updated `engine/tests/historical/normalize/configuration.rs` catalog-copy helpers
  for the new paths; its grouped target was not run.
- Added this guide; annotated the older `docs/tofixlater/historical/modernization.md`
  as a report of the previous snapshot.
- Narrowly updated `.gitignore` so the moved catalog TOMLs are trackable while
  raw data, drafts, jobs, configuration outside these trees, and secrets stay ignored.

Unrelated checkout work was left untouched.


## Reproducible export reconciliation

Run from any working directory (the script resolves repository paths itself):

```sh
python scripts/audit_historical_catalog.py \
  --report docs/historical/catalog-reconciliation.json \
  --provenance docs/historical/catalog-provenance.json
python -m unittest discover -s scripts -p test_audit_historical_catalog.py -v
```

The JSON report records source/catalog SHA-256 digests, declared identities,
counts, all field differences, explicit exceptions, unsupported reasons, and
many-to-one schema membership. It returns nonzero for unapproved differences or
invalid input. The only exceptions are the exact original/current Gate Spot maps
in records 24/25; unrelated changes in either rule still fail. Object ordering is
ignored; missing keys, nulls, types, array ordering, and additional metadata are
significant. Duplicate JSON keys are rejected.

The export lacks an exchange-name table. Numeric exchange IDs are resolved by
joining each exported format code to the explicit Exchange/Format ID Identity
tables in retained `docs/tofixlater/raw-formats/*.md`, then requiring all formats
for each numeric exchange ID to agree. SQL insertion order and filenames alone
are not identity evidence. The resulting ID/code association is in the report.

`catalog-provenance.json` preserves all 36 original raw records and all 49 original
normalization-rule records, with notes and source timestamps, linked to profile,
shared schema, format code, and rule target. No note is merged across markets.
No source metadata required interpretation to preserve; ambiguous or illustrative
quantity statements remain verbatim documentation, never contract-value constants.
The engine does not load these documents or the exports.

The catalog additionally rejects profile/raw exchange and format-code conflicts,
incorrect exchange/data-type format-code prefixes, epoch timestamp encodings that
contradict declared units/representations, spot contract quantities, source-ordered
archives declaring sorting required, and update rules on standalone snapshots.
Task checks now also reject a format-code mismatch. Existing validation handles
references, supported containers, target boundaries, unsupported declarations,
market/instrument combinations, and authoritative positive contract value/asset
requirements. Missing external facts are not proved by these checks. Unsupported
rules remain inventory-only and cannot be selected for execution.

No executable TOML schema or fingerprint algorithm changed in this reconciliation.
Fingerprints hash parsed profile and physical definitions, including IDs; comments,
file paths, and JSON object-key order do not affect them. Changing either definition,
including a physical ID/reference, encodings, rules, or availability declarations,
invalidates pinned v2 fingerprints and tagged resolved v1 provenance. Old hashes
are rejected, not rewritten. Unpinned documents do not assert a previous hash.
The legacy descriptive-ID mapping above remains diagnostic documentation, not a
runtime alias. Published manifests and historical provenance were not modified.

Verification for this reconciliation: four Python audit tests; 21 focused catalog
unit tests (including parsing, references, semantic contradictions, fingerprints,
Gate maps, and stale v1/v2 job provenance); one focused job provenance test; the
requested `cargo check` and release normalization binary build all passed. Targeted
`rustfmt --check` passed. Repository-wide `cargo fmt --check` is blocked by the
existing missing `engine/src/bin/marketforge-tui.rs` target and unrelated formatting.
No missing file was restored. The old database-snapshot integration test references
the deliberately removed `database-catalog.json`; it was not run or restored.
No end-to-end tests, pipelines, real archives, benchmarks, databases, or published
dataset directories were used. These results do not verify runtime normalization.

Remaining scope: the five unsupported profiles listed above remain unsupported;
no unapproved export discrepancy remains. External correctness questions beyond
document consistency remain outside this audit. Python orchestration and new inverse/option support are deferred. The subsequent
runtime configuration migration is documented in [normalization-runtime.md](normalization-runtime.md).
