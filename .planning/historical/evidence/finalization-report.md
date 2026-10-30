# Historical finalization: database verification and defect repair

Date: 2026-10-30. Branch: `codex/historical-modernization`.
Milestone acceptance remains with the user.

## Outcome

The actual PostgreSQL catalog contains **36 raw formats and 49 normalization
mappings**, matching the expected migration inventory. All 36 profiles resolve
uniquely to 19 physical definitions. Physical schema comparisons match for every
record, and every deduplication group contains equivalent original physical
schemas/wrappers. Of 49 mappings, 47 are structurally identical and two contain
the justified Gate.io Spot correction. Profile classifications are **30 EXACT
MATCH, 1 JUSTIFIED CORRECTION, 5 UNSUPPORTED, 0 UNRESOLVED migration differences**.
31 profiles compile 42 rules for execution; the five unsupported profiles retain
seven database mappings without pretending to compile or execute them.

The public catalog, version-2 job facade, version-1 compatibility boundary,
Shared normalization engine and six grouped test targets were preserved.
This finalization added focused validation/provenance repairs and corrected test
classification; it did not replace those systems.

## Preserved architecture and public API

Historical format loading remains separate from Live. `FormatCatalog::load` reads
and validates the catalog; `get` resolves an explicit profile; `for_task` resolves
the legacy compatibility tuple; `check_task` validates instrument/wrapper
compatibility. `load_document` / `load_document_with_catalog` and the established
`load_processing_job` entrypoint resolve both wire protocols into the existing
`ProcessingJob` / `WorkTask` model. Profiles compile once per catalog, outside
record loops, and the default catalog remains cached.

Version 2 is still the intended authoring contract. A complete example remains
[`engine/fixtures/jobs/historical-profile-v2.json`](../../../engine/fixtures/jobs/historical-profile-v2.json):
a task provides `format_profile`, optional `profile_fingerprint`, input/member,
stream/task IDs, exchange and authoritative instrument metadata, without embedding
raw schemas or normalization rules. Version 1 continues through the same validated
Shared compilers and workers. Direct Rust execution APIs remain unchanged.

## Independent database evidence and method

Credentials were discovered in the existing `.secrets` environment file and read
only into process memory. PostgreSQL credentials were passed through subprocess
environment variables, never command arguments or generated files. No secret file
was modified. Connections used `default_transaction_read_only=on`, an eight-second
connect timeout and a 15-second statement timeout. Catalog capture used
`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`; all other inspections also used
explicit read-only transactions. No database write was issued.

Tables were discovered through `information_schema.columns`, rather than assumed.
The relevant tables are `catalog.raw_formats`, `catalog.normalization_rules`,
`catalog.exchanges`, `catalog.instruments` and `catalog.instrument_specs`.
[database-catalog.json](database-catalog.json) preserves every original raw-format,
mapping and exchange record, including IDs, schemas, notes and timestamps.
[database-audit.sql](database-audit.sql) records the read-only catalog query.
[database-audit-summary.json](database-audit-summary.json) records capture time,
counts and snapshot SHA-256. The capture is evidence of that point in time, not a
claim that later database edits have been audited.

Comparison used parsed JSON/TOML values. Object key ordering is irrelevant; field
arrays, positions, types, rule arrays and all nested values remain significant.
Resolution checked the complete exchange/instrument/market/format tuple, not only
format code. Compression comparison recognizes only the documented spelling alias
`tar.gz` → Rust `tar_gzip`; it does not equate different wrappers. Raw schemas match
in full, including headers, ordering annotations, granularity, instrument scope,
archive members and timestamp annotations. Physical record/container formats and
wrapper declarations also match. All 19 deduplicated groups were compared against
the original database rows, not generated seed data.

The complete per-profile identity/mapping/evidence/resolution table is in
[audit-matrix.md](audit-matrix.md) and [audit-matrix.json](audit-matrix.json).
The 19 physical groups and original row IDs are in
[physical-deduplication.json](physical-deduplication.json). A new offline Rust test
compares all 36/49 captured originals against the actual catalog and permits only
the explicit, asserted Gate.io exception.

## Gate.io Spot depth: intentional correction confirmed

Current PostgreSQL raw-format ID **20**, mapping IDs **24** (`l2_snapshot`) and
**25** (`l2_update`), maps `0 → bid`, `1 → ask`. The schema exposes a numeric `side`
column separately from action/price/amount. Established version-1 Gate.io Spot
jobs and the TOML profile map `2 → bid`, `1 → ask`.

A bounded gzip sample of
`BTC_USDT-2026090100.csv.gz` read **65,536 rows / 3,712,002 characters**, below a
4 MiB decoded limit. It contained **36,711 side-2 rows**, **28,825 side-1 rows**,
and **no side-0 rows**. The complete first snapshot was observed within the bound:
29,191 side-2 levels and 24,732 side-1 levels. Maximum side-2 price was **78,585.6**;
minimum side-1 price was **78,585.7**. This is consistent with bids below asks and
with the independent legacy job mapping. It does not depend solely on tests.

[gateio-spot-sample.json](gateio-spot-sample.json) records the observations.
Two original best-price snapshot rows were retained as a tiny regression fixture.
The production depth worker test asserts their numeric side mapping, exact prices,
non-crossed orientation, base/quote quantity relationship and zero rejected rows.
Existing synthetic production-worker equivalence tests retain snapshot and
make/take update coverage with side 2/1.

**Resolution:** retain TOML `2 → bid`, `1 → ask`; database unchanged. No evidence
of a side-0 variant was found in this sample. This does not prove every archive
uses this encoding. No heuristic accepting both encodings was introduced.
Datasets previously generated from database rules 24/25 may lack valid bids or
have failed integrity checks and should be audited/regenerated from the corrected
profile. Datasets generated from the existing 2/1 legacy jobs are unaffected by
this finalization.

## Quantity semantics and authoritative metadata

[quantity-audit.json](quantity-audit.json) records every profile, its own database
raw/mapping notes, quantity encodings and job/instrument witnesses. Read-only
instrument queries retrieved **23 authoritative job instruments**. Their quantity
types, contract values and denominations agree with every supported profile and
its existing job metadata; no economic metadata discrepancy was found.
Trade and depth profiles were reviewed separately.

| Market/archive family | Retained native quantity | Interpretation |
|---|---|---|
| Spot trades/books | Base | Quote = base × price |
| Binance linear perpetual/future trades | Base | Contract size is not substituted for base quantity |
| Binance inverse perpetual/future trades | Contracts | Quote = count × authoritative quote-denominated contract value; base = quote / price |
| Bybit linear trades/books | Base | Quote = base × price |
| Bybit inverse trades/books | Quote | Base = quote / price; do not manufacture contract counts |
| OKX linear derivatives trades/books | Contracts | Base = count × authoritative base-denominated contract value |
| OKX inverse derivatives trades/books | Contracts | Quote = count × authoritative quote-denominated contract value |
| Gate.io linear trades/books | Contracts | Signed native size supplies side; absolute size is a contract count |
| Gate.io inverse trades/books | Contracts | Signed count × quote-denominated contract value |
| Bitget linear trades/books | Base | Separate book schema reviewed; inverse book semantics not inferred from it |

Examples from the captured metadata are Binance BTC inverse contract value 100
USD, OKX BTC linear 0.01 BTC / inverse 100 USD, and Gate.io BTC linear 0.0001 BTC /
inverse 1 USD. These are **instrument witnesses**, not constants placed in TOML.
No contract values/assets were moved from instrument metadata into format rules.

[bounded-depth-observations.json](bounded-depth-observations.json) contains native
price/quantity witnesses for **nine real depth profiles**, bounded by 4 MiB and
65,536 records, plus independent expected decimal calculations under the captured
metadata. It explicitly records four incomplete observations: two inner Bitget
XLSX workbooks exceed the inspection bound, and two OKX future TARs have a different
first member from the requested member. Those inspections stopped safely. Their
synthetic production-worker equivalence coverage passed, but those four real
samples are **unverified**, not passing real-archive tests.

Real trade-prefix equivalence passed for all **18 supported trade profiles**;
tiny native-format depth equivalence passed for all **13 supported depth profiles**.
The comparison covers complete canonical values, optional fields, timestamps,
identities, quantities, depth boundaries/levels/operations/sequences, ordering,
integrity counters and malformed-input errors. It compares direct legacy jobs
against TOML-resolved jobs through the same production workers, not TOML against
itself. Decimal parsing and multiplication preserve the established exact Decimal
semantics; repeating division retains the existing finite Decimal behavior.
No quantity algorithm or quantity encoding was changed without evidence.
[Additional source review](quantity-external-sources.md) distinguishes facts from
unit interpretations not independently established by exchange depth docs.

## Unsupported profiles retained with precise reasons

| Profile | Confirmed blocker |
|---|---|
| `bitget.inverse.perpetual.bitget-b1.v1` | Book denomination not established; current database contains no Bitget inverse instrument metadata. No safe base/quote/contracts choice. |
| `bybit.option.option.bybit-t3.v1` | TradeSpec does not implement catalog instrument lookup or required IV/index/mark mappings. Canonical optional fields exist, but ingestion support is incomplete. |
| `bybit.option.option.bybit-b2.v1` | `per_member_instrument` scope needs option-chain discovery and independent per-instrument book orchestration. |
| `okx.option.option.okx-t1.v1` | TradeSpec lacks required catalog lookup, composite identity and sorting of the multi-instrument option-chain input. |
| `okx.option.option.okx-b2.v1` | `per_member_instrument` scope needs option-chain discovery and independent per-instrument book orchestration. |

The database has 1,002 Bybit options with base quantity metadata and 1,996 OKX
options with contract metadata. Those records do not implement missing processor
behavior. [unsupported-instrument-metadata.json](unsupported-instrument-metadata.json)
records the aggregate checks. TOML rejection messages were clarified; support
status and original rules remain unchanged. Version-2 negative tests exercise all
five unsupported profiles and verify explicit rejection.

## Focused processing-job and provenance defects repaired

1. **Ambiguous archive selection.** Version 2 previously chose array element zero
   when `source_compression` was absent. The loader now defaults only for a
   single supported container; multiple wrappers require explicit selection.
   Existing `check_task` rejects an unsupported explicit choice. Regression covers
   omitted multi-wrapper selection, valid explicit selection and invalid selection.
   Existing one-wrapper jobs retain their behavior.
2. **Discarded provenance pin.** Version-1 snapshot reload removed
   `marketforge_profile` before checking its ID/fingerprint. Identical executable
   rules could therefore be retagged after another fingerprinted definition edit.
   Recorded IDs and hashes are now checked before resolving both catalog-backed
   and `legacy-inline` snapshots. Original untagged version-1 jobs remain accepted
   when their configuration matches. Conflicting rules remain rejected.
3. **Wrong effective execution configuration in provenance.** Execution uses
   authoritative global `ProcessingConfig`; the snapshot previously serialized
   legacy job resource/integrity values instead. Both direct and session paths
   now write a cloned resolved job with the effective runtime resources/policy.
   `processing-job.sha256` deterministically fingerprints the actual snapshot
   bytes, including schemas/rules/profile pins, instrument data, selected wrapper,
   ordering, paths and execution settings. Production-pipeline tests run a tiny
   archive through both paths, verify atomic staging-directory publication,
   snapshot/checksum survival, exact settings/tasks, reload and manifest presence.

No canonical schema, event ordering, quantity calculation, integrity policy or
archive decoding behavior was weakened. There is no second normalization engine.
No Shared production change was needed in this finalization.

All **43 actual version-1 jobs** load against the catalog, including Binance
inverse futures. Their raw schemas, normalization configurations, source ordering,
compression and metadata match; no new semantic canonicalization was necessary.
Version-2 tests cover valid equivalence, unknown/unsupported profiles, duplicate /
ambiguous catalog resolution, wrong exchange/kind/market, missing/nonpositive
contract values or missing asset, invalid wrappers, wrong pins and conflicting
inline fields. No fallback to an unrelated profile was added.

The resolved snapshot remains adjacent to `manifest.json` and is preserved by the
existing directory commit. The strict manifest has no generic configuration-link
field; adding one would require extending its schema and downstream constructors.
This milestone therefore retains the established adjacent snapshot convention and
adds its checksum rather than redesigning the manifest. Keep the matching catalog
revision with datasets. A changed catalog pin fails reload; committed data is not
reinterpreted through a later TOML edit. Older datasets lacking effective-config
snapshots/checksums cannot retroactively gain that evidence without a separate
reconstruction; their market-data rows were not changed by these repairs.

## Test classification and preservation

The six targets remain unchanged; package integration-target count stays **43**.
The original modernization replaced 160 Historical targets with these six.

| Target | Before finalization | Final Cargo discovery |
|---|---:|---:|
| `historical_normalize` | 112 | 124 |
| `historical_merge` | 233 | 246 |
| `historical_convert` | 170 | 200 |
| `historical_slow_normalize` | 39 | 33 |
| `historical_slow_merge` | 80 | 67 |
| `historical_slow_convert` | 160 | 131 |
| Total | 794 | 801 |

49 bounded cases moved from opt-in to normal. Evidence and exact case names are in
[reclassified-tests.json](reclassified-tests.json): seven in-memory combined
alignment cases, five Arrow timeline cases, five tiny synthetic integrity cases,
one ten-record sink-failure case, one 162-row dataset metadata case, five
1,000-record-or-smaller reference-reader cases, eight bounded execution-reader
cases, seven preflight rejection cases, and ten Parquet-footer-only partition
planning cases. Classification is based on the individual workload, not filenames.
Mixed files were split without changing test bodies/assertions. Expensive/unbounded
siblings remain opt-in. No new `#[ignore]` was introduced.

Actual Cargo executable discovery (`--list`, including ignored-state discovery)
confirmed all **794 previous cases preserved**, no missing cases and no ignore-state
changes. A source-body comparison also checked 767 directly declared old test
bodies without finding assertion changes; nested cases are accounted for by the
actual Cargo identity comparison. The earlier 781-case pre-modernization coverage
remains included. [test-preservation.json](test-preservation.json) and
[cargo-test-discovery.json](cargo-test-discovery.json) provide the detailed record.

`independently_validates_converted_values` existed in a nested Rust file without
an explicit Cargo target before grouping, so was not automatically discoverable
then either. It is now registered under `historical_slow_convert`. It invokes an
independent Python/PyArrow validator over external converted datasets and remains
**unexecuted**. Python code was not modified. Its environment/path dependencies
remain requirements for a separately authorized campaign.

The 60-second threshold applies to an individual case. Known long scans,
materialization, benchmarks and cases with uncertain production-sized dependencies
remain conservatively opt-in. Prior six 30-second interruptions are not passes and
were not rerun: combined checkpoint construction, combined depth-adapter comparison,
two output-manifest tests, full output validation and real depth-event-index
validation. Existing runtime evidence was consulted; expensive cases were never
executed just to classify them.

## Verification baseline

`verification-summary.json` and `verification-results.json` retain the machine
readable results and explicitly mark opt-in cases unverified.

- `scripts/verify-historical.sh normal`: **601 PASS**, zero failures/timeouts:
  124 Historical Normalize + 246 Merge + 200 Convert + 31 relevant Shared depth cases.
  Longest individual case: approximately **11.89 seconds**.
- Focused Shared library normalization/spec run: **28 PASS**; 127 unselected cases
  are **NOT_RUN**, not claimed passing. This includes exact numeric/timestamp and
  base/quote/linear/inverse contract quantity regression coverage.
- After clarifying unsupported-profile reasons: all **15 configuration cases PASS**;
  the filtered run's 586 other discovered cases are NOT_RUN for that rerun.
- Historical-only locked/offline/no-default-feature check: **PASS**.
- Live-only library compatibility check: **PASS**; no Live integration suite run.
- Minimal-feature library check: **PASS**.
- All three normal integration targets and relevant Shared targets compile: **PASS**.
- All three slow targets compile and are discoverable: **PASS compilation only**.
- Post-import-cleanup feature checks and both target compilations: **PASS**.
- All **231 opt-in cases are unexecuted**: 226 unverified plus five existing ignored
  cases. No benchmark, slow-suite body, production-scale archive processing,
  all-features build or unrestricted Historical test command was executed.
- Six Historical Cargo registrations and feature dependencies were inspected:
  normal targets require `historical`; slow targets require `historical-slow-tests`;
  `historical` and defaults do not enable the slow feature.

Normal run evidence:
`engine/target/historical-verification/20261030T010602Z-522122/`.
Final compile evidence:
`engine/target/historical-verification/20261030T011056Z-529183/`.
Focused library evidence: `engine/target/historical-finalization-lib-run/`.
Builds were sequential against the shared Cargo target directory.
Only pre-existing conversion CLI dead-code warnings remain; no conversion
implementation was changed to silence them.

For subsequent bounded normal verification, use `scripts/verify-historical.sh normal`.
For compile-only verification, use `scripts/verify-historical.sh compile`.
Slow compilation command (do not execute bodies during this milestone):

```sh
cargo test --manifest-path engine/Cargo.toml --locked --offline \
  --no-default-features --features historical-slow-tests \
  --test historical_slow_normalize --test historical_slow_merge \
  --test historical_slow_convert --no-run
```

## Remaining limits and output impact

The five unsupported profiles remain explicit safe failures. Four bounded real
depth observations stopped at their declared limits. The opt-in suites, independent
Python validator and unselected library cases remain unverified. Exchange depth
docs do not independently establish every archive quantity denomination; incomplete
observations were not used to change economic behavior. No new quantity defect was
proven, and no existing canonical quantity calculation was altered.

Gate.io's already-migrated 2/1 correction is now independently evidenced; it was
retained, not silently introduced in this patch. Existing data produced using 0/1
database rules needs an audit/regeneration decision. Provenance changes affect
new dataset sidecars and reject stale/tampered pins; they do not rewrite existing
canonical rows. No acceptance decision is made here.

## Local commits and scope preservation

- `29eca98aaa67f407164991ffc493ad9b732792ea` — archive selection, snapshot-pin validation, effective execution provenance and production-pipeline regressions.
- `c975ec63bc321d1dc36e1c96db711480a670e3db` — per-case test classification, preserved discovery and independent-validator registration.
- The final audit/documentation commit is identified in the final response and local Git log.

The prior checkpoint `24ee5369475d6ee3ff7c127ddec4a77b212de774` remains in history.
No push, merge, reset or history overwrite was performed. Existing user changes
to `.gitignore`, Live planning files and the large untracked workspace inventory
were preserved and excluded from commits. No Live source/config/tests/planning,
Python/Forge source, Historical conversion implementation or secret was modified.

## Changed files

- `.planning/historical/evidence/finalization-report.md`
- `.planning/historical/evidence/audit-matrix.json`
- `.planning/historical/evidence/audit-matrix.md`
- `.planning/historical/evidence/bounded-depth-observations.json`
- `.planning/historical/evidence/cargo-test-discovery.json`
- `.planning/historical/evidence/database-audit-summary.json`
- `.planning/historical/evidence/database-audit.sql`
- `.planning/historical/evidence/database-catalog.json`
- `.planning/historical/evidence/database-job-instruments.json`
- `.planning/historical/evidence/database-schema.json`
- `.planning/historical/evidence/gateio-spot-sample.json`
- `.planning/historical/evidence/physical-deduplication.json`
- `.planning/historical/evidence/quantity-audit.json`
- `.planning/historical/evidence/quantity-external-sources.md`
- `.planning/historical/evidence/reclassified-tests.json`
- `.planning/historical/evidence/test-preservation.json`
- `.planning/historical/evidence/unsupported-instrument-metadata.json`
- `.planning/historical/evidence/verification-results.json`
- `.planning/historical/evidence/verification-summary.json`
- `data/.config/historical/formats/depth/bitget.inverse.perpetual.bitget-b1.v1.toml`
- `data/.config/historical/formats/depth/bybit.option.option.bybit-b2.v1.toml`
- `data/.config/historical/formats/depth/okx.option.option.okx-b2.v1.toml`
- `data/.config/historical/formats/trades/bybit.option.option.bybit-t3.v1.toml`
- `data/.config/historical/formats/trades/okx.option.option.okx-t1.v1.toml`
- `docs/historical/modernization.md`
- `docs/historical/test-inventory.json`
- `docs/historical/test-preservation.json`
- `engine/src/historical/config/job.rs`
- `engine/src/historical/normalize/executor.rs`
- `engine/src/historical/normalize/session.rs`
- `engine/tests/historical/convert/combined/combined_alignment.rs`
- `engine/tests/historical/convert/combined/combined_alignment_external.rs`
- `engine/tests/historical/convert/execution/execution_reader.rs`
- `engine/tests/historical/convert/execution/execution_reader_bounded.rs`
- `engine/tests/historical/convert/main.rs`
- `engine/tests/historical/convert/reference/reference_reader.rs`
- `engine/tests/historical/convert/reference/reference_reader_bounded.rs`
- `engine/tests/historical/convert/trades/validate_values.rs`
- `engine/tests/historical/merge/combined/merge_unified_timeline.rs`
- `engine/tests/historical/merge/combined/merge_unified_timeline_bounded.rs`
- `engine/tests/historical/merge/main.rs`
- `engine/tests/historical/merge/trades/merge_trade_stream.rs`
- `engine/tests/historical/merge/trades/merge_trade_stream_bounded.rs`
- `engine/tests/historical/normalize/configuration.rs`
- `engine/tests/historical/normalize/equivalence.rs`
- `engine/tests/historical/normalize/main.rs`
- `engine/tests/historical/normalize/resources/gateio-spot-observed-snapshot.csv`
- `engine/tests/historical/normalize/trades/trade_worker_metrics.rs`
- `engine/tests/historical/normalize/trades/trade_worker_metrics_bounded.rs`
- `engine/tests/historical/slow/convert/main.rs`
- `engine/tests/historical/slow/merge/main.rs`
- `engine/tests/historical/slow/normalize/main.rs`
