# Historical format profiles and bounded verification

Historical archive formats live exclusively in `data/.config/historical/formats/`.
The Historical-only Cargo feature enables TOML; it does not enable Live or require
Live initialization. No Historical loader reads Live configuration.

## Inventory and identity

The catalog contains **36 profiles**, preserving **49 normalization-rule records**,
and **19 physical definitions** shared by profiles with identical physical schemas.
See [the profile inventory](profile-inventory.json). Profile identity is explicit:

```
<exchange>.<market category>.<instrument type>.<format code lowercase>.v1
bybit.spot.spot.bybit-t1.v1
bybit.linear.perpetual.bybit-t2.v1
bybit.inverse.perpetual.bybit-t2.v1
okx.linear.future.okx-t1.v1
```

Format code alone is insufficient. Resolution also checks exchange, instrument
kind, and market category inferred from the authoritative instrument's contract
kind. It rejects ambiguity rather than selecting the first matching file.
The schema version and profile revision are validated explicitly.

Files are grouped into `trades/` and `depth/`. A `kind = "physical"` file declares
physical CSV/JSONL/XLSX representation, row/event representation, permitted source
containers, header behavior, raw field names/types/positions, and source schema
metadata. A `kind = "profile"` file references that physical identity and declares
compatibility, source ordering and an ordered `[[rules]]` list. Snapshot and update
rules remain distinct (`l2_snapshot`, `l2_update`); trade rules use `trade`.
Timestamp and quantity encodings are explicit on every supported rule. Rules
reuse the existing Shared trade/depth specifications and transformations.

Raw schema metadata retains the established Shared JSON schema contract inside
TOML tables. Its fields, supported scalar types, uniqueness and positional layout
are validated, and normalization source fields must exist in that schema.
Instrument-specific contract values, value assets, tick sizes, IDs, and symbols
are absent from format definitions: they remain authoritative job instrument
metadata. Contract quantities require positive contract values and a value asset.

`FormatCatalog::load` reads sorted files and validates/compiles supported rules
before processing. Errors identify profile and file. Duplicate definitions,
unsupported versions, malformed identities, unknown fields/transforms, missing
sources, invalid targets and incompatible instruments fail closed. A catalog is
immutable. `load_document_with_catalog` permits an explicit snapshot per processing
context. The normal loader caches one default catalog for its process lifetime;
`MARKETFORGE_HISTORICAL_FORMATS` may select that root before first loading. The
source-tree default is `data/.config/historical/formats`. There is no per-record
filesystem lookup. Actual archive headers and resolved column indices are bound
once when each archive processor is constructed.

## Supported and unresolved definitions

**31 profiles / 42 rules compile and are executable**. Every existing one of the
43 generated process jobs agrees with those definitions. Existing jobs cover
spot, linear/inverse perpetual, and supported linear/inverse futures across Bybit,
Binance, OKX, Bitget and Gate.io, for applicable trades and depth capabilities.
There is no invented Binance depth archive or unavailable Bitget inverse trade.

Five profiles / seven rules remain deliberately **unsupported**:

- Bybit option trades and option depth.
- OKX option trades and option depth.
- Bitget inverse perpetual depth.

Options processing is deferred by the existing Historical product documentation.
The records include multi-instrument archive and optional-field semantics that the
current processors cannot correctly implement as single-instrument tasks. Bitget
inverse depth's raw inspection explicitly says quantity conversion semantics are
not established. Its encoding is not guessed from the linear format. All five
records remain discoverable with a reason; attempting to resolve them for execution
returns an error before opening an archive. Their rules are retained for inventory
and are explicitly not counted as successfully compiled normalization rules.

Standalone PostgreSQL export files were not present in the checkout during this
work. The unchanged catalog seed definitions independently contain exactly the
requested 36 raw-format and 49 normalization-rule records. Those, the 43 established
job definitions, raw-format inspection documents and fixtures supplied the recovery
inventory. An external export comparison remains unverified; no database access or
Python modification was used to manufacture it.

## Lightweight wire protocol and legacy compatibility

The intended new wire format has `protocol_version = 2`. A task supplies
`format_profile`, runtime archive path/member, task/stream IDs, exchange, symbol,
instrument ID and authoritative instrument metadata. It omits `format_code`,
`raw_schema`, `normalizations`, and `source_ordering`. Compression defaults to the
profile's container; an optional task `source_compression` can select only a
container explicitly permitted by that profile. Current migrated profiles each
permit their established wrapper. Source ordering is stable normalization behavior
and cannot be overridden. Job-level canonical ordering, integrity policy, resources,
streams and output paths retain their contracts.

A complete parser-valid example is
[`engine/fixtures/jobs/historical-profile-v2.json`](../../engine/fixtures/jobs/historical-profile-v2.json).
Its abbreviated task is:

```json
{
  "task_id": 0,
  "stream_id": "bybit:spot:spot:BTCUSDT:trade",
  "input_path": "/tmp/marketforge-example/input.csv.gz",
  "format_profile": "bybit.spot.spot.bybit-t1.v1",
  "exchange": "bybit",
  "instrument_id": 1,
  "symbol": "BTCUSDT",
  "instrument": {
    "instrument_kind": "spot",
    "contract_kind": null,
    "tick_size": "0.1",
    "contract_value": null,
    "contract_value_asset": null
  },
  "archive_member": null
}
```

An optional `profile_fingerprint` pins the expected SHA-256 identity and rejects a
changed definition. `FormatCatalog::get(id).fingerprint` supplies that value.

Version 1 legacy JSON remains executable without changing Python. Known format
identities must agree with TOML raw schema, normalization rules, source ordering,
and allowed container. Conflicts are errors; neither side silently wins. JSON null
annotation entries in raw schemas are equivalent to absent annotations in TOML;
executable fields and rule values are never defaulted this way. Unknown custom v1
format codes retain transitional inline support through the **same Shared
compilers**, with a `legacy-inline` provenance identity. A known format code with
incompatible instrument metadata cannot escape validation through that path.
Mixing v2 task fields with embedded legacy fields is rejected by strict serde.

Wire v2 resolves into the existing `ProcessingJob`/`WorkTask` **v1 execution and
snapshot model**. That internal compatibility type still contains resolved fields;
it is not the new authoring contract. Its serialized form remains a reloadable v1
snapshot, rather than pretending an expanded task is a lightweight v2 request.
No second normalization engine, alternate worker implementation or Python producer
was introduced. Direct existing Rust callers constructing resolved tasks keep their
API; profile validation is the job-document loading boundary.

## Provenance and reproducibility

Each resolved task carries a namespaced `raw_schema.marketforge_profile` annotation
with profile ID and SHA-256 fingerprint. SHA-256 covers the parsed profile plus
physical definition, excludes file location and formatting, and retains rule order.
Changing financial rules, field mappings, encodings or ordering changes identity.

Processing writes the complete resolved `processing-job.json` snapshot into its
fresh staging directory. That file contains exact schemas/rules and instrument
metadata, and commits with the existing dataset-directory publication operation.
It lives beside `manifest.json`; canonical schemas and the existing manifest
schema are unchanged. It is not a new persistence service or market-data log.
Reloading a snapshot validates it against the same profile catalog; retain that
catalog revision with datasets. A later conflicting catalog fails instead of
silently changing replay semantics.

## Shared normalization and explicit defect corrections

All processing still uses Shared canonical types, exact Decimal quantities,
`TradeProcessor`, `DepthProcessor`, field extraction, book logic and source readers.
Historical archive scheduling/publication remain Historical. Shared has no new
Historical dependency. The older private CSV processor is compiled only as a test
witness: production uses the existing canonical trade processor. Its existing unit
tests remain preserved.

Corrections are explicit rather than hidden inside migration equivalence:

- Decimal inputs that exceeded Decimal precision were previously accepted with
  rounding. They are now rejected by exact parsing. Existing representable
  scientific notation such as `7.98e-05` remains supported without rounding.
- Fractional seconds with subnanosecond precision previously truncated silently.
  They now fail. Exactly representable nanoseconds, including scaled Decimal and
  scientific forms, remain valid.
- Depth rules previously ignored unknown fields and delayed unsupported transform
  errors. The Shared compiler now rejects those at configuration load, including
  incomplete bid/ask arrays, unsupported scalar/decode/order mappings, and invalid
  sequence declarations. Valid migrated rules retain the same output.
- A direct merge resource estimate for zero active streams returned a made-up
  reservation despite its established rejecting test. It now rejects; the separate
  explicit empty-partition policy remains intact. No canonical output changes.
- Stale manifest/failure-report test inputs violated current counter accounting.
  Their fixture counters were corrected; validation and assertions were retained.
- Conversion job parser tests assumed a one-reference fixture while mutable
  generated jobs had grown to three. Those tests now use checked-in, controlled,
  production-shaped parser fixtures with their original assertions. Conversion
  implementation and arithmetic were not refactored.
- The Shared depth-spec inventory test omitted four current future-depth jobs.
  Its exact inventory assertions now cover 19 jobs, 264 tasks and 519 rules.

Existing datasets containing silently rounded overprecision input or truncated
subnanosecond timestamps may require investigation/regeneration. The correction
rejects such input; it does not guess replacement economic data. Representative
valid archive inputs retain exact legacy-versus-TOML canonical results.

## Test organization and safety

Historical's **160 individual integration targets** became three normal groups:

```
historical_normalize   # trades/depth, execution, parquet, resources, config/equivalence
historical_merge       # core/trades/depth, partition, execution, materialize
historical_convert     # existing planning/reference/arithmetic/output tests
```

Three opt-in groups are rooted at `engine/tests/historical/slow/{normalize,merge,convert}/main.rs`:
`historical_slow_normalize`, `historical_slow_merge`, `historical_slow_convert`.
Their `required-features` is `historical-slow-tests`, which enables Historical but
is not enabled by Historical or defaults. Existing test files stay in responsibility
directories; explicit grouped module registration provides isolation without
copying helpers or replacing test bodies. Shared targets remain in Shared.
The complete map and reasons are in [test-inventory.json](test-inventory.json).

All **781 original Historical integration cases** are preserved, with the same
ignore states, under `<old target>::<old case>` names. Thirteen new cases cover
configuration/equivalence. See [test-preservation.json](test-preservation.json).
The package's declared integration target count fell from **197 to 43**, including
unchanged Shared and Live targets. Live test files were not modified.

A case known or reasonably expected to exceed **60 seconds individually** belongs
to the opt-in category. Uncertain full-dataset scans, expensive equivalence,
materialization, benchmark workloads and prior timeouts are conservatively isolated.
This does not claim every opt-in case exceeds 60 seconds. Known short real-data
cases remain normal, including an approximately 12-second depth merge and small
OKX processing fixtures. Sampling is bounded and does not process full archives.

During investigation, six previously uncertain cases reached the **30-second**
per-case guard and were stopped: combined checkpoint construction, a combined depth
adapter comparison, two output-manifest cases, full converted-output validation,
and depth-event-index validation. Runs were stopped to reclassify related external
workloads. Those outcomes are incomplete, not passes or proven production defects.
No known expensive case was retried after classification, no opt-in target body was
executed, and no benchmark was executed. Their final opt-in bodies remain unverified.

## Commands

The supported bounded entrypoint is:

```sh
scripts/verify-historical.sh normal
scripts/verify-historical.sh compile
```

It performs Historical-only, Live-only library compatibility, and minimal checks;
compiles normal groups and relevant Shared targets; and compiles slow groups with
`--no-run`. Normal mode uses the existing `engine/scripts/bounded_tests.py` to run
each selected case sequentially in a separate process with a 30-second monotonic
deadline and process-group cleanup. Logs are under
`engine/target/historical-verification/<UTC timestamp>-<pid>/`.
No Live integration suite runs.

Normal target compilation, from the repository root:

```sh
cargo test --manifest-path engine/Cargo.toml --locked --offline \
  --no-default-features --features historical \
  --test historical_normalize --test historical_merge --test historical_convert \
  --no-run --message-format=json > engine/target/normal-artifacts.jsonl
```

Then, from `engine/`, use the bounded supervisor rather than an unrestricted
Historical test command:

```sh
python3 scripts/bounded_tests.py target/normal-artifacts.jsonl target/normal-run \
  --run --targets 'historical_(normalize|merge|convert)'
```

Slow target compilation (permitted in this milestone):

```sh
cargo test --manifest-path engine/Cargo.toml --locked --offline \
  --no-default-features --features historical-slow-tests \
  --test historical_slow_normalize --test historical_slow_merge \
  --test historical_slow_convert --no-run
```

A future explicitly authorized slow campaign can omit `--no-run` for a chosen
slow target and exact test filter, under its own resource/deadline policy. That is
**not authorized or executed by this milestone**. Never use `--all-features` or run
benchmark binaries as ordinary verification. A timeout or unexecuted case must
remain reported as unverified.
