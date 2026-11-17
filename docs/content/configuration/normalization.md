
# MarketForge — Normalization Parameters and Variations

**Document:** `normalization-parameters-and-variations.md`  
**Scope:** Historical data normalization engine  
**Source:** MarketForge Rust code snapshot, November 17, 2026<br>
**Purpose:** Reference for configuration, testing, debugging, performance tuning, and data-integrity investigations.

---

## 1. Overview

MarketForge's historical normalization engine converts exchange-native historical data into canonical Parquet datasets.

The engine supports multiple exchanges, instrument types, market categories, input formats, integrity policies, and resource configurations.

Normalization consists of several distinct operations:

1. Resolving prepared jobs and source metadata.
2. Grouping compatible tasks into logical streams.
3. Decoding exchange-native archives.
4. Identifying instruments and routing family archives.
5. Normalizing trades or reconstructing order books.
6. Validating source integrity.
7. Reconciling multiple sources belonging to the same stream.
8. Writing canonical Parquet files.
9. Evaluating final integrity policies.
10. Validating and publishing completed datasets.

Each operation has different configuration parameters and failure conditions.

### Configuration ownership

MarketForge separates configuration into three major categories.

| Configuration source | Responsibility |
|---|---|
| Runtime TOML | Resources, Parquet targets, integrity policies, scratch processing, diagnostics |
| Prepared batch JSON | Jobs, tasks, source archives, instruments, requested intervals, output identities |
| Exchange profiles | Native schemas, field mappings, timestamp interpretation, trade identity, depth semantics |

The runtime configuration is loaded through:

```text
data/.config/historical/general.toml
```

The prepared normalization fixtures are stored under:

```text
engine/tests/historical/fixtures/
```

Exchange profiles reside within the historical configuration catalog.

The current schema-v4 reader also accepts `families` and minimal `family_id` tasks for dated futures. See [prepared source selection](../../historical/normalization-source-selection.md) for exact fields, discovery, inherited specification provenance and namespace/legacy-ID limits.

Runtime remains schema 1 with defaulted, validated `[reconciliation.*]` and `[timestamp_regression.*]` extensions. Old configurations retain strict behavior. Unknown fields/values are rejected. Existing configured budgets are preserved. See [Processing policies](#processing-policies).

Verified evaluator semantics: enabled `action = "fail"` fails on the first violation, regardless of `max_count`, `max_rate` or sample floors. `degrade` marks observed violations degraded and fails when an absolute count is exceeded (`>`). Immediate worker checks enforce action/count; final evaluation additionally checks global/file/stream rates and independent UTC hourly/daily rate windows. Rate equality is permitted, and rate checks defer below their applicable sample floor. Missing trustworthy timestamps fail configured window verification even when the observed category has no violations. Disabled integrity evaluation bypasses these policy decisions, not archive/resource/reconstruction/publication checks.

---

## 2. Supported Exchanges

The current prepared fixture contains five exchanges.

| Exchange | Identifier | Trades | Depth |
|---|---|---|---|
| Binance | `binance` | Yes | Not present in current fixture |
| Bybit | `bybit` | Yes | Yes |
| Bitget | `bitget` | Yes | Yes |
| Gate.io | `gateio` | Yes | Yes |
| OKX | `okx` | Yes | Yes |

This describes the current test workload, not necessarily every format supported by the wider MarketForge platform.

### Exchange-specific fixtures

```text
normalize_binance.json
normalize_bybit.json
normalize_bitget.json
normalize_gateio.json
normalize_okx.json
```

The original combined fixture is:

```text
normalize.json
```

These fixtures allow independent exchange-level investigations without executing the entire workload.

---

## 3. Instrument Parameters

Every concrete instrument has several identifying properties.

| Parameter | Description | Examples |
|---|---|---|
| `exchange` | Data provider | `bybit`, `okx`, `gateio` |
| `instrument_type` | Instrument lifecycle/type | `spot`, `perpetual`, `future` |
| `market_category` | Market settlement/contract category | `spot`, `linear`, `inverse` |
| `symbol` | Exchange-native instrument symbol | `BTCUSDT`, `BTC-USD-SWAP` |
| `base_asset` | Base currency | `BTC`, `ETH` |
| `quote_asset` | Quote currency | `USD`, `USDT`, `USDC`, `BTC` |
| `instrument_id` | Canonical numerical instrument identity | Integer |
| `tick_size` | Minimum configured price increment | `0.1`, `0.01`, `0.000001` |
| `contract_value` | Contract multiplier/value | `100`, `1`, `0.01`, `0.0001`, `null` |
| `contract_value_asset` | Asset denominating contract value | `BTC`, `USD`, `null` |

### Instrument-type variations

**Spot**

Represents directly traded assets.

Example:

```text
bybit.spot.spot.BTCUSDT.trades
```

**Linear perpetual**

A derivative contract with no scheduled expiration.

Example:

```text
bybit.linear.perpetual.BTCUSDT.trades
```

**Inverse perpetual**

A derivative whose contract-value and quantity interpretation differs from linear contracts.

Example:

```text
bybit.inverse.perpetual.BTCUSD.trades
```

**Linear future**

A derivative with a specified expiration.

Example:

```text
bybit.linear.future.BTCUSDT-25DEC26.trades
```

### Important distinction

`instrument_type` and `market_category` are separate properties.

For example:

```json
{
  "instrument_type": "perpetual",
  "market_category": "inverse"
}
```

The instrument type determines whether the instrument is spot, perpetual, or dated futures.

The market category distinguishes spot, linear, and inverse contract semantics.

These properties must not be inferred solely from a symbol's spelling.

---

## 4. Data Types

The current normalization pipeline handles two principal canonical content types.

| Content type | Description |
|---|---|
| `trades` | Individual executed trades |
| `depth` | Order-book updates and reconstructed price levels |

### Trades

Trade processing includes:

- Parsing source records.
- Extracting timestamps.
- Matching instrument identities.
- Mapping trade identifiers.
- Resolving side and quantity.
- Validating chronological order.
- Reconciling overlapping sources.
- Writing canonical trade records.

### Depth

Depth processing additionally includes:

- Initializing order-book state.
- Processing snapshots.
- Applying incremental updates.
- Validating sequence continuity.
- Detecting gaps.
- Recovering from snapshots.
- Emitting canonical price-level changes.
- Preserving reconstruction metadata.

**Important:** A single depth source message can generate multiple canonical price-level rows.

Consequently:

```text
records_read != events_normalized
```

is expected for some depth workloads.

---

## 5. Depth Feed Variations

The current prepared batch distinguishes two explicit Gate.io feed types.

| Feed | Configuration value | Meaning |
|---|---|---|
| Snapshot | `"snapshot"` | Snapshot-based depth source |
| Incremental | `"incremental"` | Incremental depth source |

Example:

```json
{
  "source_options": {
    "feed": "snapshot"
  }
}
```

Or:

```json
{
  "source_options": {
    "feed": "incremental"
  }
}
```

Other exchanges may use their own native snapshot/delta representations.

The explicit `feed` parameter must match the source profile and its interpretation.

Changing a feed label does not transform the underlying source data.

---

## 6. Input Formats and Compression

### Source record formats

The normalization subsystem recognizes three depth source formats.

| Format | Engine representation | Support |
|---|---|---|
| CSV | `Csv` | Supported |
| JSON Lines | `Jsonl` | Supported |
| Excel workbook | `Xlsx` | Supported for appropriate depth tasks |

Trades use the CSV decoding path in the supplied implementation.

Depth format dispatch is determined from the resolved source schema.

For depth:

- Positional fields indicate CSV.
- Non-positional fields indicate JSONL unless workbook metadata explicitly identifies XLSX.
- `workbook_sheets` must be a positive integer when present.
- Mixing positional and non-positional depth fields is rejected.

Family routing supports native-symbol extraction from CSV and JSONL sources.

The supplied implementation does not support mixed-instrument family routing from XLSX.

### Source container formats

The current workload includes archives using:

```text
.csv.gz
.zip
.tar.gz
.gz
```

The resolved task contains:

```text
source_compression
archive_member
```

`source_compression` determines the source-container reader.

`archive_member` identifies a particular member within an archive when required.

The full supported compression enum is not included in this snapshot.

---

## 7. Prepared Batch JSON Parameters

The prepared batch describes which sources should be normalized.

### Batch-level parameters

| Parameter | Purpose |
|---|---|
| `schema_version` | Prepared-batch protocol version |
| `kind` | Batch document kind |
| `status` | Preparation status |
| `run_id` | Logical run identifier |
| `run_directory` | Associated run directory |
| `batch_path` | Batch document location |
| `ranking_snapshot` | Ranking metadata |
| `ranking_policy` | Instrument-ranking policy |
| `requested_interval.start` | Requested starting timestamp |
| `requested_interval.end` | Requested ending timestamp |
| `jobs` | Normalization jobs |
| `instrument_namespace` | Canonical instrument namespace |
| `instruments` | Authoritative instrument definitions |

The current prepared fixture uses:

```json
{
  "schema_version": 4,
  "kind": "normalization_batch",
  "status": "prepared",
  "ranking_policy": "turnover",
  "requested_interval": {
    "start": "2026-10-27T00:00:00Z",
    "end": "2026-10-30T00:00:00Z"
  }
}
```

Only `turnover` is demonstrated by this fixture. The full set of ranking policies is defined outside the supplied normalization snapshot.

### Job-level parameters

| Parameter | Purpose |
|---|---|
| `operation` | Operation requested |
| `job_id` | Unique job identity |
| `dataset_id` | Dataset identity |
| `tasks` | Sources assigned to the job |
| `ordering.primary` | Primary output ordering |
| `ordering.tie_break` | Tie-breaking rule |
| `output.content_type` | `trades` or `depth` |
| `output.staging_path` | Intermediate output destination |
| `output.dataset_path` | Final dataset destination |

The current fixture uses:

```json
{
  "operation": "normalize",
  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank"
  }
}
```

The engine still applies its internal reconciliation and source-precedence rules. The batch ordering fields should not be treated as permission to rearrange stateful depth events arbitrarily.

### Task-level parameters

| Parameter | Description |
|---|---|
| `task_id` | Task identifier within its job |
| `input_path` | Source archive path |
| `exchange` | Exchange identifier |
| `instrument_type` | Spot, perpetual, future |
| `market_category` | Spot, linear, inverse |
| `base_asset` | Base asset |
| `quote_asset` | Quote asset |
| `symbol` | Concrete symbol, when applicable |
| `instrument` | Concrete contract specification |
| `stream_id` | Logical canonical stream |
| `stream_rank` | Stream ranking/ordering metadata |
| `instrument_id` | Canonical instrument ID |
| `source_options` | Optional source interpretation |
| `scope` | Optional family scope |
| `instrument_selection` | Optional family selection |

### Concrete instrument task

```json
{
  "task_id": 15,
  "exchange": "bybit",
  "instrument_type": "perpetual",
  "market_category": "linear",
  "symbol": "BTCUSDT",
  "instrument": {
    "tick_size": "0.10",
    "contract_value": null,
    "contract_value_asset": null
  },
  "stream_id": "bybit.linear.perpetual.BTCUSDT.depth",
  "stream_rank": 5,
  "instrument_id": 9
}
```

The actual source path is omitted from this illustrative fragment.

---

## 8. Family Archives and Instrument Selection

Some archives contain records for multiple concrete instruments.

MarketForge represents these using a family scope.

Example:

```json
{
  "scope": {
    "kind": "family",
    "identifier": "BTC-USD_UM"
  },
  "instrument_selection": {
    "mode": "all"
  }
}
```

### Family parameters

| Parameter | Meaning |
|---|---|
| `scope.kind` | Family or concrete scope representation |
| `scope.identifier` | Native instrument-family identifier |
| `instrument_selection.mode` | Selection strategy |
| `instrument_selection.symbols` | Explicit symbol selection, where supported |
| `instrument_namespace` | Identity namespace |
| `source_priority` | Source-precedence order |
| `archive_member` | Selected archive member |

The current fixture demonstrates selection mode `all`.

The engine also references explicit selected-symbol collections, but the complete selection enum is defined outside this snapshot.

### Family routing behavior

Records are routed using exchange-native instrument identities.

A record may be:

- Routed to a supported concrete instrument.
- Filtered because it belongs outside the requested scope.
- Filtered because it is not selected.
- Rejected because its native instrument identity is missing or malformed.
- Rejected because selected instrument metadata is unavailable.
- Rejected because the instrument is unsupported.

### Important limitation

Family routing requires authoritative concrete instrument metadata.

It cannot safely invent missing contract specifications.

This is especially relevant to OKX futures-chain archives.

---

## 9. Runtime Resource Parameters

Runtime resources are owned by the processing configuration rather than individual prepared tasks.

### Resource configuration

| Parameter | Meaning | Effect |
|---|---|---|
| `resources.workers` | Worker count | Processing concurrency |
| `resources.memory_budget_bytes` | Maximum accounted memory | Global memory admission |
| `resources.scratch_budget_bytes` | Scratch storage allowance | Reconciliation and report spilling |
| `resources.scratch_path` | Temporary processing location | Scratch-file destination |
| `resources.parquet.row_group_target_bytes` | Target row-group size | Parquet batching |
| `resources.parquet.file_target_bytes` | Target Parquet file size | File rotation |
| `resources.normalization_limits` | Normalization safety limits | Per-source and per-operation bounds |

### Known latest-run resource values

The recent production validation used:

| Resource | Known value |
|---|---:|
| Memory ceiling | 64 GiB |
| Scratch ceiling | 128 GiB |
| Open-file ceiling | 256 |
| Nominal Parquet row-group target | 64 MiB, from earlier configuration context |
| Nominal Parquet file target | 256 MiB, from earlier configuration context |

The first three values are also documented in the memory-admission diagnostics. The exact current Parquet settings should be verified directly in `general.toml`.

The current worker count is not established by the uploaded snapshot.

### Memory accounting

MarketForge reserves resources before admitting work.

The memory estimate includes:

- Active source decoding.
- Input parsing.
- Order-book reconstruction.
- Parquet writer memory.
- Reconciliation workspaces.
- Source metadata.
- Integrity observations.
- Failure-report retention.
- Finalization requirements.

The refactored engine spills completed detailed integrity observations rather than retaining all observations in memory.

Memory admission is based on estimated reservations, not directly measured process RSS.

A successful admission does not guarantee that the operating system will never encounter memory pressure.

---

## 10. Normalization Safety Limits

The supplied code references these members of `NormalizationLimits`.

| Parameter | Purpose |
|---|---|
| `record_bytes` | Maximum permitted source-record size |
| `depth_levels` | Order-book level limit |
| `metric_entries` | Maximum detailed integrity observation entries |
| `reconciliation_bytes` | Reconciliation memory quota |
| `open_files` | Open-file budget |
| `workbook_bytes` | Workbook memory allowance |
| `workbook_xml_bytes` | Workbook XML allowance |
| `workbook_rows` | Workbook row allowance |

These limits protect processing against oversized records, unexpectedly large books, excessive report cardinality, and resource exhaustion.

### Limit variations

**Higher limits**

May permit larger records, deeper books, or more extensive integrity observations.

However, they increase potential memory requirements and may cause admission to reject more concurrent work.

**Lower limits**

Reduce worst-case resource exposure but may reject otherwise valid source data.

### Important distinction

A resource-limit failure is not necessarily a data-integrity failure.

For example:

```text
resource limit: memory bytes
```

means the requested reservation exceeded the configured allowance.

A malformed record or invalid book is a separate category of failure.

---

## 11. Scratch Processing Parameters

The engine supports disk-backed processing for source reconciliation.

### Scratch format

The snapshot demonstrates two representations.

| Value | Description |
|---|---|
| `json` | JSON-based scratch records |
| `typed_trades` | Typed trade scratch representation |

These correspond to the Rust variants `ScratchFormat::Json` and `ScratchFormat::TypedTrades`.

### Scratch codec

The snapshot demonstrates:

| Value | Description |
|---|---|
| `zlib` | Compressed scratch records |
| `uncompressed` | Uncompressed scratch records |

These correspond to `ScratchCodec::Zlib` and `ScratchCodec::Uncompressed`.

The exact serialized TOML spellings should be confirmed from the shared configuration definition.

### Merge fan-in

Parameter:

```text
merge_fan_in
```

This controls the requested fan-in for external merging.

The effective fan-in is constrained by the configured open-file limit.

The implementation reserves additional file handles for readers, output writers, and processing machinery.

### Practical trade-offs

| Configuration | Potential benefit | Potential cost |
|---|---|---|
| Compressed scratch | Lower temporary disk usage | Compression/decompression CPU |
| Uncompressed scratch | Lower codec overhead | Greater scratch I/O and storage |
| Higher merge fan-in | Fewer merge passes | More simultaneous readers |
| Lower merge fan-in | Fewer concurrent handles | More merge passes |
| Typed trade scratch | Specialized trade serialization | Format-specific compatibility requirements |
| JSON scratch | General-purpose serialization | Potentially larger representations |

These are expected engineering trade-offs, not measured performance conclusions.

---

## 12. Trade Identity Parameters

Trade identity is essential for reconciling overlapping archives.

The supplied code supports an explicit trade-identity contract.

The relevant metadata key is:

```text
marketforge_trade_identity
```

### Trade identity scopes

| Scope | Meaning |
|---|---|
| `global` | IDs can be interpreted in a globally defined identity domain |
| `instrument_scoped` | IDs are interpreted within an established instrument identity domain |
| `ordered_run` | Native decimal transaction keys in a checked, non-reset concrete-stream run; source and complete unique-union ordering must validate before loss processing/publication |
| `source_local` | IDs are local to their source |
| `unreliable` | IDs cannot be trusted for cross-source reconciliation |

These are demonstrated by the reconciliation code and tests.

### Example contracts

```json
{
  "scope": "source_local"
}
```

```json
{
  "scope": "instrument_scoped",
  "reset_domain": "instrument_lifetime"
}
```

`instrument_lifetime` requires independent evidence; the label itself is not proof. `ordered_run` also requires an explicit domain label, but validates only the supplied non-reset run, not an instrument lifetime. Bitget spot/perpetual profiles use different labels. Older consumers that do not recognize this native identity-contract value must upgrade; canonical Parquet row schemas and instrument IDs are unchanged.

### Reconciliation rules

MarketForge requires reliable native identity to resolve ambiguous overlapping trade sources.

It does not consider any of the following sufficient on their own:

- Equal timestamps.
- Equal prices.
- Equal quantities.
- Equal trade sides.
- Equal archive dates.

A trade with an unreliable identity cannot automatically be matched to another source's trade.

### Chronological append

A simpler path is possible when the source contract and observed data establish that sources can safely be appended chronologically.

The implementation requires:

- Explicit source-local identity semantics for the append path.
- Validated chronological ordering.
- Non-overlapping accepted timestamp intervals.

Otherwise the engine uses identity reconciliation or rejects unsupported ambiguity.

### Current known issue

```text
bitget.linear.perpetual.BTCUSDT.trades
```

The current source identity/reset-domain evidence is insufficient for overlapping archives.

This should be investigated at the profile and native-ID-contract level.

The safeguard must not be bypassed by arbitrary timestamp-based deduplication.

---

## 13. Depth Sequence Parameters

Depth normalization derives sequence behavior from its normalization rules.

### Supported sequence policies

| Policy | Required rule fields | Meaning |
|---|---|---|
| `Unsequenced` | Neither sequence field | No native sequence validation available |
| `Consecutive` | `sequence_start` | Consecutive sequence interpretation |
| `Ranged` | `sequence_start`, `sequence_count` | Updates cover sequence ranges |

A configuration declaring `sequence_count` without `sequence_start` is invalid.

A task containing conflicting depth sequence semantics is also rejected.

### Sequence-related integrity conditions

The engine distinguishes:

- Valid sequence progression.
- Missing sequence information.
- Sequence gaps.
- Regressions.
- Recovery using subsequent snapshots.
- Unverifiable continuity.

The sequence policy is resolved from normalization metadata rather than arbitrarily selected during execution.

---

## 14. Depth Snapshot and Recovery Parameters

Depth sources have additional reconstruction metadata.

### Snapshot depth

The resolved schema may contain:

```json
{
  "order_book": {
    "snapshot_depth": {
      "bids": 200,
      "asks": 200
    }
  }
}
```

This is an illustrative metadata structure corresponding to the fields read by the engine.

When bid and ask depth values are equal, the engine can record a specific depth-per-side value in boundary metadata.

### Boundary continuity states

The implementation distinguishes several reconstruction outcomes.

| State | Meaning |
|---|---|
| `Verified` | Verifiable sequence and boundary evidence |
| `RecoveredFromSnapshot` | Continuity recovered using a snapshot |
| `Unverifiable` | Available evidence cannot prove full continuity |
| `GapDetected` | Unrecovered continuity interruption |
| `Invalid` | Invalid boundary state |
| `BoundaryMismatch` | Inconsistent boundary evidence |

The final three are explicitly rejected by the depth recovery validator.

### Depth overlap handling

Each source may contain its own independently reconstructed order-book history.

MarketForge orders sources by their observed time intervals.

If independently reconstructed histories overlap, the current reconciliation path rejects them when equivalence and safe recovery cannot be established.

Example failure:

```text
unsupported depth overlap:
independent source books overlap without proven
event equivalence and snapshot/sequence recovery
```

This is not controlled by a simple ignore-duplicates parameter.

Safely supporting overlapping histories requires stronger event identity, continuity, or recovery evidence.

---

## 15. Integrity Policy Parameters

Integrity rules determine how the engine responds to source-data problems.

### Global policy switch

```text
integrity_policy.enabled
```

The policy supports an enabled/disabled state.

When disabled, violations can still be recorded and the resulting integrity status can be degraded.

Disabling the policy is not equivalent to proving the data valid, and it does not disable independent structural reconciliation safeguards.

### Integrity categories

The engine tracks seven categories.

| Category | Meaning |
|---|---|
| `parse_failure` | Source record cannot be parsed |
| `invalid_record` | Parsed record violates validity requirements |
| `sequence_gap` | Missing or discontinuous sequence |
| `timestamp_regression` | Timestamp moves backward |
| `missing_snapshot` | Required book initialization is missing |
| `invalid_book` | Order-book state violates validity requirements |
| `transformation_failure` | Normalization transformation fails |

### Rule parameters

Each category has a corresponding integrity rule.

| Parameter | Purpose |
|---|---|
| `action` | How the engine responds to a violation |
| `max_count` | Maximum permitted absolute violations |
| `max_rate` | Maximum violation fraction |
| `minimum_samples` | Minimum observations before rate enforcement |
| `windows.hourly` | Optional hourly threshold |
| `windows.daily` | Optional daily threshold |

### Actions

The supplied implementation demonstrates:

| Action | Behavior |
|---|---|
| `Fail` | Violation is immediately fatal |
| `Degrade` | Violation may be tolerated subject to configured thresholds |

The complete enum definition is outside the supplied snapshot, so these are the verified variants rather than a claim that no other variant exists.

### Rate thresholds

Rates are fractions, not percentages.

Examples:

| Value | Meaning |
|---|---|
| `0` | Zero tolerated violation rate |
| `0.0001` | 0.01% |
| `0.001` | 0.1% |
| `0.01` | 1% |
| `0.1` | 10% |
| `1` | 100% |

The implementation validates that rate thresholds are finite and between zero and one.

It compares violations against eligible observations using decimal arithmetic.

An observed rate equal to the threshold is permitted; exceeding it is not.

### Minimum sample count

A rate threshold is not evaluated until the observation count reaches `minimum_samples`.

This prevents rate checks from making decisions on insufficient samples.

An absolute `max_count` limit can still fail earlier.

### Policy scopes

The final evaluator supports:

- Global violation counts.
- Global violation rates.
- Per-file violation rates.
- Per-stream violation rates.
- Daily violation rates.
- Hourly violation rates.

The implementation also tracks observations that cannot be attributed to trustworthy event-time windows.

If a time-window policy requires attribution but eligible observations lack trustworthy timestamps, that can itself cause a policy failure.

### Immediate versus final evaluation

**During source processing**, the engine enforces immediately fatal categories.

**During final evaluation**, it checks counts, rates, time windows, and whole-job integrity conditions.

This separation prevents premature rejection based on incomplete rate denominators.

---

## 16. Source Ordering Parameters

The engine distinguishes several kinds of ordering.

| Ordering | Description |
|---|---|
| Prepared task order | Order in the batch |
| Source priority | Deterministic source precedence |
| Source record order | Native archive record order |
| Event timestamp order | Chronological order of canonical events |
| Stream rank | Cross-stream tie-breaking metadata |

### Source priority

Source priority remains deterministic provenance and tie ordering. Under current strict reconciliation it does not authorize conflicting-event exclusion; reliable identity conflicts fail before publication.

For the current prepared-batch protocol, it may be provided through:

```text
marketforge_source.source_priority
```

The implementation falls back to the task's original position when no explicit priority is available.

### Important invariants

Source ordering does not justify discarding valid events.

For example, a later source containing an earlier timestamp may require reconciliation, rather than simple append.

For depth data, sorting individual price-level updates by timestamp does not repair an invalid order-book reconstruction.

---

## 17. Stream Grouping Parameters

MarketForge groups tasks into compatible logical execution streams.

Grouping depends on resolved semantics such as:

- Stream ID.
- Exchange.
- Instrument ID.
- Symbol.
- Instrument specification.
- Normalization rules.
- Source ordering.
- Instrument namespace.
- Profile identity.
- Source options.

Source file paths are not themselves the primary grouping identity.

Compatible daily archives therefore normally belong to one logical stream.

### Stream execution variations

**Single-source stream**

One input source can proceed directly through normalization and writing where the relevant path supports it.

**Multi-source trade stream**

Multiple trade archives may require chronological append or identity reconciliation.

**Multi-source depth stream**

Multiple depth archives require preservation of stateful source-event order and reconstruction boundaries.

**Family stream**

A mixed-instrument archive requires routing into concrete instrument streams before appropriate reconciliation.

---

## 18. Parquet Output Parameters

### Configurable targets

| Parameter | Meaning |
|---|---|
| `row_group_target_bytes` | Desired size of a Parquet row group |
| `file_target_bytes` | Desired size of each Parquet part |

These are resource settings, not an independent prepared-task field.

### Physical output structure

Canonical output is organized by logical stream.

```text
streams/
  <stream-fingerprint>/
    trades/
      part-000000.parquet
      sources.json
```

For depth:

```text
streams/
  <stream-fingerprint>/
    depth/
      part-000000.parquet
      events.parquet
      segments.json
      boundary.json
      sources.json
```

The exact files present depend on the output type and whether execution reached successful finalization.

### Depth-specific auxiliary files

| File | Purpose |
|---|---|
| `events.parquet` | Source-event index and canonical-row offsets |
| `segments.json` | Reconstruction segments |
| `boundary.json` | Source boundary and book-state evidence |
| `sources.json` | Source contribution metadata |

### File-size targets

The configured file target is not necessarily a hard upper bound on every physical output file.

Actual file sizes depend on writer buffering, row-group boundaries, encoding, and rotation behavior.

A file larger than the nominal target should be investigated against the writer's actual implementation before being classified as incorrect.

---

## 19. Dataset Publication Parameters

A normalization job uses distinct staging and final dataset destinations.

### Staging

Contains intermediate output during execution.

Staged Parquet files are not authoritative completed datasets.

### Published dataset

A dataset is successfully published only after completing the required validation and finalization steps.

### Dataset manifest

The manifest records:

- Protocol version.
- Job ID.
- Dataset ID.
- Content type.
- Canonical row count.
- Parquet file count.
- Timestamp boundaries.
- Source-task inventory.
- Stream metadata.
- Auxiliary files.
- Integrity status.
- Integrity evaluation.
- Processing metrics.

The snapshot defines manifest protocol versions 3 and 4.

Version 4 supports additional reconciled source-row ownership information.

### Publication rule

A failed integrity evaluation cannot produce a committed dataset manifest.

A processing failure may still leave useful staging artifacts and diagnostic evidence.

---

## 20. Normalization Diagnostics Parameters

The runtime provides diagnostic instrumentation.

### Configuration

| Parameter | Meaning |
|---|---|
| `diagnostics.mode` | Diagnostic collection mode |
| `diagnostics.interval_ms` | Snapshot collection interval |

### Verified modes

| Mode | Description |
|---|---|
| `disabled` | Do not start optional periodic diagnostics |
| `basic` | Collect periodic processing and resource snapshots |
| `detailed` | Include additional measurements such as current process RSS on supported systems |

### Diagnostic information

The implementation can record:

- Active processing phases.
- Phase durations.
- Source counters.
- Processing throughput indicators.
- Resource snapshots.
- Scratch usage.
- Memory-accounting information.
- Process CPU measurements.
- Available RSS measurements.
- Software/build metadata.
- Workload identity.

### Diagnostic outputs

The implementation writes files such as:

```text
live.jsonl
performance.json
effective-config.json
```

The Ratatui dashboard also redirects diagnostic log output to a temporary log while it owns the terminal.

### Measurement distinctions

The following are not interchangeable:

```text
Estimated admission memory
Reserved/accounted memory
Measured process RSS
```

Similarly:

```text
Source records read
Canonical rows normalized
Rows submitted to staging
Rows definitively published
```

Each describes a different stage of execution.

---

## 21. Progress and Task-State Variations

The normalization dashboard tracks source tasks, stream operations, and final job outcomes.

Relevant task states include:

| State | Meaning |
|---|---|
| Running | Task is executing |
| Completed | Task successfully completed its applicable work |
| Failed | Task encountered a failure |
| Skipped | Task was deliberately not executed |
| Stopped | Work stopped following a failure or cancellation |
| Not run | Task was not started |

Stream-level reconciliation can fail even if individual input tasks successfully decoded and normalized their records.

Likewise, a complete source task does not prove its dataset was published.

### Major processing phases

The code exposes phases including:

- Source reading.
- Decoding.
- Family routing.
- Source normalization.
- Reconciliation.
- External sorting.
- Identity resolution.
- Depth replay.
- Parquet finalization.
- Integrity evaluation.
- Manifest validation.
- Publication.

The dashboard uses independent phase telemetry, avoiding reliance on input-record counters during long post-input operations.

---

## 22. Output Integrity Metrics

Processing counters distinguish different forms of work and failure.

### Source counters

| Counter | Meaning |
|---|---|
| `records_read` | Physical source records examined |
| `records_matched` | Records matched to a selected instrument |
| `records_skipped_instrument` | Records excluded by instrument selection |
| `records_rejected` | Rejected records |
| `records_rejected_unmatched` | Rejections without an established instrument match |
| `records_processed` | Successfully processed source records |
| `records_unprocessed` | Matched records not processed |
| `records_unrouted` | Records not routed to a concrete instrument |

### Canonical output counters

| Counter | Meaning |
|---|---|
| `events_normalized` | Canonical rows generated |
| `events_written` | Canonical rows recorded as written |
| `rows_suppressed` | Rows suppressed by reconciliation |
| `rows_unpublished` | Normalized rows not published |

The engine validates the relationship:

```text
events_normalized
    = events_written
    + rows_suppressed
    + rows_unpublished
```

The counters deliberately distinguish source parsing failures from reconciliation suppression.

### Task counters

```text
tasks_completed
tasks_failed
```

These count task outcomes and are distinct from the dashboard's broader task-state classifications.

---

## 23. Normalization CLI Parameters

The normalization command used in the current workflow is:

```bash
./engine/target/release/marketforge-normalize \
  --project-root . \
  --config data/.config/historical/general.toml \
  --batch engine/tests/historical/fixtures/normalize_bybit.json \
  --output-root "data/tests/historical/normalize/bybit/$(date +%Y%m%d-%H%M%S)" \
  --tui
```

### Verified workflow arguments

| Argument | Purpose |
|---|---|
| `--project-root` | Repository/project root |
| `--config` | Runtime TOML |
| `--batch` | Prepared normalization batch |
| `--output-root` | Execution output destination |
| `--tui` | Interactive terminal dashboard |

These are confirmed by the command already used successfully in the MarketForge workflow.

The CLI parser itself is not included in the supplied Repomix snapshot, so the complete flag inventory and defaults are not established here.

Use the executable's help output as the authoritative CLI reference.

---

## 24. Benchmark Parameters

The normalization subsystem includes a separate benchmark interface.

Its argument parser is included in the supplied snapshot.

### Benchmark CLI options

| Parameter | Default | Variations |
|---|---|---|
| `--input` | Required for ordinary benchmark runs | Prepared batch path |
| `--workers` | `1` | Comma-separated positive worker counts |
| `--repetitions` | `3` | Positive integer |
| `--warmups` | `0` | Nonnegative integer |
| `--scheduler` | `global` | `global`, `per-job` |
| `--retention` | `metadata` | `metadata`, `full` |
| `--dry-run` | Disabled | Enabled/disabled |
| `--project-root` | `.` | Project path |
| `--config` | `data/.config/historical/general.toml` | Runtime TOML |
| `--output-root` | Generated/default benchmark location | Explicit directory |
| `--memory-budget` | Runtime configuration | Byte override |
| `--scratch-budget` | Runtime configuration | Byte override |
| `--row-group-target` | Runtime configuration | Byte override |
| `--file-target` | Runtime configuration | Byte override |

The benchmark executable also includes an internal hidden trial-execution argument that should not be treated as a normal user-facing tuning parameter.

### Scheduler variations

**Global**

Schedules work across the selected jobs using the global processing path.

**Per-job**

Executes individual jobs separately.

These modes can affect resource utilization and execution timing.

They must not change canonical data correctness.

### Retention variations

**Metadata**

Successful trial datasets are removed after verification, while supported metadata artifacts are preserved.

**Full**

Successful trial datasets are retained.

Failed trial artifacts are handled separately.

### Benchmark measurements

The benchmark system can report:

- Source records per second.
- Canonical rows per second.
- Input and output throughput.
- Execution time.
- CPU usage.
- Peak RSS when available.
- Logical and physical I/O.
- Parquet file count.
- Parquet row-group count.
- File-size distributions.
- Worker scaling efficiency.
- Output correctness fingerprints.

The system also supports benchmark summaries and comparisons.

### Comparison dimensions

Verified comparison categories include:

```text
software
workers
memory-budget
scratch-budget
parquet
scheduler
hardware
build
integrity-policy
profiles
cache
```

A performance comparison is only useful when workload identity, correctness evidence, and relevant configuration differences are controlled.

---

## 25. Known Internal Constants

Some behavior is controlled by implementation constants rather than ordinary user configuration.

| Constant/behavior | Value or property |
|---|---|
| Default maximum integrity diagnostics | 20 |
| Maximum individual diagnostic message | 1,024 bytes |
| Maximum stored policy-violation samples | 100 |
| Minimum report serialization workspace target | 8 MiB |
| Detailed observation retention | Disk-backed after sealing |
| Stream directory fingerprint | 64 lowercase hexadecimal characters |
| Canonical timestamp representation | Nanosecond-based integer |
| Integrity window sizes | Hourly and daily |

These should not be represented as arbitrary TOML options unless the shared configuration explicitly exposes them.

The report workspace can also grow beyond its minimum based on validated identity lengths.

---

## 26. Recommended Test Variations

To investigate normalization failures efficiently, divide the workload along independent dimensions.

### Exchange

```text
Binance
Bybit
Bitget
Gate.io
OKX
```

### Content type

```text
Trades
Depth
```

### Market category

```text
Spot
Linear
Inverse
```

### Instrument type

```text
Spot
Perpetual
Future
```

### Instrument

Examples:

```text
BTCUSDT
BTCUSD
BTCUSDT-25DEC26
ETHBTC
ETHUSDC
BTC-USD_UM-261127
```

### Source count

| Variation | Investigation |
|---|---|
| One source | Basic decoding, normalization, writer behavior |
| Two adjacent sources | Boundary overlap and cross-source continuity |
| Three sources | Multi-boundary reconciliation |
| Entire logical stream | Full source reconciliation |
| Entire exchange | Exchange-level integration |
| Complete batch | System integration |

### Data integrity

Relevant investigations include:

- Clean sources.
- Malformed records.
- Timestamp regressions.
- Duplicate trade identities.
- Overlapping trade intervals.
- Overlapping depth intervals.
- Missing depth snapshots.
- Sequence gaps.
- Recovery after a gap.
- Incorrect contract metadata.
- Missing family instrument metadata.

### Performance

Relevant variations include:

- Worker count.
- Memory budget.
- Scratch budget.
- Scratch codec.
- Scratch format.
- Merge fan-in.
- Parquet row-group target.
- Parquet file target.
- Scheduler mode.

Change one primary variable at a time when the objective is to understand its effect.

For reproducibility, preserve source identities and original input data.

---

## 27. Current Investigation Matrix

The latest full normalization run exposed several independent issues.

| Exchange | Market | Instrument | Data | Investigation |
|---|---|---|---|---|
| Bitget | Linear perpetual | BTCUSDT | Trades | Native ID/reset-domain overlap |
| Bybit | Inverse perpetual | BTCUSD | Depth | Reconciliation/unpublished output |
| Bybit | Linear perpetual | BTCUSDT | Depth | Reconciliation/unpublished output |
| Bybit | Linear future | BTCUSDT-25DEC26 | Depth | Reconciliation/unpublished output |
| Bybit | Spot | BTCUSDT | Depth | Reconciliation/unpublished output |
| Bybit | Spot | ETHBTC | Depth | Reconciliation/unpublished output |
| Bybit | Spot | ETHUSDC | Depth | Reconciliation/unpublished output |
| Gate.io | Inverse perpetual | BTC_USD | Depth snapshots | Timestamp regression |
| Gate.io | Spot | BTC_USDT | Incremental depth | Source/stream failure |
| OKX | Linear future | BTC-USD_UM-261127 | Depth | Family-source failure |

The precise Bybit root cause must still be established independently for each affected stream.

The reported unsupported depth-overlap error should not automatically be attributed to all six Bybit streams without matching the failing stream fingerprint.

---

## 28. Parameter Ownership and Safe Changes

Not every parameter should be modified for the same reason.

| Problem | Primary investigation layer |
|---|---|
| Wrong instrument identity | Batch metadata / registry |
| Incorrect tick size | Authoritative instrument metadata |
| Wrong contract multiplier | Instrument metadata |
| Wrong CSV column mapping | Exchange profile |
| Incorrect timestamp units | Exchange profile |
| Incorrect native trade-ID interpretation | Trade identity contract |
| Unrecognized family instrument | Family selection / instrument metadata |
| Missing snapshot | Source data / depth profile |
| Invalid sequence semantics | Depth normalization profile |
| Overlapping depth histories | Reconciliation contract / source evidence |
| Excessive integrity rejections | Source evidence and integrity policy |
| Excessive memory reservation | Resource estimation / runtime configuration |
| Scratch exhaustion | Scratch configuration and resource accounting |
| Too many open files | File limit / merge fan-in |
| Excessive Parquet file count | Writer targets and rotation behavior |
| Poor throughput | Benchmark-controlled runtime parameters |
| Frozen or stale dashboard | Progress instrumentation / TUI |

### Configuration safety

Do not use changes to integrity thresholds, memory budgets, or source selections merely to conceal an unresolved data problem.

In particular:

- Do not reinterpret source-local trade IDs as globally unique without evidence.
- Do not discard overlapping depth updates without proving correct reconstruction.
- Do not fix timestamp regressions by sorting stateful events blindly.
- Do not fabricate missing futures contract specifications.
- Do not treat incomplete staging files as published datasets.

---

## 29. Configuration Files to Inspect for Exact Defaults

To complete the inventory of **current configured values**, inspect:

```text
data/.config/historical/general.toml
```

For the full configuration schema, inspect the shared Rust modules defining:

```text
ProcessingConfig
ResourceConfig
NormalizationLimits
ParquetResourceConfig
IntegrityPolicy
IntegrityRule
IntegrityWindowRule
ScratchFormat
ScratchCodec
DiagnosticsMode
```

For exchange-specific variations, inspect the historical format catalog and profiles associated with:

```text
binance
bybit
bitget
gateio
okx
```

For the complete normalization CLI, inspect the source of:

```text
marketforge-normalize
```

The complete Rust enum definitions and current TOML contents are required before documenting every permitted value and default as authoritative.

---

## 30. Summary

MarketForge's normalization behavior is controlled by six broad groups of parameters.

| Group | Main responsibility |
|---|---|
| Source and instrument parameters | What data is processed and how instruments are identified |
| Format and normalization parameters | How native records become canonical events |
| Reconciliation and sequence parameters | How sources are combined without corrupting chronology or state |
| Integrity policy parameters | Which data problems are tolerated or fatal |
| Resource and output parameters | How processing uses CPU, memory, scratch storage, and Parquet |
| Diagnostics and benchmark parameters | How execution is monitored, measured, and compared |

The most important distinction for the current investigation is between **source interpretation**, **cross-source reconciliation**, and **integrity policy**.

These solve different problems.

A valid source can still fail reconciliation. A successful reconciliation can still fail the final integrity policy. A task can successfully write staging files while the dataset remains unpublished.

For reliable debugging, isolate the smallest workload that preserves the failing condition, retain authoritative source evidence, and change only the configuration or implementation component responsible for that condition.

---

## Source Code References

Relevant modules within the supplied normalization snapshot:

```text
config.rs
task.rs
streams.rs
streams_depth.rs
reconciled.rs
reconcile.rs
reconcile/external.rs
reconcile/spool.rs
sequence_policy.rs

worker/trades.rs
worker/depth.rs
worker/ordering.rs

metrics/counters.rs
metrics/integrity.rs
metrics/policy.rs
metrics/windows.rs
metrics/retained.rs

resources/admission.rs
resources/estimate.rs
resources/classification.rs
resources/compute.rs

parquet/mod.rs
manifest/models.rs
manifest/validation.rs

progress/reporter.rs
progress/diagnostics.rs

benchmark/model.rs
benchmark/plan.rs
benchmark/analysis.rs
benchmark/comparison.rs
benchmark/verification.rs
```

**Document status:** Source-grounded parameter inventory. Exact shared-config defaults, complete CLI variants, and full exchange-profile rule vocabularies remain to be verified from their authoritative source files.
## Processing policies

These schema-1 runtime extensions default to strict; old configurations need no migration. Tables do not activate permissive processing. Positive tolerances and loss allowances must be explicit. Processing, integrity acceptance, reconstruction and publication are separate checks: enabled integrity `action="fail"` rejects any occurrence even with a nonzero count threshold. `degrade` degrades on occurrence and fails above any applicable overall/file/count/rate/hourly/daily threshold. Sample floors defer rate checks, not finite count checks. Disabling integrity evaluation bypasses its policy decisions, never archive validation, processing bounds, reconstruction, resource limits or publication checks.

### Reconciliation settings

| Table/key | Default | Meaning |
|---|---|---|
| `reconciliation.trades.overlap_policy` | `strict` | `strict` or `prefer_source` |
| `reconciliation.depth.overlap_policy` | `strict` | `strict`, `snapshot_recovery` or `prefer_source` |
| `reconciliation.{trades,depth}.prefer_source.selection` | `earlier` | Only `earlier` or `later`; compare actual accepted source coverage starts |
| `reconciliation.depth.prefer_source.switch_boundary` | `snapshot` | `snapshot` or independently proven `source_boundary` |
| `reconciliation.{trades,depth}.prefer_source.on_unique_data_loss` | `fail` | `fail` rejects any exclusion; `mark_degraded` explicitly authorizes bounded exclusion |
| `reconciliation.{trades,depth}.prefer_source.max_excluded_events` | `0` | Job-wide logical event allowance for the relevant content type |
| `reconciliation.{trades,depth}.prefer_source.max_excluded_rate` | absent | Optional finite fraction in [0,1], enforced job-wide |
| `reconciliation.{trades,depth}.prefer_source.minimum_samples` | `0` | Rate-check sample floor; count allowance still applies below it |
| `reconciliation.{trades,depth}.prefer_source.max_excluded_interval_ms` | absent | Maximum contiguous excluded span within each source/stream |

`mark_degraded` requires a positive finite `max_excluded_events`; rate/sample-floor settings alone never authorize unbounded losses. Milliseconds must fit checked signed canonical nanoseconds. Invalid rates, enums, unsupported combinations, unknown keys and overflow fail configuration loading. The default preference table has no effect under strict policy.

Verified equivalence runs before loss decisions. A reliable trade identity plus matching complete contents proves a duplicate; equal timestamps/price/size/side do not. Conflicting contents under a reliable identity fail strict processing. Preference can resolve the conflict only with loss authorization, counting the losing observation conservatively as unverified. Distinct reliable IDs survive overlapping/equal-time boundaries, including nonconsecutive arbitrary-length numeric IDs. Missing or reset-ambiguous identities do not acquire global scope from their shape. Native-domain ambiguity under strict processing fails with the required identity evidence described in the diagnostic.

Preference ranks actual validated coverage starts. Equal starts use canonical physical path plus member identity as a stable reproducibility tie-break, with no quality claim. The ownership sweep covers nested, chained and three-or-more sources; legacy task priorities are not preference authority. Reliable nonconflicting trades are unioned. Unknown identities use explicit conflict-interval ownership and conservative exclusion accounting. Task/worker completion order does not choose a winner.

Depth events are atomic messages/grouped snapshots. Independent level-diff vectors are never spliced without a matching reconstruction state. Source prefixes are reconstructed from scratch spools; selected histories are applied to a separate book and complete state is checked after every event. Identical sequenced boundary snapshots compare decoded evidence, including complete snapshot contents and timestamps; JSON object key order is irrelevant. Canonical-changing terminal snapshots can be represented by the next full initialization. Deltas sharing snapshot IDs remain distinct.

Overlap `snapshot_recovery` retains unique supplied observations in independently initialized source histories and records unproven continuity. It does not exclude unique events or claim one continuous market history. `prefer_source` can exclude observations within explicit limits. Snapshot switches wait for actual authoritative snapshots. `source_boundary` requires matching complete book state and compatible native sequence; unsequenced feeds cannot invent that evidence. Returning to an earlier source requires another valid transition. Unsafe transitions, missing initialization or failed source reconstruction prevent publication even with generous loss settings.

### Timestamp settings

| Table/key | Default | Meaning |
|---|---|---|
| `timestamp_regression.trades.policy` | `strict` | `strict`, `preserve`, `reorder`, `discard` |
| `timestamp_regression.trades.max_regression_ms` | `0` | High-water regression bound for permissive handling; strict never accepts because of tolerance |
| `timestamp_regression.trades.max_reorder_window_ms` | `0` | Positive reorder bound, at least `max_regression_ms` |
| `timestamp_regression.trades.max_discarded_events` | `0` | Job-wide discard allowance |
| `timestamp_regression.trades.max_discarded_rate` | absent | Optional finite [0,1] job-wide discard rate |
| `timestamp_regression.trades.minimum_samples` | `0` | Rate floor, independent of the finite count allowance |
| `timestamp_regression.depth.policy` | `strict` | `strict`, `preserve`, `snapshot_recovery` |
| `timestamp_regression.depth.max_regression_ms` | `0` | Checked high-water bound |
| `timestamp_regression.depth.require_sequence_continuity` | `true` | Applicable native validation remains mandatory; false is unsupported |
| `timestamp_regression.depth.require_recovery_snapshot` | `true` | Authoritative initialization remains mandatory; false is unsupported |
| `timestamp_regression.depth.max_discarded_events` | `0` | Positive allowance required for lossy recovery; enforced job-wide |

Equal timestamps are never regressions. Raw-source evidence compares adjacent accepted observations within a concrete source/native reset domain. Processing uses the relevant high-water timestamp so cumulative backward steps cannot evade a bound. Validated native snapshot resets start a new domain; independent sources are not accidentally compared as one native sequence. Raw anomalies survive deduplication; logical survivor ordering violations are reported separately. Arithmetic is checked at nanosecond precision. Original event/system timestamps are never clamped, shifted or rewritten.

Trade `preserve` retains original native source order, timestamps and original record ordinals. `reorder` chronologically sorts independent executions deterministically, after identity reconciliation, using exact numeric IDs for equal-time ties and source positions as final ties. It uses admitted bounded external runs/scratch, not an unbounded time-window buffer; resource exhaustion fails. Both regression and reorder bounds must be positive. `discard` needs positive regression/count allowances, excludes only eligible regressing survivors, and cannot mask parsing/identity/overlap failures. Duplicate-related raw regressions can resolve after verified deduplication, while a fatal raw integrity rule still fails the job.

Depth `preserve` applies valid native order and checks the book without timestamp sorting. Snapshot-only profiles (including Gate.io inverse perpetual snapshot) use independent authoritative snapshots, without invented sequence fields. `snapshot_recovery` invalidates publication of affected dependent deltas and resumes at an actual validated snapshot with full initialization. Every omitted logical observation is charged conservatively. A regressing authoritative snapshot can recover immediately with zero omission; incremental streams without a subsequent snapshot fail. A snapshot restores state, not omitted history. Native sequence and book validation always remain mandatory where applicable.

### Accounting, quality and output compatibility

The rate denominator is accepted logical source observations minus verified duplicate observations; it includes excluded/unverified observations. It excludes parse/normalization rejections and never counts expanded level rows as separate events. Count/rate bounds apply job-wide per content type; stream/source totals remain in task metrics. Contiguous loss spans continue across reasons and duplicates until an observation is actually retained. Unknown uniqueness is reported as unverified exclusion and charged in full. Known-unique and unverified exclusions both degrade completeness; verified duplicate suppression does not. Exceeding any applicable bound prevents manifest publication.

`source.processing` reports raw/logical regressions, logical input/published events, original/expanded/staged rows, known/unverified exclusions by reason, maximum contiguous spans, recovery/source-switch counts and up to eight bounded decision samples with original source ordinal and selected task when available. Task/source/member/identity contracts remain in the same report; no per-event report inventory is retained. Physical source reads, integrity-rejected rows, normalized rows, suppression, unpublished survivors and committed output remain distinct. On failure committed counters become zero; staged rows and unpublished output remain evidence. Cleanup/report failures append context without replacing the primary failure.

Activated policies use **manifest 5**, preserving canonical Parquet row schemas. Its `processing_quality` records effective policies, actual physical timestamp ordering, reconstruction validity, supplied-observation preservation, continuity, inherited-metadata assertion provenance and unknown external historical coverage. Native-order depth uses event indexes and initialization segments; never replay its price-level rows through timestamp sorting. Depth boundary envelope 3 (`segmented_sources`) permits repeated source IDs for ownership returns; envelope 2 remains unique-source only. Streaming boundary readers validate the full envelope, including suffixes, with bounded decoded state. Trades additionally stream `sources.jsonl`, mapping every canonical row to task/original record ordinal, validated against row ownership before publication.

The current timestamp-first historical merger supports manifest 2/3 only and explicitly rejects 4/5 with an actionable reader-compatibility error. Event-index/segment-aware Parquet readers can consume the new representation; the normalizer does not claim existing timestamp-first consumers can replay native-order output. A committed dataset does not establish complete exchange history.

### Bitget evidence boundary

Bitget historical spot and linear-perpetual feeds now use separate `ordered_run` native-ID contracts. Each job validates nonempty decimal IDs, matching identity contents, increasing native survivor order and increasing IDs in the chronological union before publication. Missing IDs, conflicts and resets fail; no lifetime/global uniqueness is inferred. The native `trade_id` mapping was already correct. This permits lossless equal-time archive continuations and verified overlapping-ID reconciliation. See [Bitget trade reconciliation](../../historical/bitget-trade-reconciliation.md) for the primary documentation boundary, runtime checks and full-corpus evidence.

### Offline family handoff

Python should emit schema-4 family definitions and minimal `family_id` tasks, retaining prepared IDs, ranks, selections and physical paths. Rust discovers expired contracts offline and publishes exact resolved catalogs. The existing deterministic namespace strategy and exact-mapping precedence remain unchanged; a hash cannot reproduce removed arbitrary legacy IDs. Future batches needing legacy IDs must carry/reuse those exact mappings. Family inheritance is a reproducible batch assertion, not independent proof of identical historical specifications. No Python implementation or acquisition/database redesign is included here.
