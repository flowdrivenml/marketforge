# Prepared Engine contracts

## Prepared normalization batch

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

Only `turnover` is demonstrated by this fixture. The full set of ranking policies is defined in the application ranking configuration.

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

## Family archives and instrument selection

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


Forge emits normalization batch version 4 in `forge/historical/normalization/batch.py`. Rust parses and validates it in `engine/src/historical/config/batch.rs`. The maintained parser workloads are `engine/tests/historical/fixtures/normalize.json` and `engine/tests/historical/fixtures/merge.json`. They are regression inputs and may refer to local files.

A prepared merge job carries `job_id`, `operation`, `inputs`, `time_range`, and `output`. Each input carries the selected registry file ID, absolute path and SHA-256, plus its owning manifest path and SHA-256 and decimal-string Engine publication ID. Forge prepares it in `forge/historical/merge/service.py`. Rust checks it in `engine/src/historical/job/merge_load.rs`. See the [merge procedure](../workflows/merge.md) for layout restrictions.

## What each version governs

| Artifact | Producer | Consumer and supported action |
| --- | --- | --- |
| Profile schema 2 TOML | User or bundled profile catalogue | Rust profile compiler validates and resolves a source interpretation. Forge stages and installs the compiled definition by ID and fingerprint. |
| Recipe version 1 TOML | User | Forge resolves acquisition, registration or normalization selections. Each recipe has its own schema. |
| Normalization batch schema 4 JSON | Forge preparation | Rust validates and executes trades and depth jobs. Registered source paths, concrete authority IDs and output destinations are frozen for execution. |
| Historical runtime schema 1 TOML | Operator | Rust validates resource, scratch and policy settings. Merge imposes additional byte alignment and compression constraints. |
| Normalization manifests protocol 3–5 | Rust normalizer | Forge verifies and registers primary files and shared lineage. Merge accepts a narrower set based on the manifest, content, ordering and source ownership. |
| Prepared merge job JSON | Forge merge preparation | Rust validates checksum-pinned primary files and their owning manifests, then executes a compatible selection. Runtime settings are loaded separately. |

For merge, [supported layouts](../workflows/merge.md#supported-layouts) explains what can be selected from each publication. The [publication reference](publications.md) describes canonical columns and auxiliary dependencies.
