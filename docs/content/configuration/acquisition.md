# Acquisition and import configuration

Saved acquisition recipes live in `data/.config/acquisition/NAME.toml`. Commands accept `NAME` or `NAME.toml`, not arbitrary recipe paths. Names use a safe managed filename; do not pass traversal or directory separators. Application-wide `--config` selects application settings, not an acquisition recipe.

## Complete example

Save this as `data/.config/acquisition/september.toml`, or edit the template created by `acquisition new september`:

```toml
version = 1

[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-03T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"

[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"
```

This is structurally valid and accepted by the source's request validator; actual files for the dates were not checked by this documentation task. Replace dates and identifier intentionally. Dates select whole overlapping archives, not individual rows inside them.

## Schema

| Field | Requirement/default | Meaning |
| --- | --- | --- |
| `version` | Optional, defaults to integer `1`; only `1` allowed | Recipe version. |
| `[interval].start`, `.end` | Required timezone-aware TOML datetimes; end later than start | Half-open `[start, end)`; operation normalizes to UTC. |
| `[[requests]]` | Required nonempty array | One or more source selections sharing the interval. |
| `exchange` | Required lowercase code | Implemented adapters: `binance`, `bybit`, `okx`, `bitget`, `gateio`. The schema alone does not verify adapter support. |
| `instrument_type` | Required: `spot`, `perpetual`, `future`, `option` | Type separate from market category. |
| `market_category` | Required: `spot`, `linear`, `inverse`, `option` | Must also fit the source's support matrix. |
| `data_type` | Required: `trades` or `order_book_l2` | Historical archive content selection. |
| `[requests.scope].kind` | Required: `instrument`, `family`, `base_coin` | Source-native selection granularity. |
| `[requests.scope].identifier` | Required nonblank string | Native symbol/family/base coin, not an instrument reference or wildcard. |
| `[requests.source_options].feed` | Optional; default `incremental` when applicable | Gate.io L2 only: `incremental` or `snapshot`. |

Unknown fields are rejected. TOML timestamps must retain their timezone; use native datetimes as above. Structural validation does not check provider support, current listings, database readiness, or archive availability. Mapping performs adapter validation and network discovery.

## Scope and provider options

Use the [source matrix](../workflows/acquisition/historical-data.md#supported-sources) before changing combinations. Spot requires spot category; non-option derivatives require linear/inverse; options require option category. Binance accepts trades only. Bitget future/linear and options are unsupported.

- Most archive selections use exact native instrument scope. Do not use `ANY`, a glob, or a current-market qualified reference as a replacement for the provider's symbol.
- Bybit options require `base_coin` such as `BTC`; the files are not individual-contract selections.
- OKX derivatives require `family` such as `BTC-USD`; perpetual and dated-future selections remain distinct. Options also permit base-coin expansion from currently advertised families.
- Gate.io L2 defaults to incremental orderbooks. Snapshot feed uses a separate archive path and identity; declare it explicitly when intended.

Here is a complete multi-request recipe illustrating those scope differences. Its support was checked locally, not upstream availability:

```toml
version = 1

[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-02T00:00:00Z

[[requests]]
exchange = "okx"
instrument_type = "future"
market_category = "inverse"
data_type = "trades"
[requests.scope]
kind = "family"
identifier = "BTC-USD"

[[requests]]
exchange = "bybit"
instrument_type = "option"
market_category = "option"
data_type = "order_book_l2"
[requests.scope]
kind = "base_coin"
identifier = "BTC"

[[requests]]
exchange = "gateio"
instrument_type = "spot"
market_category = "spot"
data_type = "order_book_l2"
[requests.scope]
kind = "instrument"
identifier = "BTC_USDT"
[requests.source_options]
feed = "snapshot"
```

Archive calendars differ. OKX trades use UTC+8 calendar days and books UTC; Bitget uses UTC+8 discovery windows. Gate trades are monthly, books hourly. The adapter converts intervals and keeps nominal bounds; a UTC midnight request can include files whose calendar boundary extends beyond it. No row-level trimming is done.

## Application HTTP/storage settings

The following is a complete optional application-settings example, **not** a recipe. Put reviewed settings in the application configuration chosen by global `--config`:

```toml
version = 1

[storage]
data_root = "data"

[presentation]
mode = "progress"
color = "auto"

[http]
requests_per_second = 2.0
max_concurrent = 4
timeout_seconds = 30.0
max_attempts = 3
```

These values are the model defaults. `max_concurrent` is 1–64; attempts 1–10; rate and timeout must be positive and finite. Acquisition's `--requests-per-second` overrides pacing but is capped per provider: Binance 2, Bybit 1, OKX 2.5, Bitget 0.5, Gate.io 1 requests/second. Retries and redirects consume the shared upstream budget; server cooldowns can stop further admission. Lower settings can reduce request pressure, not bypass throttling.

Raw/runs paths follow the configured data root. Editor selection uses `presentation.editor`, then `VISUAL`, `EDITOR`, or `vi`; see [CLI effects](../reference/cli/acquisition.md). This settings file does not contain the guarded testing database URL; that is supplied separately through the testing key.

## External import configuration

Save the following as `data/.config/acquisition/import/recorder.toml`. Replace the source path, actual identity and specification time with facts about the recording. Relative `root` resolves from the command working directory. The schema was checked without reading these illustrative source files:

```toml
version = 1
source_key = "recorder-btc"
source_kind = "recording"
root = "./recordings"
files = ["trades.csv"]
max_files = 1000
data_type = "trades"

[identity]
exchange = "binance"
market_category = "spot"
instrument_type = "spot"
symbol = "BTCUSDT"

[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-02T00:00:00Z

[specifications]
as_of = 2026-09-01T00:00:00Z
base_asset = "BTC"
quote_asset = "USDT"
quantity_type = "base"
```

| Field | Contract |
| --- | --- |
| `version` | Integer 1, default 1. |
| `source_key`, `root` | Required nonempty source identifier and source directory. Key reuse requires the same resolved root and source kind. |
| `source_kind` | `recording` (default), `purchased`, or `other`. |
| `files`, `patterns` | At least one nonempty selection. Entries are root-relative; no absolute entries or parent traversal. Patterns support segment globs and recursive `**`; expansion is bounded. |
| `max_files` | Default 1000; allowed 1–10000. Pattern traversal also stops after 100000 inspected entries. |
| `identity` | Required `exchange`, `market_category`, `instrument_type`, `symbol`. Types include spot, perpetual, future, option and equity. Type and category must agree. `UNSPECIFIED` is invalid. |
| `data_type` | Required `trades` or `order_book_l2`. |
| `interval` | Optional declared half-open interval; it is not inferred or verified from file contents. |
| `specifications` | Optional for known instrument raw registration; required for unknown instruments. Includes aware `as_of`, base/quote assets and `quantity_type` (`base`, `quote`, `contracts`). |
| `contract_value`, `contract_value_asset` | In `specifications`; required for contracts. Value is a positive finite decimal **string**. |
| `format_profile` | Optional top-level reference. Does not validate/activate the profile or establish processing readiness. |

A source selection is sorted and frozen for an import run; symlink resolution must remain within the source root. Full identity/specification declarations are the user's responsibility. There is no automatic filename-derived venue, unit, interval or contract interpretation. See [external workflow and readiness](../workflows/acquisition/external-data.md).

See the [unified Forge workflow](../workflows/acquisition/forge.md) for managed builds, manually authored profiles, historical registration declarations, namespace mappings and dataset-registration recovery.
