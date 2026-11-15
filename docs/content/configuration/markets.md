# Markets and ranking configuration

Metadata/statistics collection uses command-line scope filters and application `[http]` settings; there is no separate per-provider markets recipe required by `markets sync` or `refresh`. The testing database connection is supplied through the [guarded testing key](../workflows/acquisition/index.md#prerequisites), not an invented markets TOML connection field.

Collection uses a shared conservative 0.2 requests/second rate per exchange and at most `min(3, http.max_concurrent)` scope workers. The same-exchange scopes share the client, retry budget and cooldown. Do not assume `http.max_concurrent` means one independent worker per instrument.

## Complete turnover policy

Create `activity` with `markets rank new activity`, then edit `data/.config/markets/rankings/activity.toml`. This complete example explicitly permits reported exclusions and limits the ranking universe to Bybit; it makes no claim that required observations/prices are currently stored:

```toml
version = 2
metric = "turnover_24h"
comparison_currency = "USDT"
exchanges = ["bybit"]
markets = ["spot", "linear", "inverse"]
instruments = []
max_age_seconds = 3600
max_window_skew_seconds = 300
estimates = "exclude"
allow_partial_universe = true
manual_preferences = []

[conversion]
automatic = true
valuation = "reference_price_turnover"
max_price_age_seconds = 3600
max_leg_skew_seconds = 300
max_hops = 2
bridge_assets = []
prefer_same_exchange = true
allow_cross_exchange = false
allow_mixed_exchanges = false
source_exchanges = ["bybit", "binance", "okx", "gateio", "bitget"]
equivalent_currencies = []
```

Validate with `marketforge markets rank validate activity`; building still requires fresh stored inputs and writes a snapshot. Policy names use the managed `.toml` library; `--config` does not select a ranking policy.

## Policy fields and defaults

| Field | Default / allowed values |
| --- | --- |
| `version` | Integer `1` or `2`; omitted means 1. Version 2 enables automatic conversion by default for turnover; version 1 is manual-only by default. |
| `metric` | `turnover_24h` (default) or `volume`. Other statistics cannot currently be ranking metrics. |
| `exchanges` | Empty means all supported exchanges; explicit codes restrict the universe. |
| `markets` | `['spot', 'linear', 'inverse']`; at least one category required. Options excluded. |
| `instruments` | Empty means no reference filter; otherwise exact canonical references returned by the catalogue, not native-symbol globs. |
| `max_age_seconds` | 3600; range 1–604800. Checks both fetch time and source-window end. |
| `max_window_skew_seconds` | 300; range 0–86400. Controls source-window alignment/future-time tolerance; volume also requires a compatible 24-hour duration. |
| `comparison_currency` | `USDT`; turnover only. Uppercase currency code, maximum 32 characters. |
| `estimates` | `exclude` or `allow`; controls volume-derived estimates, not whether FX reference valuation exists. |
| `allow_partial_universe` | `false`. Explicit true permits exclusions/partial-refresh inputs with retained reasons. At least one comparable entry is required. |
| `manual_preferences` | Empty list. Turnover reference priority order before descending score; no duplicates or references outside the universe. Rejected for volume. |

Filters select the ranking universe before display limits; the current policy has no base/quote/glob/type field for universe selection. Use explicit `instruments` when necessary. Display filters on `rank show` do not alter a published policy.

## Turnover conversion controls

| `[conversion]` field | Default / constraints |
| --- | --- |
| `automatic` | False in version 1, true in version 2 turnover policies. |
| `valuation` | Only `reference_price_turnover`. |
| `max_price_age_seconds` | Omitted inherits policy age; explicit 1–604800. |
| `max_leg_skew_seconds` | Omitted inherits window skew; explicit 0–86400. |
| `max_hops` | Integer 1 or 2 (default 2); at most one bridge. |
| `bridge_assets` | Omitted defaults to BTC/ETH; empty permits any connecting spot asset. Nonempty ordered allowlist, at most 16 unique uppercase codes. |
| `prefer_same_exchange` | Must be `true`. |
| `allow_cross_exchange` | `false`; true permits a complete path on another allowed exchange. |
| `allow_mixed_exchanges` | Must be `false`: two legs cannot mix venues. |
| `source_exchanges` | All five providers by default; nonempty unique list in preference order. |
| `equivalent_currencies` | Empty; explicit native currencies assumed 1:1 with comparison currency. Never automatic; do not include the comparison currency itself. |
| `manual_rates` | Empty map; each currency entry supplies positive finite decimal `rate`, timezone-aware `as_of` and nonblank `source`. Rate is comparison units per one native unit. |

Unknown fields, invalid currency codes, duplicate venues/currencies, unsupported hops and inconsistent permissions fail validation. Conversion preference is identity → manual → explicit equivalence → stored spot. Within eligible spot paths, same target exchange comes first, then fewer legs, configured source preference, newest weakest leg, configured bridge preference, and deterministic identity tie-breaks. A local bridge can beat a foreign direct pair.

Manual timestamps must represent actual observation time and pass freshness/future-time checks at build; stale overrides do not silently fall back. Legacy top-level `conversion_rates`, `conversion_as_of` and `conversion_source` remain supported. Do not repeat a currency in legacy and new manual-rate forms. An override for the comparison currency must be exactly 1.

Rates and scores retain exact-ratio evidence from decimal inputs and are rounded to 80 significant digits for storage; terminal abbreviation is not the calculation precision. [Rank inspection](../workflows/markets/rankings.md#conversion-and-volume-evidence) exposes evidence without revaluing an old result.

## Complete base-asset volume policy

Save this as `data/.config/markets/rankings/base-volume.toml`. It needs current observations with trustworthy base-volume scope/specification evidence:

```toml
version = 2
metric = "volume"
volume_unit = "base_asset"
group_by = "base_asset"
exchanges = ["bybit"]
markets = ["spot", "linear", "inverse"]
max_age_seconds = 3600
max_window_skew_seconds = 300
estimates = "exclude"
allow_partial_universe = true
```

Both `volume_unit` and `group_by` are required explicitly. Do not include `comparison_currency`, `[conversion]`, legacy rates or manual preferences: volume policies reject them. Automatic FX is not used. Each base asset has its own descending quantity order, with reference tie-breaks. See [volume semantics](../workflows/markets/rankings.md#choose-the-comparison).

## Standard presets

`markets rank update` only reads `turnover.toml` and/or `volume.toml` in this directory. It does not create them; `rank new` always creates a turnover template regardless of filename. To establish a volume policy, write and validate the appropriate volume TOML intentionally. Custom names use `rank build`, not `rank update`. The generated turnover template is strict (`allow_partial_universe = false`), unlike the permissive examples on this page.
