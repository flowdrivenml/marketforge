# Supported historical sources

Use `mf acquisition sources | cpout` to see which combinations the installed archive adapters accept. Use `mf --json acquisition sources | cpout` for the same capability rows in JSON. These are historical archive routes. Availability depends on the exact scope, date, and upstream response. Use the [acquisition guide](../workflows/acquisition/historical-data.md) to map a recipe.

| Exchange ID | Instrument type | Market category | Trades | L2 depth | Scope kind |
| --- | --- | --- | --- | --- | --- |
| `binance` | `spot` | `spot` | Yes | — | `instrument` |
| `binance` | `perpetual`, `future` | `linear`, `inverse` | Yes | — | `instrument` |
| `bybit` | `spot` | `spot` | Yes | Yes | `instrument` |
| `bybit` | `perpetual`, `future` | `linear`, `inverse` | Yes | Yes | `instrument` |
| `bybit` | `option` | `option` | Yes | Yes | `base_coin` |
| `bitget` | `spot` | `spot` | Yes | Yes | `instrument` |
| `bitget` | `perpetual` | `linear`, `inverse` | Yes | Yes | `instrument` |
| `bitget` | `future` | `inverse` | Yes | Yes | `instrument` |
| `okx` | `spot` | `spot` | Yes | Yes | `instrument` |
| `okx` | `perpetual`, `future` | `linear`, `inverse` | Yes | Yes | `family` |
| `okx` | `option` | `option` | Yes | Yes | `family`, `base_coin` |
| `gateio` | `spot` | `spot` | Yes | Yes | `instrument` |
| `gateio` | `perpetual`, `future` | `linear`, `inverse` | Yes | Yes | `instrument` |

## Historical scopes

| Kind | Selects | Example |
| --- | --- | --- |
| `instrument` | One provider-native instrument | Bybit `BTCUSDT` |
| `family` | An archive family used by the provider for derivatives | OKX inverse dated futures `BTC-USD` |
| `base_coin` | Options grouped or resolved by their underlying coin | Bybit options on `BTC` |

Explicit historical instrument and family scopes can find delisted instruments absent from today's catalogue if their archives are still published. Use the exact identifier expected by the historical source. See [scope and provider options](configuration/acquisition.md#scope-and-provider-options).

## Discovery by provider

### Binance

The adapter lists daily trade files under the native instrument's paginated S3 prefix. It has separate spot, linear futures and inverse futures routes. Historical L2 and options are unsupported.

### Bybit

Spot and non-option derivatives use instrument directory listings for trades and depth. Options use a base coin for daily archive discovery with queries of at most seven calendar days. This window bounds individual queries. Provider history and access determine which files are returned.

### Bitget

Spot discovery resolves compact symbols through the historical symbol endpoint. Supply the explicit display symbol, such as `BTC/USDT`, if lookup is ambiguous. Availability queries use windows of up to seven UTC+8 calendar days. Options and linear dated futures are unsupported.

### OKX

Spot discovery uses native symbols such as `BTC-USDT`. Derivatives use a family identifier. Family bundles can stay family-scoped in managed storage. Options accept an explicit family or expand a base coin using currently advertised option families. Choose an explicit historical family when the provider no longer advertises it. The adapter queries one archive day at a time, using UTC+8 for trades and UTC for the historical 5000-level depth module.

### Gate.io

The adapter probes monthly trade candidates and hourly L2 candidates for existence, using separate spot, USDT derivatives and BTC derivatives routes. The [L2 feed option](configuration/acquisition.md#scope-and-provider-options) selects incremental or snapshot archives. Dated futures use the same candidate construction as the corresponding perpetual market. Only a successful probe establishes that a candidate archive was found.
