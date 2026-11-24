# Discover and inspect instruments

## Synchronize the catalogue

Synchronization is a network and registry-writing operation. For a small selection:

```bash
mf markets sync --exchange bybit --market spot --instrument BTCUSDT --testing
```

For a broader scope, omit `--instrument`; repeat exchange and market options for subsets:

```bash
mf markets sync --exchange bybit --exchange okx --market linear --testing
```

Exchange and market filters select supported combinations. The exact symbol is applied within each provider scope, whose endpoints can still return full pages. With neither, collection visits all 14 scopes. An unsupported-only selection fails. Independent scope failures do not stop other scopes; inspect the final per-source outcomes.

| Provider | Spot | Linear | Inverse |
| --- | --- | --- | --- |
| Binance | Spot | Perpetual and dated futures | Perpetual and dated futures |
| Bybit | Spot | Perpetual and dated futures | Perpetual and dated futures |
| OKX | Spot | SWAP and FUTURES | SWAP and FUTURES |
| Gate.io | Spot | Perpetual and delivery | Perpetual only |
| Bitget | Spot | Perpetual and delivery when listed | Not collected |

Options are excluded from current markets collection. A dated-contract parser does not guarantee a currently listed contract. Expired/historical records remain stored; current endpoints do not establish an exhaustive expired-contract catalogue. Absence from sync does not automatically delete or delist a retained record.

## Browse stored instruments

```bash
mf markets list --testing --exchange bybit --market spot --limit 20
mf markets list --testing --base-asset ETH --quote-asset USDT --match 'ETH*'
mf --json markets list --testing --exchange okx --market inverse
mf markets show bybit.spot.BTCUSDT --testing
```

Asset filters are exact and case-insensitive. `--instrument` matches an exact native symbol; `--match` is a case-insensitive full-symbol glob supporting `*` and `?`, not regular expressions. Quote it in the shell. `%`, `_` and bracket expressions are literal. Filters combine with AND. The list default is 50 rows, bounded to 1–10000; JSON lists obey the limit and report truncation.

## Identity and contract details

Market category (`spot`, `linear`, `inverse`) and instrument type (`spot`, `perpetual`, `future`) are distinct. Use the reference returned by `list`, ordinarily `exchange.market.symbol`. When types collide on the same native symbol, use `exchange.market.instrument_type.symbol`. An ambiguous short reference fails. The separate [manual catalogue](../../reference/cli/catalog.md) uses `exchange.instrument_type.market_category.symbol`. Preserve punctuation such as the hyphens in an OKX native symbol. Do not derive identity by splitting symbols or guessing a date suffix. A policy's explicit instrument references must match the catalogue's canonical returned references.

`show` groups identity, native status, launch/expiry when supplied, assets/settlement, tick size, quantity step and constraints, quantity denomination, and contract value/unit. Missing specifications stay missing. Expiry is supplied for dated futures, not fabricated from perpetual placeholder timestamps. Retained inactive/expired entries can still appear in browsing; they are not a promise that refresh will collect them.

An instrument outside the chosen scope remains stored. Its catalogue status and its latest observation are independent. Current collection support, manual registration, [historical archive acquisition](../acquisition/index.md), and [normalization profile coverage](../normalization/index.md) each have separate contracts.

Next, [retrieve current statistics](market-statistics.md) for the successfully synchronized selection. A symbol-filtered sync certifies that selection; it is not a full-scope sync for an unfiltered refresh.

See the [unified Forge workflow](../acquisition/forge.md) for managed builds, manually authored profiles, historical registration declarations, namespace mappings and dataset-registration recovery.


## Historical authority during initialization

`mf init --testing` now appends historical specification and family evidence after
catalogue synchronization, before ranking publication. Existing downloaded bytes
remain in place. See [historical inference policy](../../reference/configuration/markets.md#historical-metadata-inference)
for evidence precedence, BTC reference constraints, provenance, incomplete family
populations, cached archive discovery and options limitations. Initialization
reports metadata readiness separately from exchange collection failures.
