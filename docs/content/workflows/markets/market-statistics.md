# Retrieve and interpret market statistics

## Refresh a synchronized selection

After a successful matching [metadata sync](instrument-discovery.md), refresh statistics:

```bash
marketforge markets refresh --exchange bybit --market spot --instrument BTCUSDT --testing
marketforge markets show bybit.spot.BTCUSDT --testing
```

Refresh contacts public endpoints and writes retained observations. Browsing reads stored values only. A current successful metadata membership is required for each selected scope; older or incomplete syncs do not certify the universe. A broad successful sync can support a symbol-filtered refresh; a narrow sync does not certify an unfiltered refresh. Refresh does not automatically synchronize metadata.

A scope's browsing cache is published only when its required endpoint sequence succeeds. Individual observations can be retained before a later failure, so a failed scope may leave diagnostic observations without updating the displayed latest values. Successful independent scopes remain usable when others fail. Read collection status and per-scope outcomes rather than assuming every value was refreshed.

## What values mean

| Value | Meaning and availability |
| --- | --- |
| Last price | Native quote units per base unit, when supplied; not an execution guarantee or separate time series. |
| Base/quote volume | Rolling quantities in their stated asset; fields differ by provider and derivative type. Missing is not zero. |
| Contract volume | Contract count where mapped. Convert only using the applicable saved contract specification. |
| Turnover | Native monetary activity in the recorded denomination; may be reported, contract-value derived, or an explicitly flagged volume/last-price estimate. |
| Open interest | Optional current quantity with its explicit unit (base, quote or contracts); not comparable across units by bare number. |
| Funding rate | Optional signed raw rate when returned; not annualized or converted to a universal funding interval. Dated futures have no funding value. |

Spot does not universally expose funding/open interest. The mapper retains Bybit open interest in base units for linear and quote units for inverse, Gate derivative open interest in contracts, and available Bitget open interest in base units. Binance/OKX ticker mapping does not retrieve additional funding or open-interest endpoints. A field supported by the model need not be populated for every source.

## Provider-specific volume and turnover

| Mapping | Monetary interpretation |
| --- | --- |
| Binance spot/linear | Reported `quoteVolume`; `volume` is base volume. |
| Binance inverse | Native quote turnover if supplied; otherwise contract count × verified quote-denominated contract value. `baseVolume` remains base quantity. |
| Bybit spot/linear | `turnover24h` in quote; `volume24h` in base. |
| Bybit inverse | `volume24h` supplies quote turnover; `turnover24h` supplies base quantity. Do not treat the former as contract count. |
| OKX spot | `volCcy24h` quote quantity and `vol24h` base quantity. |
| OKX inverse | Contract `vol24h` × verified quote face value. |
| OKX linear | `volCcy24h` base quantity × `last`, explicitly estimated; not accumulated execution-time monetary turnover. |
| Gate.io | Reported spot `quote_volume` or derivative `volume_24h_quote`; available base/contract quantities remain separate. |
| Bitget | Ordinary products use `turnover24h`; Reality spot products use platform-local `platformTurnover24h`. |

Bitget Reality classification uses metadata `isReality`, not an `R` prefix. Broader turnover and volume are not substituted for missing platform-local data. Reality base volume remains missing rather than dividing turnover by last price. Unknown spot classification or incompatible legacy observations need metadata/statistics refresh, not guessed units. Raw values and interpretation evidence are retained.

USDT, USDC and USD are distinct. A contract's settlement currency alone does not establish turnover denomination. Reported native money is not multiplied by face value again. See [rankings](rankings.md) for explicit currency conversion and estimate controls.

## Timestamps and freshness

`fetched_at` is retrieval time. `source_observed_at` is the upstream ticker time when supplied. `window_start`, `window_end` and `window_basis` describe the statistic's interval, not the timestamp of every underlying trade.

Binance's supplied `openTime`/`closeTime` are retained. OKX/Bitget ticker generation timestamps support inferred preceding 24-hour windows. Without source time, the window is inferred from fetch time and labeled accordingly. A separate last-trade price timestamp is not invented: conversion evidence identifies the ticker/fetch-time proxy.

Stored browsing marks missing collection separately from freshness; `stale` is based on `fetched_at` older than one hour (or absent). This display rule is weaker than ranking freshness: ranking checks fetch time **and** source-window time, future timestamps, and inter-window alignment. Re-fetching an old source interval does not make it suitable for ranking.

These are retained current/rolling observations, not historical tick data or a backtest-ready point-in-time dataset.

## Filter, sort and inspect

```bash
marketforge markets list --testing --exchange okx --market linear --sort turnover --limit 30
marketforge markets list --testing --base-asset ETH --sort freshness --limit 20
marketforge --json markets show bybit.spot.BTCUSDT --testing
```

`identity` is the default sort. `turnover` sorts descending **within native denomination groups**, never by a hidden cross-currency conversion. `freshness` orders by stored measured/window-end time, not the one-hour fetch-time indicator. There is no `--instrument-type` option on `markets list`; use returned type metadata, exact symbols or qualified `show` references.

For stale/missing results, inspect the last collection's per-scope errors, synchronize changed metadata if necessary, and deliberately refresh the required scope. Do not convert missing values into zeros or loosen policy constraints without understanding the resulting comparison.
