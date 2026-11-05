# Turnover activity scope and time evidence

Rankings compare monetary turnover, not raw volume counts. Conversion remains a
separate reference-price valuation: a reported native turnover does not become a
volume-derived estimate merely because it is converted to another currency.

## Bitget Reality tokens

Bitget's [instrument endpoint](https://www.bitget.com/docs/catalog/market-market-data/market-instruments)
and [rToken guide](https://www.bitget.com/docs/uta/reality-trading-guide) identify
spot Reality tokens with `isReality = "yes"`. An `R` prefix and `isRwa` are not
substitutes for that flag. The [ticker documentation](https://www.bitget.com/legacy-docs/uta/public/Tickers)
defines `platformTurnover24h` as platform turnover for rTokens. Broader
`turnover24h` is not an exchange-local ranking measure for these products.

The mapper uses `platformTurnover24h` for these instruments. Zero is valid;
missing, malformed, nonfinite or negative values remain missing with an explicit
reason. It never falls back to broader turnover or estimates platform base volume
by dividing turnover by price. `volume24h` is omitted from the platform-local
base-volume column for Reality instruments; its original value remains in raw
provenance. Ordinary instruments with `isReality = "no"` keep native
`turnover24h` and `volume24h`. Unknown spot classification requires a metadata
refresh rather than guessing the scope. Derivatives do not use the spot flag.

Collection now carries the retained instrument API metadata into ticker mapping.
Mapping `current-metrics-v3-activity-scope` records the selected field, activity
scope, base-volume scope, calculation/estimate semantics, and timestamp basis in
`catalog.metric_observations.raw_json.interpretation`. The original ticker and
instrument specification remain separately retained. This reuses the existing
JSON provenance contract; no SQL migration is required.

New builds reject legacy Bitget spot observations when retained/current metadata
identifies a Reality token, or the old ticker explicitly contains conflicting
platform and broader turnover. This check does not invalidate other exchanges or
ordinary Bitget observations indiscriminately. Refresh metadata and statistics
to replace the input selection; old observations and published snapshots are not
rewritten. Published entries continue to refer to their original observations.

## OKX and source timestamps

OKX's [ticker endpoint](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-tickers)
reports derivative `vol24h` in contracts and `volCcy24h` in base currency. It does
not supply the quote-currency accumulated turnover used here. Linear notional is
therefore `volCcy24h * last`, explicitly a volume-derived estimate, never exact
accumulated monetary turnover. Native monetary values elsewhere are not multiplied
by a contract value a second time. No additional undocumented scope differences
were inferred for other providers.

Binance's [24-hour ticker](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
supplies `openTime` and `closeTime`: these actual bounds are preserved. A recent
fetch does not replace an old `closeTime`. Where exact bounds are not supplied,
the existing 24-hour approximation is explicitly labelled inferred from source
time or fetch time. Ticker generation time, interval end and fetch time remain
distinct fields/concepts. A separate last-trade timestamp is not invented: the
interpretation identifies the ticker-time or fetch-time proxy used for FX source
prices. Existing FX freshness and leg-skew checks continue to apply.

## Current-priority policy and inspection

Only `data/.config/markets/rankings/all-markets.toml` has its turnover-window
tolerance changed to 900 seconds. It allows volume-derived estimates (the saved
file already specified `estimates = "allow"` before this change). Its prior bytes
are saved as `all-markets.toml.before-activity-scope.bak` beside it. General age,
conversion-price age and conversion leg-skew limits are unchanged. This is a
current-priority policy choice, not proof that source intervals are identical.
Other policies can still exclude estimates and require tighter alignment.

Instrument details show field/scope, source/fetch times and interval basis/bounds.
Ranking tables mark estimates, summarize their count, and expose native evidence
through `--conversion INSTRUMENT_REFERENCE` and full JSON. Old observations show
legacy/missing evidence honestly. Window incompatibility diagnostics include the
observed end, comparison end and configured tolerance.

```sh
marketforge markets show bitget.spot.RSPYUSDT --testing
marketforge markets rank validate all-markets
marketforge markets rank build all-markets --testing
marketforge markets rank show SNAPSHOT_ID --testing --conversion bitget.spot.RSPYUSDT
marketforge markets rank show SNAPSHOT_ID --testing --conversion okx.linear.AAVE-USDT-SWAP
marketforge markets rank show SNAPSHOT_ID --testing --excluded
marketforge --json markets rank show SNAPSHOT_ID --testing
```

## Verification: 2026-11-05

Ruff lint/format and strict Pyright pass. The complete maintained Forge suite
passed **416 tests with zero skips**, including guarded real-database tests.
Regression fixtures cover the supplied RSPY values, zero/invalid/missing platform
turnover, metadata classification, legacy mapping compatibility and immutable
snapshots, OKX estimate arithmetic/policy treatment, actual Binance bounds,
899/900/901-second alignment, independent FX age rejection, and human/JSON evidence.
Existing conversion, arbitrary bridge and Gate pagination tests still pass.

Live verification used the guarded testing database only. Bitget spot metadata
sync collected 3,677 records, followed by a successful targeted RSPY refresh.
The bounded concurrent global refresh completed all 14 supported scopes:

| Exchange | Spot | Linear | Inverse |
| --- | ---: | ---: | ---: |
| Binance | 1372 | 804 | 30 |
| Bybit | 521 | 927 | 26 |
| OKX | 1210 | 800 | 27 |
| Gate.io | 2186 | 30 | 0 |
| Bitget | 3664 | 872 | unsupported |

These are collected observation counts, not a claim of complete historical
coverage or exhaustive current metadata availability. No additional global
metadata synchronization was performed; statistics used certified stored scope
membership. Missing listed tickers remain explicit in collection outcomes.

Published snapshot: `45ff5a9f-7c65-483c-8a8d-08942e4a9412`.
It includes 12,460 of 12,469 instruments, including 800 volume-derived estimates.
Nine Binance observations remain excluded for source-window differences above
900 seconds; freshly fetching them did not make their source timestamps newer.
The top three entries were Binance linear BTCUSDT, Binance linear ETHUSDT and OKX
linear BTC-USDT-SWAP (the latter explicitly estimated).

Independent reproduction from original retained ticker inputs verified:

- RSPYUSDT, position 5952, observation 45886: `platformTurnover24h = 55046.2049`
  USDT; score `55046.2049`. Broader `turnover24h = 34272520633.1351` remains raw.
- AAVE-USDT-SWAP, position 285, observation 50376:
  `358988.16 AAVE * 132.28 USDT/AAVE = 47486953.8048 USDT`, flagged estimated.
- Both use identity currency conversion; volume estimation is distinct from FX.
- Hashes of all seven pre-existing published snapshot headers, entries and
  conversion records remained unchanged after refresh/build.

Human instrument details, ranking details and full JSON were exercised via the
CLI. No Rust execution, historical downloads, normalization or production access
occurred. Live availability and prices can change; these results do not establish
execution-time notional accuracy for estimates or identical statistical intervals.
