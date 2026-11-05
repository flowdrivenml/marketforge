# Markets, dated contracts, and terminal views

The public command remains `marketforge`. No filters means all 14 supported
exchange/market scopes; repeat `--exchange` and `--market` to select subsets:

```sh
marketforge markets sync --testing
marketforge markets refresh --testing
marketforge markets sync --exchange bybit --testing
marketforge markets sync --market linear --testing
marketforge markets refresh --exchange okx --market inverse --testing
marketforge markets list --exchange okx --market inverse --sort turnover --limit 30 --testing
marketforge markets show okx.inverse.BTC-USD-261127 --testing
marketforge markets rank list --testing
marketforge markets rank show SNAPSHOT_ID --testing --limit 50
marketforge markets rank show SNAPSHOT_ID --testing --excluded --all
marketforge --json markets list --testing
marketforge --json markets rank show SNAPSHOT_ID --testing
```

Run sync first after upgrading: older completion records do not certify the new
futures-aware universe. The collection version is `markets-futures-v3`. Examples
of dated symbols above reflect a live check on 2026-11-05, not permanent listings.

## Supported scopes and metadata

| Exchange | Spot | Linear derivatives | Inverse derivatives |
| --- | --- | --- | --- |
| Binance | Spot | Perpetual + dated | Perpetual + dated |
| Bybit | Spot | Perpetual + dated | Perpetual + dated |
| OKX | Spot | SWAP + FUTURES | SWAP + FUTURES |
| Gate.io | Spot | Perpetual + delivery | Perpetual only |
| Bitget | Spot | Perpetual + delivery when listed | Excluded |

Options remain excluded. A current listing does not promise expired-contract
coverage. Absence does not delete or delist a retained instrument. No archive
family/bundle is created as a concrete contract. Instrument type (`spot`,
`perpetual`, `future`) and market category (`spot`, `linear`, `inverse`) are separate.

Classification uses contract metadata and documented endpoint context, not date
suffix guesses. Binance includes its documented `TRADIFI_PERPETUAL` enum. Bybit
filters recognized cross-category rows: `baseCoin` queries explicitly return
linear and inverse contracts regardless of the requested category. Unknown types
fail rather than becoming successful empty discoveries. OKX requires both SWAP
and FUTURES requests, checks returned instrument types against endpoint context,
and distinguishes contract families from instrument IDs. Gate delivery is USDT
settled; the documented delivery API does not establish inverse delivery support.
Gate perpetual contract metadata uses sequential limit/offset pagination. Repeated
pages and repeated Bybit cursors fail the scope. Bitget's V3 `type=delivery` is
supported without assuming that any such linear contracts are currently listed.

Listing/expiry nanoseconds, settlement, native status, tick/quantity constraints,
quantity denomination and available contract values persist in existing catalog
columns. Missing specifications remain missing. Only futures receive expiry;
perpetual placeholder delivery times do not become fictitious expiries. Previously
known launch/expiry values survive missing metadata in later future responses.

Existing short references `exchange.market.symbol` remain valid where unambiguous.
Collisions use `exchange.market.instrument_type.symbol`; ambiguous short lookups
fail explicitly. Rankings and standalone concrete streams accept that qualified
identity. Older published rankings are not rewritten.

## Global scheduling, completeness, and cancellation

The existing bounded async scheduler was reused. There are at most
`min(3, http.max_concurrent)` scope workers. Admission interleaves exchanges before
revisiting the same upstream. Clients are initialized serially, once per exchange,
and share the existing conservative 0.2 requests/second budget, connection pool,
retries and cooldown across that exchange's scopes/products. Dependent pagination
and endpoint sequences stay sequential. No task is created per instrument.

Each worker owns its database connection; transactions remain short and do not
span network calls. One `WorkspaceOwnership` encloses the CLI collection operation.
Durable source outcomes distinguish queued, running, complete, failed and cancelled
scopes, with accurate collected counts and per-type counts. Independent failures
continue and produce exit 1. Cancellation stops admission, joins workers, records
not-started scopes honestly, renders the final result and exits 130.

Migration **0008_instrument_sync_members.sql** adds a foreign-key constrained
membership relation between metadata syncs and observed instruments. Membership
writes share the instrument-registration transaction. Only successfully completed
scope memberships certify a statistics selection; partial/cancelled listings do
not establish a current universe. This preserves historical instruments without
requiring tickers for contracts absent from a successful current listing. Empty
current selections can complete with zero observations.

Retained observations are persisted as they arrive, but the latest-value cache is
published only after all endpoints for that scope succeed, atomically with the
scope's completion record. A failed later FUTURES
request cannot freshen the browsing cache with only SWAP values. Successful scopes
remain usable when another scope fails; rankings inspect scope completeness rather
than confusing a partial global run with failure of every source.

## Units, precision, and rankings

Statistics remain native Decimal/NUMERIC values. USDT/USDC are not silently USD.
Native turnover is preferred and never multiplied by contract value again. Exact
contract-value arithmetic is used only where the native contract volume requires
it. OKX linear base-volume times last-price turnover remains explicitly estimated.
OKX family routing suffixes such as `USD_UM` are not currency denominations: those
families use the separately supplied underlying (`uly`). Existing plain USDT/USDC
families retain their denomination. Funding is missing for dated contracts.

Stored spot currency valuation is now available in version-2 ranking policies; see
[the conversion guide](ranking/README.md). Legacy manual-only policies remain compatible.
Existing comparability, freshness,
estimate handling, policy conversion requirements and immutable publication rules
remain. Ranking enumeration now filters in SQL and uses 1,000-row keyset pages;
there is no global 10,000-row fetch before policy filtering. Display limits never
limit the evaluated ranking universe. Unknown/currently unlisted retained records
can still cause strict policies to reject incomplete/stale inputs rather than
claiming a complete statistical universe.

The metadata decoded-body allowance remains **32 MiB**, including enforcement on
decoded HTTP content. A dedicated size exception becomes an actionable provider
message stating the limit, without printing URLs, query secrets or arbitrary
exception contents.

## Presentation contracts

Default markets and ranking commands render focused shared Tokyo Night views.
Lists show native units, estimates, missing values, `Not collected` versus `Stale`
versus `Fresh`, filters and displayed/truncated counts. Details group identity,
contract specifications, statistics and timestamps. Small nonzero prices use
scientific notation where useful; detailed values retain Decimal precision.
Turnover sorting remains grouped by native denomination, not a currency conversion.

Ranking tables use stored positions and actual snapshot comparison currency.
Full snapshot IDs remain available. Human show defaults to 50 ranked rows and a
grouped exclusion summary. `--excluded` adds individual exclusions; `--all` removes
human row limits. JSON show always returns the complete snapshot/entries, even
with a human `--limit`; existing fields and string Decimal serialization remain.

Collection uses one 4 Hz Live renderer on interactive stderr, with elapsed time,
active scopes, terminal counts and six recent outcomes. Redirected stderr receives
one concise transition per terminal scope, no cursor animation. `--silent` hides
progress but retains the final result and failures. Global `--json` emits one
structured stdout result without Rich/progress contamination. Existing explicit
color settings remain respected. Rendering failures cannot alter collection writes.

## Verification and limits

Deterministic tests use controlled events/transports, plus real guarded
`marketforge_test` connections with rollback or UUID-scoped cleanup. Coverage
includes actual independent exchange overlap, worker bounds, shared client and
excessive-cooldown admission, cancellation, connection ownership, membership,
partial pagination, exact ticker type matching, failed-cache publication,
10,005-record enumeration, qualified identities, terminal modes, row limits and
complete ranking JSON. No production access, bulk archive downloads, Rust jobs,
or end-to-end normalization are involved.

Bounded public checks on **2026-11-05** parsed current metadata and one selected
dated contract's statistics per supported populated derivative scope:

| Scope | Dated contracts observed in bounded listing | Selected statistics |
| --- | ---: | --- |
| Binance linear | 4 | BTCUSDT_261225; native USDT |
| Binance inverse | 10 | BTCUSD_261225; contract-value USD |
| Bybit linear, BTC-only | 8 | BTCUSDT-06NOV26; native USDT |
| Bybit inverse, BTC-only | 2 | BTCUSDH27; native USD |
| OKX linear FUTURES | 275 | BTC-USD_UM-261127; estimated USD |
| OKX inverse FUTURES | 12 | BTC-USD-261127; contract-value USD |
| Gate linear delivery | 30 | BTC_USDT_20261113; native USDT |
| Bitget linear | 0 | No dated contract available to verify statistics |

Counts are observations, not fixed expectations. Bybit's BTC-filtered responses
had no remaining cursor; these are not full exchange counts. Other listing counts
refer to the queried endpoint response at that time. Gate inverse dated futures
and Bitget inverse were not probed or invented. Bitget linear delivery parsing is
fixture/documentation verified; current dated statistics could not be live checked.

A real CLI sync and refresh for `okx.inverse.BTC-USD-261127` persisted one contract
and one observation in testing and exercised human/JSON output and stored details.
The entire unfiltered global command was verified deterministically over all 14
scopes; no exhaustive live global synchronization is claimed.

Official sources: [Binance exchange information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information),
[Binance maintained enum changelog](https://github.com/binance/binance-connector-java/blob/master/clients/derivatives-trading-usds-futures/CHANGELOG.md),
[Bybit instrument metadata](https://bybit-exchange.github.io/docs/v5/market/instrument),
[OKX instruments/tickers](https://www.okx.com/docs-v5/en/),
[Gate delivery](https://www.gate.com/docs/developers/apiv4/en/delivery/),
[Gate perpetual metadata pagination](https://www.gate.com/docs/developers/apiv4/en/futures/),
and [Bitget V3 instruments](https://www.bitget.com/legacy-docs/uta/public/Instruments).

Final quality checks used the repository-root `.venv/`: **359 maintained Python
tests passed, no skips** (including real testing-database integration tests),
Ruff lint/format checks passed, and strict Pyright reported zero errors/warnings.
Testing credentials were loaded through the guarded literal environment-file
loader and never sourced as shell code or printed. The only new migration was
0008; existing migration files, production, Rust, historical processing and
unrelated checkout changes were left untouched.

## Asset and native-symbol inspection filters

```sh
marketforge markets list --testing --market spot --match 'ETH*'
marketforge markets list --testing --base-asset ETH --quote-asset USDT
marketforge markets list --testing --base-asset ETH --match '*BTC'
```

`markets list` and `markets rank show` share `--base-asset`, `--quote-asset` and
`--match`. Both asset selectors are exact, case-insensitive metadata comparisons.
All filters combine with AND. `--match` is a case-insensitive glob over the full
native symbol: `*` matches zero or more characters and `?` one character. Quote
patterns in the shell so it cannot expand them before MarketForge receives them.
SQL `%` and `_` are literal, as are brackets and backslashes; patterns are escaped
and bound as query parameters. `--instrument` keeps its existing exact,
case-sensitive native-symbol behavior.

Catalogue filters execute in SQL before sorting and LIMIT, rather than filtering
a previously fetched page. The instrument's quote asset is independent of any
ranking comparison currency: `ETH-USDT-SWAP` has quote asset USDT, but its full
symbol does not match the suffix glob `'*USDT'`.
