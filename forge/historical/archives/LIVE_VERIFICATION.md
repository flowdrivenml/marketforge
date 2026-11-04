# Bounded mapping verification, 2026-11-04

Configuration: `data/.config/acquisition/test.toml`, 25 requests for
`[2026-11-01T00:00:00Z, 2026-11-04T00:00:00Z)`. Only the explicitly selected,
identity-verified `marketforge_test` database was accessed. No archive bodies
were downloaded and no existing acquisitions were deleted.

The original report `98385502-6c29-42e5-a1c5-137001eb292b` and its event log were
inspected alongside registry membership and source metadata. Rate-limited live
HTML/JSON discovery and existence probes then tested the fixes. Fresh discovery
for non-Bybit sources is recorded in run `b22bdc40-1545-4067-98cc-063fbf8e1cf4`.
That intermediate run exposed a heading case-normalization bug while the Bybit
fix was being developed; those failed scans established no reusable coverage.
Run `c4b10a93-df05-4fed-8b5e-7b2d3054c719` verified all six Bybit requests live
after correction and reused the other freshly mapped results. Final audit run
`35223296-2748-4b52-a5fd-dee3e34d28d3` reused all 25 corrected-version snapshots,
recording observed interval unions and request counts: 25 successful discovery
requests, including five empty, 126 distinct per-request observations, no failures.
These are discovery results, not a completeness certification.

| Exchange / market | Trades | L2 | Verification provenance |
| --- | ---: | ---: | --- |
| Bybit spot BTCUSDT | 3 | 3 | live correction run, then cache |
| Bybit linear BTCUSDT | 3 | 3 | live correction run, then cache |
| Bybit inverse BTCUSD | 3 | 3 | live correction run, then cache |
| Binance spot BTCUSDT | 3 | — | live refreshed run, then cache |
| Binance linear BTCUSDT | 3 | — | live refreshed run, then cache |
| Binance inverse BTCUSD_PERP | 3 | — | live refreshed run, then cache |
| OKX spot BTC-USDT | 3 | 2 | live refreshed run, then cache |
| OKX linear BTC-USDT family | 3 | 2 | live refreshed run, then cache |
| OKX inverse BTC-USD family | 3 | 2 | live refreshed run, then cache |
| Gate spot BTC_USDT | 0 | 70 | live probes, then cache; limitations below |
| Gate linear BTC_USDT | 0 | 0 | live probes, then cache; limitations below |
| Gate inverse BTC_USD | 0 | 0 | live probes, then cache; limitations below |
| Bitget spot BTC/USDT | 3 | 2 | live read-only POST, then cache |
| Bitget linear BTCUSDT | 7 | 2 | live read-only POST, then cache |

## Bybit

All three trade URLs returned 200 directly, with no redirect:
`public.bybit.com/spot/BTCUSDT/`, `/trading/BTCUSDT/`, and `/trading/BTCUSD/`.
Spot's title/h1 is `Directory listing for /spot/BTCUSDT/`; both derivative
headings omit the final slash. They use an unordered list. The former exact
heading comparison rejected legitimate derivative pages. Sanitized fixtures
retain representative November links and these actual heading structures.

Trade filenames are `BTCUSDT_2026-11-01.csv.gz` for spot and
`BTCUSDT2026-11-01.csv.gz` / `BTCUSD2026-11-01.csv.gz` for derivatives, repeated
for November 2 and 3. Book archives use
`2026-11-01_BTCUSDT_ob200.data.zip` (BTCUSD for inverse). Their daily UTC intervals
span the requested range. Safe redirect handling is fixture-tested; no redirect
was needed for the observed live URLs.

## Gate.io

[Official route documentation](https://www.gate.com/developer/historical_quotes)
distinguishes monthly `deals`/`trades`, hourly incremental `orderbooks`, and hourly
`orderbooks_slice` snapshots. For this request, the adapter probes the documented
incremental feed, retaining exact UTC hourly intervals. It does not substitute a
snapshot feed for an absent incremental feed.

The 70 spot-depth identities are distinct hourly partitions, not pagination or
deduplication errors. They span the requested interval with two missing candidate
hours: November 2 at 02:00 and 03:00 UTC. They must not be reduced to three files.
The report's interval-union check detects those interior gaps even though its
minimum and maximum bounds match the requested interval.

All three requested November monthly trade candidates returned 404. The four
bytes of a file were not fetched, and no reason for non-publication was established;
in particular, these observations do not establish unsupported products or complete
absence of trade data. All requested linear/inverse incremental-depth candidates
also returned 404. Separate bounded probes confirmed snapshot objects for all three
markets at `orderbooks_slice/202611/{symbol}-2026110100.gz`. Those snapshots are a
different feed and were not registered as incremental-depth replacements. No live
claim is made about snapshot availability beyond that probed hour.

A possible bucket-listing alternative was inspected and rejected: root GETs with
nine distinct symbol/market/dataset prefixes all returned the same first 1,000
`delivery_usdt/...` keys, empty Prefix/Marker, and `IsTruncated=true`. The endpoint
ignored the supplied filters. Following such a cursor or caching it as an empty
filtered result would be incorrect. Runtime discovery therefore retains bounded
per-object HEAD probes; no bucket pagination is involved. Unexpected statuses or
HTML/JSON error pages fail discovery rather than establishing empty coverage.

The empty incremental/trade results are successful *candidate interpretation*
with no matching objects observed, and availability/coverage remains limited or
unknown. They are not presented as proof that the exchange/product is unsupported.

## OKX

The observed spot files belong to BTC-USDT. Perpetual trade/depth filenames belong
to BTC-USDT-SWAP and BTC-USD-SWAP, with corresponding family metadata. Current
validation checks those identities even when an explicit `instId` is returned.
Dated futures cannot pass perpetual validation.

Each market's two depth files are dated November 1 and 2, with UTC intervals
`[Nov 1, Nov 2)` and `[Nov 2, Nov 3)`. November 3–4 is unrepresented. Three trade
files are dated November 1–3 in UTC+8, giving the nominal UTC envelope
`[Oct 31 16:00, Nov 3 16:00)`. The final eight hours of the requested UTC interval
are unrepresented. No live reason for the publication lag was established.

## Bitget

Returned `displayName` and filenames distinguish BTC/USDT spot from BTCUSDT
linear perpetuals. Linear trades returned:

- `BTCUSDT-BTCUSDT_20261101_001.zip` and `_002.zip`;
- `BTCUSDT-BTCUSDT_20261102_001.zip` through `_005.zip`.

These are seven distinct numbered partitions across two source dates. Their
metadata gives nominal daily UTC intervals `[Oct 31 16:00, Nov 1 16:00)` and
`[Nov 1 16:00, Nov 2 16:00)`. Intra-day partition boundaries are not supplied and
must not be invented. Duplicate filenames do not create additional observations.

Spot trades have one November 1 partition and two November 2 partitions:
`BTC/USDT-BTCUSDT_20261101_001.zip`, `_20261102_001.zip`, and `_20261102_002.zip`.
Both depth requests return November 1 and 2 files: spot uses
`BTC/USDT-BTCUSDT_1_20261101.zip`, linear uses
`BTCUSDT-BTCUSDT_3_20261101.zip`, with the corresponding November 2 names.
Their source timestamps give the same UTC+8 daily boundaries. Missing requested
tail intervals are reported; no assumption of one-file-per-day completeness is made.
No inverse Bitget or option requests were added.

## Reproduction

```console
uv run --project forge --locked marketforge acquisition map test --testing --refresh --requests-per-second 1
uv run --project forge --locked marketforge --json acquisition map test --testing
uv run --project forge --locked marketforge acquisition download NAME --testing
uv run --project forge --locked marketforge acquisition download NAME --testing --silent
```

The download commands are user verification examples, not commands run against
the full 25-request configuration during this task. Use a deliberately small
saved configuration for a bounded transfer. Mocked streaming/reuse/cancellation
and real-registry tests verify downloading without live bulk transfers.
