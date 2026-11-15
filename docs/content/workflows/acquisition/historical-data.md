# Acquire historical archives

Use a saved TOML recipe to select a source, instrument or archive scope, data type, and time interval. Public historical coverage varies by provider, instrument and date. A supported request can legitimately find no files; neither current instrument listings nor a successful search prove complete historical coverage.

## Supported sources

This table describes the current archive adapters, not the separate current-market collection matrix.

| Source | Instrument type / market category | Data | Scope and discovery |
| --- | --- | --- | --- |
| Binance | spot/spot; perpetual or future/linear or inverse | `trades` | Native instrument; paginated daily S3 listing. |
| Bybit | spot/spot; perpetual or future/linear or inverse | `trades`, `order_book_l2` | Native instrument; daily directory listings. |
| Bybit | option/option | Both | `base_coin`, such as BTC; list-files requests split into at most seven calendar days. Not an individual option contract. |
| Bitget | spot/spot; perpetual/linear or inverse; future/inverse | Both | Native instrument; historical symbol lookup and availability requests in seven-day windows. |
| OKX | spot/spot | Both | Native spot instrument; daily historical queries. |
| OKX | perpetual or future/linear or inverse | Both | Explicit `family`, such as BTC-USD; not a contract symbol. |
| OKX | option/option | Both | Explicit `family`, or `base_coin` expanded to currently advertised families. |
| Gate.io | spot/spot; perpetual or future/linear or inverse | Both | Native instrument; probes monthly trade or hourly book candidates for existence. |

Binance L2/options and Bitget options/linear dated futures are not supported by these adapters. Gate.io dated-future archive routes are implemented as candidates; that does not establish that any particular archive exists. Its current-market metadata support is narrower. OKX depth uses the 5000-level historical module. Family bundles remain family-scoped files, not automatically identified concrete contracts.

Use explicit historical scopes for delisted instruments/families. The archive adapters do not require them to appear in today's instrument list. OKX option `base_coin` expansion can only discover currently advertised families; use an explicit historical family when appropriate. See [scope configuration](../../configuration/acquisition.md#scope-and-provider-options).

## 1. Create and validate a recipe

These commands create a template, open the configured editor, and validate the saved structure:

```bash
marketforge acquisition new september
marketforge acquisition edit september
marketforge acquisition validate september
marketforge acquisition list
```

`new` creates `data/.config/acquisition/september.toml` and refuses to overwrite it; it does not open an editor. `edit` opens one and validates after saving. Select dates and scope using the [complete TOML example](../../configuration/acquisition.md#complete-example). `validate` checks schema, timezone-aware dates and required fields. Source support and availability are checked during mapping, not structural validation.

## 2. Inspect cached knowledge or discover archives

A cache-only plan needs both flags:

```bash
marketforge acquisition plan september --testing --cache-only
marketforge acquisition plan september --testing --cache-only --verify-local --limit 20
```

Before discovery, a request may be unresolved. Planning will not contact a provider to fill the gap. It reports a bounded archive sample, not a total download-size estimate. `--verify-local` hashes only that sample without changing registry status.

To discover availability and record it:

```bash
marketforge acquisition map september --testing
```

Mapping lists source archive identities and locations, records a completed scan when discovery succeeds, and produces a run report. Gate.io mapping includes existence probes; listing/metadata requests are still network work even though no archive body is downloaded. Successful empty results are cached. Observed nominal interval bounds and gap diagnostics are not event-level completeness checks.

## 3. Download or reuse local copies

```bash
marketforge acquisition download september --testing
marketforge --json acquisition download september --testing --silent
```

Download resolves the same request, maps when necessary, and acquires its discovered files. Matching completed copies are checked by size and SHA-256 before reuse. A changed upstream revision gets a new managed copy; completed files are not overwritten. Files are downloaded whole: the requested interval selects overlapping archives, not a crop of their contents.

For a deliberate rediscovery or lower request rate:

```bash
marketforge acquisition download september --testing --refresh --requests-per-second 0.5
```

`--refresh` bypasses discovery freshness; it is not a forced redownload of valid local bytes. It cannot detect an upstream revision that exposes no revision metadata. Unknown or expired locations are renewed when a usable local copy cannot be recovered or reused.

## 4. Inspect the result

```bash
marketforge storage list --testing --exchange bybit --instrument BTCUSDT --data-type trades
marketforge storage coverage --testing --exchange bybit --instrument BTCUSDT --gaps
marketforge runs list --limit 10
```

Use the returned local archive ID and run UUID with `storage show`, `runs report`, and `runs logs`; see [management and recovery](managing-acquisitions.md). Progress uses stderr. The final result and report location remain available with `--silent`; global `--json` emits a structured stdout result.

## If acquisition is incomplete

Inspect request outcomes and individual archive events before retrying. Unsupported scope is different from an empty successful search. Partial scans are not reusable coverage. A retry can reuse valid files and recover durable publication reservations, but it does not resume an incomplete HTTP body at its previous byte offset. Provider cooldowns and retry budgets are respected; repeated retries are not a way to bypass them.
