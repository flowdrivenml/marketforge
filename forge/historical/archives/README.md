# Historical acquisition

Use saved TOML configurations in `data/.config/acquisition/`:

```console
marketforge acquisition map september --testing
marketforge acquisition download september --testing --silent
marketforge --json acquisition download september --testing --refresh --requests-per-second 1
```

`NAME` and `NAME.toml` resolve through the existing managed configuration library.
`new`, `edit`, `list`, and structural `validate` retain their existing interfaces.
`--testing` is required. The only database connection key is
`MARKETFORGE_DATABASE_URL_TEST`, from the environment or the workspace's literal
`.secrets/postgres.env` file. Shell expressions are never executed. The connection
must explicitly name `marketforge_test`; the server database name and testing
identity are verified before acquisition mutations. Production has no fallback.
Apply numbered migrations through the existing migration runner before use.

`map` discovers identities and URLs without requesting archive bodies. `download`
performs mapping as needed and streams the resulting archives. `--refresh` bypasses
fresh discovery; otherwise fully completed searches are fresh for six hours.
Successful empty searches are cached. Adapter version changes invalidate reuse.
`--requests-per-second` overrides configured pacing, capped at the conservative
adapter budgets: Binance 2, Bybit 1, Bitget 0.5, OKX 2.5, Gate.io 1 request/second.
Every attempt and redirect consumes the shared budget. Connections, concurrency,
retry limits, timeouts, and server cooldowns use the shared HTTP client. Read-only
Bitget discovery POSTs explicitly permit retries; arbitrary POSTs do not.
A server cooldown exceeding the permitted wait permanently closes admissions
for that upstream client. Acquisition never recreates it to evade the cooldown.

Exit codes: 0 success, 1 partial/item failure, 2 setup/validation/ownership failure,
130 interruption. Progress goes to stderr. `--silent` suppresses progress while
retaining final results and failures. Global `--json` emits a single result object
on stdout, including request outcomes, file counts, and report/event locations;
setup errors follow the existing JSON-on-stderr contract. JSON mode suppresses
progress. Human results show failed requests and point to individual archive
outcomes. `queued` counts all enqueued files; the other counters record terminal
outcomes, rather than byte progress.

## Migrated capabilities

The legacy capability sets were inventoried before migration. Public data names
are `trades` (formerly `TRADE_TICKS`) and `order_book_l2`.

| Exchange | Instruments / category | Data | Selection and discovery |
| --- | --- | --- | --- |
| Binance | spot/spot; perpetual and future/linear or inverse | trades | explicit instrument; daily S3 listing with pagination |
| Bybit | spot/spot; perpetual and future/linear or inverse | both | explicit instrument; browsable daily listings |
| Bybit | option/option | both | base_coin; historical list-files endpoint, at most seven calendar days per call |
| Bitget | spot/spot; perpetual/linear or inverse; future/inverse | both | explicit instrument; historical symbol mapping and read-only POSTs, seven-day windows |
| OKX | spot/spot | both | explicit instrument; historical GET queries |
| OKX | perpetual and future/linear or inverse; option/option | both | explicit family; option base_coin expands advertised underlying families |
| Gate.io | spot/spot; perpetual and future/linear or inverse | both | explicit instrument; monthly trade / hourly book candidates, HEAD existence probes |

Options now explicitly use `market_category = "option"`; they are never
reinterpreted as linear/inverse. Bybit option archives describe base-coin content,
not individual contracts. The old advertised option-instrument path could not
resolve a browsable option root; it is rejected with a scope diagnostic.
OKX derivative requests require family scope because the current request model
has no separate instrument-family metadata. The old instrument-plus-family
request is represented by its actual upstream family selection. No instrument
symbol is silently treated as a family. Explicit historical families/instruments
are not checked against current instrument listings. For a delisted OKX option
family, use explicit family scope: base-coin expansion can only enumerate currently
advertised families, and a missing base-coin family produces a failure.

Binance L2/options and Bitget options/linear dated futures remain unsupported.
OKX L2 selects module 5 (5000 levels), preserving the old executable mapping and
correcting its misleading module-4 docstring. Gate.io futures candidates preserve
the old capability matrix; candidate existence is probed, not assumed.

## Cache and registry

Migration 0003 adds scan-to-archive membership. Legacy completed scans without
membership become partial and must be rediscovered; identities and files remain.
No applied migration was edited. Dataset ownership stays in `historical`.

Each search creates a running scan. Observations and membership persist in
transactions of at most 100 objects, with no network call inside a transaction.
Only after every discovery page succeeds is the scan marked complete. Failed or
cancelled scans cannot supply reusable coverage. Registry failures also prevent
completion. The last bounded batch and membership count determine completion;
duplicate objects do not inflate the result count.

A completed authoritative refresh supersedes overlapping completed scans for
that exact selection. Its membership is the new result, including an empty
result. Removed objects remain known identities but are absent from that result.
They are not globally marked unavailable: another scope may still observe them.
Failed refreshes do not invalidate previous completed searches. Broad scopes
retrieve their actual observed archive IDs through membership, rather than
assuming their scope keys equal those of concrete archive content.

Object identities exclude URL signatures and CDN hostnames. S3 object keys and
source-native file identities remain stable across URL renewal. Common S3/OSS
URL expirations are recorded separately. Downloading detects missing/expired
locations and refreshes that request's discovery before queueing transfers. This
is a selection refresh, not an entire-exchange scan. Unknown signature formats
cannot be dated locally; HTTP failures are reported and `--refresh` is available.

## Files, interruption, and recovery

A fixed number of workers uses a queue bounded at twice configured HTTP
concurrency. Discovery/cache pages are bounded; discovery response bodies are capped at 16 MiB. The worker count does not grow
with the number of archives. Disk writes, integrity reads, and publication use
joined background-thread calls so cancellation cannot leave a write running
behind cleanup. Bodies stream in 1 MiB chunks. No HEAD calls obtain file sizes.

Destinations beneath the raw root combine remote registry ID, random acquisition
ID, and a sanitized filename. Remote names cannot control directories. A revised
object always gets a new acquisition and destination when its upstream validator
changes; old bytes, checksums, and lineage remain intact. Reuse verifies actual
size and SHA-256 against the database. Missing/corrupt rows are marked accordingly
and receive a new copy. Without upstream revision metadata, verified registered
bytes remain the reusable acquisition; refresh cannot detect invisible revisions.

ZIP/gzip integrity and nonempty content are checked (ZIP directory limits: 10,000 members / 8 MiB), and HTML/error documents
are rejected. Plain delimited archives are also accepted. This is basic archive
validation, not market schema validation or normalization. Files are never
extracted. Stream body failures are reported per item; partial bodies are not
replayed automatically in the same HTTP attempt.

A database reservation precedes the transfer. After validation and fsync, actual
size/hash are committed to the downloading reservation **before** atomic,
no-replacement publication. Final registration then marks it complete. A crash
between these steps leaves a downloading row with enough durable information to
verify the final or temporary file, publish if needed, and finalize registration
without HTTP. This also covers publication followed by failed final registration.
Incomplete transfers without durable hashes are removed and restarted. A normal
cancellation cleans incomplete temporary files; hard interruptions are reconciled
on the next invocation. Reservations and completed copies use no sidecar manifest.

One historical workspace/data-root owner encloses the CLI operation. Completed
files are never overwritten. Run artifacts use the existing run ID conventions:
`submitted.toml`, `report.json`, and append-only `events.jsonl`. The report embeds
request/cache decisions and counts; events contain explicit per-archive identity,
local registry identity, status, relative path, checksum, bytes, or safe diagnostic.
No download URLs, response bodies, connection strings, or exception text from
HTTP/PostgreSQL are written to these reports. On cancellation, queued unfinished
files are counted as cancelled, and a cancellation report is saved. SIGKILL cannot
write a final report, but database reservations still permit file recovery.

## Source verification

Official material consulted during implementation:

- [Binance public-data repository](https://github.com/binance/binance-public-data)
- [OKX historical market-data documentation](https://www.okx.com/docs-v5/en/#public-data-rest-api-get-historical-market-data)
- [Gate historical quotation documentation](https://www.gate.com/developer/historical_quotes)
- [Bitget history download page](https://www.bitget.com/data-download)
- [Bybit history download page](https://www.bybit.com/en/derivative-activity/history-data)

OKX requests use one local calendar day to avoid truncation: UTC+8 for trades,
UTC for books. Bitget/Bybit web endpoint field mappings are migrated legacy
assumptions, guarded by strict response/error validation. Public frontend pages
confirm offerings, not those internal API contracts. The automated fixtures
exercise parser behavior, not current endpoint availability. Only documentation
and frontend pages were inspected live; no discovery API scans, existence probes,
or archive body downloads were performed against real exchanges.

## Progress and discovery corrections

Acquisition emits typed events through `progress.py`. One bounded reducer owns
request/file counters, up to six recent results, four retained failures, and the
bounded workers' active files. A single `AcquisitionDisplay` consumes snapshots;
adapters, HTTP clients, and registry repositories do not call Rich. Renderer
exceptions disable presentation without changing operation or registry outcomes.
Interactive progress refreshes at most four times per second in the current
terminal. Redirected output prints request starts and terminal transitions as
plain text, even with forced color. Silent/JSON modes suppress progress; JSON
stdout retains the existing result contract with additive request counts and
interval observations. Color-disabled interactive terminals retain cursor-based
updates without colored styling. No extra terminal or historical TUI is launched.

Mapping success counts completed source interpretation, including empty results.
Cached and empty are labelled subsets of successful requests. Unsupported scope
and failed discovery are separate terminal outcomes. Download resolution counts
all downloaded/reused/failed/cancelled files, not just successes. Totals remain
provisional until discovery/queueing can no longer add files. Reuse is shown as
verification followed by reuse rather than a new transfer. Admission and operation
ownership are unchanged; requests remain sequential, with bounded workers inside
each download request. Cancellation restores the terminal and records never-started
requests as cancelled, not successful.

The adapter version is now `archive-v2-identity`, invalidating older discovery
snapshots. Bybit derivative headings legitimately omit a trailing slash, unlike
spot. Both `Directory listing for` and `Index of` headings are normalized against
the final response directory. Links must remain on that source and in the requested
instrument directory, and match the instrument/date/data filename convention.
Derivative trade headings, unordered-list spot pages, book filenames beginning
with a date, and safe same-directory redirects are covered by fixtures. Explicit
access errors, wrong directories, traversal, unrelated instruments, and malformed
responses remain failures. GET redirects opt in to the existing paced redirect
mechanism; every redirect still consumes a rate admission.

OKX validates family, instrument type, returned `instId`, and filename identity
independently. A perpetual family accepts only its `-SWAP` instrument, never a
similarly named dated future. Bitget validates display identity, the instrument
and book-product filename prefixes, dates, and partitions. Exact repeated identities
are deduplicated, legitimate numbered partitions are preserved, and objects outside
the requested window are excluded. Discovery reports include observed UTC interval
bounds and whether the union of nominal archive intervals spans the request. This
is a bounded SQL aggregate over existing registry metadata, not a claim of complete
market data or a new coverage framework. Internal gaps and absent tail intervals
are reported as incomplete even when discovery itself completed successfully.

URL refresh now occurs only after local verification/recovery cannot reuse a copy.
Workers coordinate one location refresh for the selection and reuse the same HTTP
client. A verified local file needs no usable remote URL. A superseded scan's
membership remains readable by the already-resolved download plan so location
renewal cannot accidentally truncate later queue pages.

See [bounded live verification](LIVE_VERIFICATION.md) for the observed source
availability, exact interval/partition explanations, cache provenance, and limits.
