# Local historical storage browsing

```sh
marketforge storage --testing
marketforge storage coverage --testing --exchange okx --instrument-type future
marketforge storage list --testing --exchange okx --instrument-type future --data-type trades --status complete
marketforge storage list --testing --market spot --match 'ETH*' --quote-asset USDT --limit 30
marketforge storage coverage --testing --base-asset ETH --quote-asset BTC
marketforge storage coverage --testing --exchange gateio --market linear --data-type order_book_l2 --feed snapshot --start 2026-10-27T00:00:00Z --end 2026-10-30T00:00:00Z --gaps
marketforge storage show local:536 --testing
marketforge --json storage coverage --testing --base-asset ETH
```

Bare `storage --testing` selects coverage. Browsing uses the configured raw root,
a verified testing database connection, and a read-only repeatable-read
transaction. It performs no HTTP requests, registry updates, checksum reads,
extraction, file-existence verification or normalization execution. `verify` and
`reconcile` remain separate maintenance operations using their existing services.

## Metadata and filtering

List and coverage accept exchange, market, instrument type, exact native
`--instrument`, scope kind, data type, feed, origin and dates. Both also support
`--base-asset`, `--quote-asset` and native scope-identifier `--match` globs. Asset
matches are exact and case-insensitive. Native `--instrument` remains exact and
case-sensitive. Glob matching is case-insensitive, with `*` matching zero or more
characters and `?` matching one. Quote patterns in the shell: `--match 'ETH*'`,
`--match '*BTC'`, `--match '*USDT'`. `%`, `_`, brackets and backslashes are literal;
SQL uses bound patterns and an explicit escape character.

All filters combine with AND before list pagination or coverage aggregation.
List alone accepts `--status`; coverage always selects complete registry copies.
Both use `--limit` (1..10000, default 100) and `--offset` (default 0). Lists sort by
local ID descending; coverage groups sort by their full identity. JSON includes
pagination totals and whether more results are available. `--archive local:ID`
remains available on list; details accept `local:ID` or a plain numeric ID.
`--wide` adds per-archive filenames beneath the list without crowding identities.
Full paths, SHA-256, source validators, timestamps and readiness are available
in details and JSON. Stored verification timestamps never mean a new verification.

Remote identity comes from the registered remote archive's exchange and flat
`scope_json`. External identity, historical specifications, source and readiness
come from their existing registry relationships. Filenames are never decoded
into identities or assets. The old inventory fields remain in archive JSON,
with additional joined scope, interval, source and metadata diagnostics.

Catalogue resolution matches exchange, instrument type, market category and
native identity together. Aggregate joins return one row per local copy. Explicit
external historical asset specifications take precedence over current catalogue
assets. Instrument scopes use exact catalogue identity. Base-coin scopes use
matching catalogue base assets. Family scopes require explicit family membership
in the latest retained catalogue source metadata (`instFamily`); consistent
values across those members can establish family assets. Missing membership or
conflicting member assets remain unknown. No family/name/string splitting is
used; unsupported family metadata cannot match an exact asset filter. JSON
includes asset sources, catalogue candidate counts and diagnostics. Group JSON
also preserves unknown/mixed asset values.

Canonical Gate depth registry identities omit `source_feed` for incremental
archives and record `snapshot` explicitly. Browsing follows that established
convention and exposes `feed_source`. An explicit unknown feed, or a depth import
without feed metadata, remains unknown and separate from incremental/snapshot.
`--feed unknown` selects missing feed identities. External sources with the same
market/symbol stay in separate coverage groups by external source identity.

## Nominal archive coverage

Coverage groups use origin, external source identity, exchange, instrument type,
market, scope kind/identifier, data type and feed. Every matching complete record
is scanned in 1000-record keyset pages before the **group** display limit. Thus a
small display limit never truncates a group's interval calculation.

Intervals are half-open `[start, end)`; ends are exclusive. Date filters require
timezone-aware timestamps, normalize to UTC, validate `start < end`, and use
interval overlap. A monthly archive matches dates inside the month. An archive
ending exactly at the requested start, or starting at the requested end, does
not overlap. Unknown/invalid intervals cannot establish coverage and are excluded
by date filters, with an omitted-interval count retained in JSON and summaries.
Without date filters their diagnostics remain in their groups.

Overlapping, adjacent, nested and duplicate intervals merge without double-counting
covered duration. Without dates, gaps are internal holes in each group's observed
envelope. With both bounds, gaps also include requested leading/trailing holes.
With only one bound, the other is the observed envelope bound; per-group evaluation
bounds are shown and included in JSON. Missing observed bounds yield unknown
coverage, not zero gaps. Groups require matching local records: absence of a
scope is not inferred to be proof of upstream nonexistence or a searched universe.

`--gaps` adds actual missing `[start, end)` intervals. JSON always includes merged
intervals, counts, coverage duration, evaluation bounds and diagnostics; the gaps
array is populated when requested. File counts describe registered physical local
copies, while `unique_registered_source_count` counts distinct originating
registry objects. Byte totals sum recorded sizes, with unknown-size counts.
Duplicate copies increase physical copy/byte counts, not interval duration.

All results are **nominal archive coverage**. Zero gaps means continuity of
registered intervals, not file validity, successful parsing or event completeness.

## Failed-download cleanup and filtered deletion

`mf` and `marketforge` are equivalent console entry points.

```sh
mf storage prune --testing
mf storage prune --testing --apply

mf storage delete local:536 --testing
mf storage delete --testing --instrument BTCUSDT --all
mf storage delete --testing --exchange bybit --instrument BTCUSDT --start 2026-10-30T00:00:00Z --all
```

Prune defaults to failed attempts; `--status failed` is optional. Normal prune is
preview-only. It lists exact records, observed partials, paths and blockers, with
separate totals for **records eligible for retirement**, **partial files found**
and **reclaimable bytes**. Eligible failed records can be retired when their
partials are already absent; no partial means zero bytes, not an ineligible record.
Ownership, inactivity and reference checks still apply to absent reservations.

`prune --apply` in an interactive terminal first displays every selected target,
then asks one yes/no confirmation (default no). Interactive review shows all
targets even if display pagination was supplied. Confirmation applies exactly
that saved selection: ownership is reacquired, every target is revalidated and
changed/active targets are skipped. Declining removes nothing and retires nothing.
If no targets are eligible, prune exits without prompting or suggesting apply.
A fully blocked delete preview likewise has no apply instruction.

Successful prune removes any positively owned partial and marks the existing
failed record `deleted`. It also retires eligible records whose partials are
absent. Record IDs, original failure messages and run history remain intact.
Outcomes distinguish records from files, for example:

```text
13 failed records retired · 0 partial files removed · 0 B reclaimed
```

### Saved previews for scripts

```sh
mf --json storage prune --testing
# Review the JSON and copy its apply_command, including the exact managed path:
mf storage prune --testing --apply --preview /absolute/printed/preview.json
```

The placeholder path must be replaced with the exact path printed by the preview.
The scripted `--apply --preview PATH` workflow does not prompt. Without a saved
preview, noninteractive prune apply only previews; it refuses eligible mutations
and explains the saved-preview command. A zero-eligible result is a no-op even
when redirected. JSON contains every target and is fully reviewable regardless
of the human display limit. No apply command or saved actionable plan is produced
when every target is blocked or the selection is empty.

Previews are stored under the configured data root's `.jobs/storage/`. Preview
metadata/lock files may be created, but preview does not update registry records
or remove archive bytes. Plan integrity and operation/root identity are checked
on apply. Pagination affects display, never selection. Applying a saved preview
that hid targets is refused; repeat with `--all` or `--json` to review everything.
Interactive prune never applies undisplayed targets. Newly matching rows are not
added to an old preview.

### Delete selectors and dates

Delete accepts one `local:ID` (plain numeric IDs also work) or applicable storage
filters: exact native `--instrument`, `--match`, exchange, market, instrument type,
scope kind, base/quote assets, data type, feed, origin, status and UTC date bounds.
All supplied filters combine with AND, including filters alongside a local ID.
A native symbol or either date bound can be the sole selector. Unscoped and
status-only bulk deletion are rejected; `--all` is only a display option and does
not qualify as a selector.

Delete dates use **full containment** by default. With only `--start`, an archive's
start must be at or after the bound. With only `--end`, its exclusive end must be
at or before the bound. With both, the full archive must lie inside the window.
Thus a three-day window includes its daily archives and excludes a monthly one.
`--overlap` explicitly switches to half-open interval intersection and requires
a date bound. Adjacent intervals touching a bound do not overlap. Unknown/unusable
intervals are skipped when filtering dates. Previews always show full archive
bounds, including monthly archives extending beyond an overlap window. Prune
retains the browsing interval-overlap semantics for its own date filters.

Delete remains preview-first and requires `--apply --preview PATH` to apply a fully
reviewed selection. Completed managed archives and managed external-import copies
can be removed through this workflow. Original external sources are preserved.
Failed remote reservations use the same partial-file ownership checks as prune.

### Ownership and concrete dependency checks

A failed row alone is insufficient ownership evidence. Eligible retirement requires
its downloader-reserved remote-ID/32-hex-attempt-directory/canonical-filename path,
no competing registry record in that attempt directory, no durable checksum/size
reservation, no final file and no registered dataset reference. Symlinks (root,
parent or leaf), traversal, non-regular files and multiply linked inodes are
retained. Pending/downloading attempts, uncertain activity, external imports,
ambiguous ownership and recoverable reservations are blocked. Reclaimable bytes
are eligible partial lengths observed on disk, not registry estimates; physical
filesystem free-space effects may differ.

Managed final copies are blocked by concrete registered dataset references,
readable prepared normalization/process jobs referencing their paths, or saved
normalization input selections referencing their IDs. Active managed operations,
unsafe paths, neighboring partials and missing final files also block removal.
Incomplete lineage or untracked standalone engine consumers are not blanket
blockers. The ordinary saved-preview apply command requires no override flag.

### Revalidation, receipts and retry

Preview attempts existing workspace/data-root locks to establish inactivity and
reports activity-unknown blockers when held. Apply requires those locks, takes
per-target registry row locks, and checks record metadata, status, path, eligibility
and file/parent inode identity immediately before descriptor-relative unlink.
Acquisition, imports and normalization preparation share this Python ownership.
Prune can retire a reviewed eligible reservation when a retry already removed its
partial, but will not remove a changed/replacement file. Blocked targets do not
become enabled merely because their state later changes.

A durable `.apply.json` receipt records intent before removing bytes. After unlink
and directory sync, prune/delete set the existing archive row to `deleted`,
preserving its ID and historical references.
If registration fails, reapplying that preview can finalize an unchanged record
whose reviewed file is absent, without deleting a replacement. Already deleted rows are
idempotent; reconciliation excludes `deleted` rows. Apply reports partial successes,
blockers, changed targets, records retired, files removed and bytes reclaimed
separately. Retain preview/receipt pairs when recovery is pending. Apply returns
nonzero on blocked/changed/failed targets; a fresh zero-eligible prune is a no-op.
