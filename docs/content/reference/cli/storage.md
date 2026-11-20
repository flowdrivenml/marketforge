<!-- Suggested file: docs/content/reference/cli/storage.md -->

# Raw Data Storage

MarketForge keeps track of the historical data files you download or import. As your collection grows, you need a way to see which files you already have, what periods they cover, and whether they are still available locally.

For example, imagine downloading BTCUSDT trades from Bybit over several months.

After some time, you might want to know:

- Which dates have already been downloaded?
- Are there any missing days?
- How much historical data is stored?
- Where is a particular archive located?
- Are the downloaded files still intact?
- Can an interrupted download be recovered?
- How can you safely remove files you no longer need?

You could try answering these questions by searching through directories. However, exchanges use different filenames, archive formats, and ways of organizing their historical data.

**MarketForge solves this by keeping a record of the raw files it manages.**

The `mf storage` commands let you inspect those records, check local files, recover certain interrupted operations, and safely remove unwanted copies.

This guide explains how storage works and how to use its commands.

---

## 1. How MarketForge Storage Works

When MarketForge downloads an archive, it does more than save a file.

It also records information about that file in PostgreSQL.

This includes details such as the exchange, instrument, data type, historical period, local file location, and information needed to verify the downloaded copy.

The same managed storage system can also contain files you import yourself.

For example, you might have:

- BTCUSDT trades downloaded from Bybit.
- ETHUSDT order-book archives downloaded from OKX.
- A historical dataset purchased from another provider.
- Recordings collected by your own trading infrastructure.

All these files can be managed through MarketForge's raw-data storage.

### Raw files and storage records

It is useful to understand the difference between a file and its storage record.

**Raw file**

The actual historical data stored on your computer.

For example:

```text
BTCUSDT2026-10-27.csv.gz
```

**Storage record**

Information MarketForge keeps about that file.

A record can describe:

| Information | Example |
|---|---|
| Exchange | Bybit |
| Instrument | BTCUSDT |
| Instrument type | Perpetual |
| Market category | Linear |
| Data type | Trades |
| Historical date | October 27, 2026 |
| Local status | Complete |
| File location | Managed raw-data directory |
| Verification information | File size and SHA-256 checksum |

MarketForge uses this information to find files without opening every archive.

It also allows later processing stages to select the appropriate raw inputs.

### Where are the files stored?

By default, MarketForge uses the project's `data/` directory.

Raw files are kept beneath the configured data root:

```text
data/
└── raw/
```

The exact organization inside this directory is managed by MarketForge.

You should not depend on a particular internal filename or directory structure when looking for data.

Instead, use the storage commands to find the relevant files and inspect their registered paths.

If you configure a different data root, MarketForge uses that location instead.

To inspect your current application settings:

```bash
mf config show
```

The output includes the resolved data root.

### Acquisition, storage, and datasets are different

These three parts of MarketForge solve different problems.

| Component | Main responsibility |
|---|---|
| **Acquisition** | Find historical archives on exchanges and download them. |
| **Storage** | Keep track of raw files, inspect their availability, verify them, and manage local copies. |
| **Datasets** | Inspect the normalized or merged data produced by MarketForge. |

For example:

```text
                 BYBIT
                   |
                   v
             Acquisition
                   |
                   v
          Downloaded Archives
                   |
                   v
             Raw Storage
                   |
                   v
             Normalization
                   |
                   v
           Parquet Datasets
                   |
                   v
          Dataset Inspection
```

Storage manages the original source files.

It does not automatically normalize trades or reconstruct order books.

Those operations belong to the processing pipeline.

---

## 2. Before You Begin

The storage commands in this guide use the MarketForge testing database.

Make sure Forge is installed, PostgreSQL is configured, and the required database migrations have been applied.

See [Installation](../../getting-started/installation.md) if you have not completed the setup.

You can inspect the database status with:

```bash
mf database status --testing
```

### What does `--testing` mean?

The current storage commands require the explicitly configured MarketForge testing database.

For example:

```bash
mf storage --testing
```

The `--testing` option selects the guarded testing database.

It does **not** make every operation a dry run.

Some storage commands only read information. Others can update file statuses, recover incomplete operations, or delete managed files.

We will distinguish these actions throughout the guide.

---

## 3. View Your Historical Data Collection

Suppose you have already downloaded historical data from several exchanges.

You want to see what is available locally without downloading anything new.

The quickest way is:

```bash
mf storage --testing
```

This opens the default storage overview.

MarketForge reads its registered raw-file information and summarizes the historical periods covered by the stored collection.

The default view is equivalent to requesting storage coverage.

### What does the overview show?

The overview groups matching files by their recorded market identities and historical coverage.

This helps you answer questions such as:

- Which instruments have downloaded data?
- Which data types are available?
- What historical periods do the files cover?
- Are there gaps between the recorded archive intervals?

For example, suppose you previously downloaded:

```text
Bybit BTCUSDT Trades
    October 25
    October 26
    October 27

Bybit BTCUSDT Depth
    October 26
    October 27

OKX ETHUSDT Trades
    October 27
```

The overview can help you identify the periods represented by these files.

You do not need to inspect each filename individually.

### Does opening the overview verify the files?

No.

**Storage browsing reads the saved records. It does not automatically reopen and verify every file.**

This makes browsing useful even when your collection contains many large archives.

However, a previously completed file could have been moved, damaged, or removed since its last verification.

For an actual integrity check, use `mf storage verify`, explained later.

<!-- SCREENSHOT PLACEHOLDER: storage-overview.png

Suggested image:
The default output of:

mf storage --testing

Capture:
- Several exchanges or instruments
- Trades and depth categories
- Historical date ranges
- File or coverage counts
- Any displayed coverage indicators

Purpose:
Give users an immediate visual understanding of what
MarketForge knows about their local historical data.

Suggested alt text:
"MarketForge raw storage overview showing historical data coverage across instruments."

-->

---

## 4. List Downloaded Archives

The overview is useful for understanding your collection as a whole.

But sometimes you need to inspect the individual files.

For example, you might want to find every BTCUSDT trade archive downloaded from Bybit.

Use:

```bash
mf storage list --testing
```

This displays registered local archive records.

Unlike the coverage overview, which focuses on historical periods, the list command focuses on individual records.

### Filter by exchange and instrument

To find Bybit BTCUSDT archives:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT
```

MarketForge searches the stored records and returns matching entries.

This does not contact Bybit.

It only searches the local PostgreSQL registry.

### Filter by market type

A symbol is not always enough to identify the market you want.

For example, Bybit has spot and derivative markets that may use the same symbol.

To select BTCUSDT linear perpetual data:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --instrument-type perpetual \
    --market linear
```

Now the selection is more specific.

It distinguishes the linear perpetual from other instruments that might share the BTCUSDT symbol.

### Filter by data type

Suppose you only want executed trades.

Add:

```bash
--data-type trades
```

The complete command becomes:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --instrument-type perpetual \
    --market linear \
    --data-type trades
```

For order-book data, use:

```bash
--data-type order_book_l2
```

### Show filenames

By default, the terminal displays a compact view.

Use `--wide` to include registered filenames:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --wide
```

This is useful when you want to identify a particular daily or hourly archive.

For complete record details, including the stored path and verification information, use `storage show`.

<!-- SCREENSHOT PLACEHOLDER: storage-list.png

Suggested image:
Output of:

mf storage list --testing --exchange bybit --instrument BTCUSDT --wide

Capture:
- Multiple archive records
- Local IDs
- Archive filenames
- Status information
- Dates

Purpose:
Show how users can browse their actual raw files.

Suggested alt text:
"MarketForge listing downloaded Bybit BTCUSDT historical archives."

-->

---

## 5. Inspect an Individual Archive

Every registered local archive has a unique identifier assigned by PostgreSQL.

You can use this identifier to inspect the corresponding record.

For example, suppose `storage list` returns a local archive with ID `536`.

You can inspect it using:

```bash
mf storage show 536 --testing
```

Replace `536` with the actual ID from your collection.

MarketForge also accepts the prefixed form:

```bash
mf storage show local:536 --testing
```

Both forms refer to the same local archive.

### Why inspect a single file?

An individual record provides more detail than the general list.

Depending on the recorded information, you can inspect:

- The original archive identity.
- The exchange and market.
- The instrument or source scope.
- The historical interval.
- The local storage location.
- The current recorded status.
- The file size.
- The checksum or other retained verification evidence.

This is useful when investigating a particular file or deciding whether it can be reused.

### What is a checksum?

A checksum is a value calculated from a file's contents.

MarketForge uses SHA-256 checksums to help verify that a stored copy has not changed.

If the file changes, its calculated checksum will normally differ from the one recorded when the copy was accepted.

This lets MarketForge detect many forms of accidental modification or corruption.

However, simply displaying the saved checksum does not recalculate it.

To verify the current file contents, use the verification command.

<!-- SCREENSHOT PLACEHOLDER: storage-show.png

Suggested image:
Output of:

mf storage show LOCAL_ID --testing

Capture:
- Archive identity
- Local file path
- Historical interval
- Recorded status
- File size
- Checksum information

Purpose:
Show what information is available for a single managed archive.

Suggested alt text:
"Detailed MarketForge storage record for a historical archive."

-->

---

## 6. Check Historical Coverage

One of the most useful storage features is the ability to inspect which historical periods are represented by your downloaded files.

Imagine you want to research BTCUSDT trading activity over an entire month.

You have downloaded many archives, but you are not sure whether every day is represented.

You could manually compare filenames.

However, this becomes difficult when working with several exchanges, different archive intervals, and multiple data types.

MarketForge provides **coverage inspection** to help with this.

### View coverage for an instrument

Use:

```bash
mf storage coverage \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --data-type trades
```

MarketForge examines the registered historical intervals of the matching completed files.

It combines overlapping or adjacent intervals to summarize the periods represented by those files.

For example, suppose the registry contains:

```text
October 25    Complete
October 26    Complete
October 27    Complete
```

The combined nominal coverage may be represented as:

```text
October 25 ───────────────── October 28
```

The end boundary is exclusive.

If the same period is represented by more than one matching copy, overlapping intervals do not create extra covered time.

### Find missing periods

Suppose one day is missing:

```text
October 25    Complete
October 26    Missing
October 27    Complete
```

MarketForge can identify the gap between the registered intervals.

Use:

```bash
mf storage coverage \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --data-type trades \
    --gaps
```

The `--gaps` option includes missing periods in the coverage result.

This helps you identify dates that may need another acquisition.

### Check a specific date range

You can restrict the coverage inspection to a historical interval.

For example:

```bash
mf storage coverage \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --data-type trades \
    --start 2026-10-01T00:00:00Z \
    --end 2026-11-01T00:00:00Z \
    --gaps
```

This requests a coverage view for October 2026.

When both date boundaries are provided, the gap calculation can include missing periods at the beginning or end of the requested interval.

Without explicit boundaries, MarketForge can only report gaps within the observed coverage envelope.

For example, if your earliest recorded archive starts on October 10, MarketForge cannot assume that October 1–9 was part of your intended collection unless you specify that period.

### Understanding nominal coverage

**An important limitation: archive coverage is not the same as complete market-data coverage.**

Suppose an archive is registered as covering October 27.

This means the archive's recorded period represents that day.

It does not prove that every trade or order-book update from that day is present.

A file might:

- Contain missing events.
- Have gaps in its source sequence.
- Start later than its declared interval.
- End before its declared interval.
- Contain invalid records.
- Lack the snapshots required for continuous order-book reconstruction.

Storage coverage does not inspect these details.

Those questions belong to source processing and integrity analysis.

Similarly, missing or unknown interval information must not be interpreted as proof of complete coverage.

### Coverage and completeness

Keep these three ideas separate:

| Question | Where to check |
|---|---|
| Do I have registered archives for this period? | `mf storage coverage` |
| Are the local file bytes still intact? | `mf storage verify` |
| Does the data contain valid, complete market events? | Normalization and relevant integrity evidence |

A collection can have no nominal coverage gaps while still containing incomplete market data.

<!-- SCREENSHOT PLACEHOLDER: storage-coverage.png

Suggested image:
Storage coverage for Bybit BTCUSDT over a multi-day interval.

Capture:
- Covered intervals
- Missing intervals, if present
- Separate data types when useful

Purpose:
Show how coverage helps users identify missing archive periods.

Suggested alt text:
"MarketForge historical archive coverage showing downloaded periods and nominal gaps."

-->

---

## 7. Filter Your Storage Collection

As your collection grows, you may have thousands of archive records.

Storage filters let you focus on the data you need.

You can combine multiple filters in one command.

For example:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --market linear \
    --instrument-type perpetual \
    --instrument BTCUSDT \
    --data-type trades
```

MarketForge returns records that match the combined selection.

### Available filters

| Filter | What it selects |
|---|---|
| `--exchange VENUE` | A registered exchange or execution venue. |
| `--market CATEGORY` | Spot, linear, inverse, option, or equity market. |
| `--instrument-type TYPE` | Spot, perpetual, future, option, or equity instrument. |
| `--instrument SYMBOL` | A specific registered instrument or native scope identifier. |
| `--data-type TYPE` | Trades or L2 order-book archives. |
| `--scope-kind KIND` | Instrument, instrument family, or base-asset scope. |
| `--feed FEED` | Recorded feed variant, such as incremental or snapshot. |
| `--origin ORIGIN` | Files acquired remotely or supplied externally. |
| `--base-asset ASSET` | Instruments associated with a base asset. |
| `--quote-asset ASSET` | Instruments associated with a quote asset. |
| `--match GLOB` | Match native symbols using a pattern. |
| `--start DATETIME` | Start of a requested time range. |
| `--end DATETIME` | Exclusive end of a requested time range. |
| `--limit N` | Maximum number of displayed results. |
| `--offset N` | Position from which to continue a paginated result. |

Not every filter has the same meaning for every source.

For example, an instrument-family archive may contain multiple concrete contracts, and some historical assets or specifications may be unknown.

The recorded metadata determines which filters can match a particular archive.

### Filter by source origin

MarketForge can manage files downloaded from exchanges and files supplied by users.

To show remotely acquired archives:

```bash
mf storage list \
    --testing \
    --origin remote
```

To show externally supplied files:

```bash
mf storage list \
    --testing \
    --origin external
```

This is useful when you want to distinguish downloaded exchange data from purchased datasets or your own recordings.

Both can exist in the same managed raw-data collection.

### Search using a symbol pattern

Suppose you want to find stored instruments whose native symbols begin with `BTC`.

Use:

```bash
mf storage list \
    --testing \
    --match 'BTC*'
```

The pattern is matched against native symbols without case sensitivity.

The `*` character matches zero or more characters.

The `?` character matches one character.

Quote patterns in the shell so they are passed to MarketForge rather than expanded into local filenames.

### Filter by historical period

You can also select records whose intervals overlap a requested period:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --start 2026-10-01T00:00:00Z \
    --end 2026-11-01T00:00:00Z
```

These date filters use overlap for normal storage browsing.

An archive may appear even if only part of its registered interval overlaps the requested period.

The timestamps must include a timezone.

### Pagination

A large collection may contain more records than you want to display at once.

Use `--limit` and `--offset` to browse it in smaller groups.

For example:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --limit 20
```

To continue from the next group:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --limit 20 \
    --offset 20
```

The default browsing limit is 100, with an accepted range of 1–10,000.

**Pagination changes which rows are displayed, not which archives exist or which files are considered by coverage aggregation.**

Coverage combines all matching records before applying group pagination.

---

## 8. Understand File Statuses

Every managed archive has a recorded status.

These statuses describe the current known state of the local record.

Some common statuses are:

| Status | Meaning |
|---|---|
| `pending` | The file has been selected or reserved for acquisition. |
| `downloading` | A download or managed-copy operation is in progress or has an unfinished reservation. |
| `complete` | The managed copy was accepted as complete. |
| `missing` | A previously expected local file could not be found during an applicable check. |
| `corrupt` | A file failed an applicable integrity check. |
| `failed` | The acquisition or managed-copy attempt failed. |
| `deleted` | The managed copy has been removed through the supported lifecycle. |

A recorded `complete` status is useful, but it should not be confused with a fresh verification of the file.

If a file was accepted yesterday and deleted manually today, its registry status may remain unchanged until an operation checks the actual file.

### Filter by status

To list completed archives:

```bash
mf storage list \
    --testing \
    --status complete
```

To find failed records:

```bash
mf storage list \
    --testing \
    --status failed
```

To inspect missing files:

```bash
mf storage list \
    --testing \
    --status missing
```

These filters are useful for browsing known outcomes.

They do not perform new verification.

---

## 9. Verify Downloaded Files

Suppose you downloaded an archive several weeks ago.

The registry says it is complete, but you want to make sure the file has not been modified or corrupted.

This is what **storage verification** is for.

### Why verify files?

Historical archives are often reused for normalization, testing, and research.

If a file changes after downloading, results based on that file may no longer be reproducible.

For example, imagine that an archive was partially overwritten or moved to another directory.

MarketForge needs to distinguish a valid existing copy from a missing or damaged one.

Verification checks the actual local file against its recorded evidence.

### Verify one archive

First, find the archive ID:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --wide
```

Then verify the relevant record:

```bash
mf storage verify --testing --archive LOCAL_ID
```

Replace `LOCAL_ID` with the ID returned by the list command.

MarketForge reads the local file and performs the applicable verification checks.

This can include comparing its calculated SHA-256 checksum with the recorded value.

### Does verification change anything?

Yes, potentially.

**`storage verify` is not a read-only browsing command.**

It does not download new files or change the historical archive contents, but it may update the registry's verification information or mark files as missing or corrupt.

This is different from `storage show`, which only displays saved information.

### Verify a bounded selection

You can also run:

```bash
mf storage verify --testing --limit 20
```

Without a specific archive ID, maintenance selects a bounded group of recent records from the configured raw-data collection.

The default maintenance limit is 100.

For precise investigations, prefer `--archive LOCAL_ID`.

Do not rely on `--status` or `--origin` to narrow maintenance verification: these options may be accepted by the parser but are not currently forwarded to the maintenance selection logic.

### What happens if verification fails?

A failed verification may indicate that the file is missing, corrupt, or cannot be verified with the available evidence.

Inspect the reported result before taking action.

Depending on the problem, you may need to:

- Check whether the file was moved or modified.
- Inspect the stored path and checksum.
- Run storage reconciliation if an incomplete managed operation may be recoverable.
- Repeat the original acquisition if a new copy is required.

Verification does not automatically contact the exchange or redownload the file.

<!-- SCREENSHOT PLACEHOLDER: storage-verify.png

Suggested image:
Verification output for one completed archive.

Capture:
- Local archive ID
- Verification outcome
- File status
- Any checksum or error information

Purpose:
Show users what a successful verification looks like.

Suggested alt text:
"MarketForge verifying the integrity of a locally stored historical archive."

-->

---

## 10. Recover Interrupted Storage Operations

Downloads and imports do not always finish successfully.

A network connection can fail, the application can be interrupted, or a process can stop while a file is being registered.

MarketForge keeps information about managed operations so that some incomplete states can be recovered safely.

The `reconcile` command is designed for this purpose.

### Why is reconciliation necessary?

Consider a download that has successfully transferred its file but is interrupted before all the final registration work finishes.

The file bytes may already exist, but the registry might still show an unfinished operation.

Simply starting another download could perform unnecessary work.

MarketForge can sometimes use the saved file and operation information to complete the interrupted registration.

This is called **storage reconciliation**.

### Reconcile one archive

Use:

```bash
mf storage reconcile --testing --archive LOCAL_ID
```

MarketForge examines the selected record and its retained evidence.

When sufficient verified information exists, it may complete an unfinished managed-copy operation.

For example, it may recover a durable reservation, publish a verified temporary copy, or finish registration.

### What reconciliation does not do

Reconciliation does not:

- Contact the original exchange.
- Redownload missing file contents.
- Repair corrupted market-data records.
- Reconstruct missing trades or order-book updates.
- Guarantee that an incomplete transfer can be resumed.

If an incomplete download lacks the required verified evidence, reconciliation cannot simply assume that the file is complete.

Such partial files are retained rather than silently treated as valid.

A later acquisition can download the required file again.

### Reconcile multiple records

You can inspect a bounded selection:

```bash
mf storage reconcile --testing --limit 20
```

As with verification, the default limit is 100.

Use a specific archive ID whenever you are investigating one known problem.

### When should you rerun acquisition instead?

If the file was never fully downloaded, or its contents are missing or corrupt, the original acquisition recipe may be the appropriate recovery path.

For example:

```bash
mf acquisition download bybit-btc-20261027 --testing
```

MarketForge can reuse verified existing copies and download files that still need valid local copies.

Do not manually modify registry reservations or replace managed files to force an operation to succeed.

<!-- SCREENSHOT PLACEHOLDER: storage-reconcile.png

Suggested image:
A storage reconciliation result for an interrupted or
previously incomplete archive operation.

Capture:
- Selected archive
- Previous state
- Recovery action
- Resulting state

Purpose:
Explain what recovery actually does.

Suggested alt text:
"MarketForge reconciling an interrupted historical archive registration."

-->

---

## 11. Safely Clean Up Failed Downloads

Over time, failed downloads and interrupted operations may leave registry records or partial files that are no longer needed.

MarketForge provides `storage prune` to review and clean up eligible failed attempts.

### What is pruning?

Pruning is intended for failed acquisition attempts and eligible partial files.

It is different from deleting successfully downloaded archives.

For example, suppose a download failed halfway through and left a partial file that cannot be recovered.

You may eventually want to remove that unused partial data.

Pruning helps manage these cases while preserving the information needed to understand past operations.

### Preview what can be pruned

Run:

```bash
mf storage prune --testing
```

By default, this does **not** immediately remove the selected files.

MarketForge prepares a preview of the failed records eligible for cleanup.

The preview lets you inspect what would be affected before applying changes.

### Why does MarketForge use previews?

Deleting files is a potentially destructive operation.

A broad filter might match more records than you intended.

Instead of deleting immediately, MarketForge separates cleanup into two stages:

1. Review the selected files and eligible actions.
2. Explicitly apply the reviewed cleanup.

This gives you an opportunity to detect mistakes before removal.

### Review all selected targets

Use:

```bash
mf storage prune --testing --all
```

The `--all` option means **show every selected target for review**.

It does not mean "select every file in storage."

The actual selection still comes from the command's filters and pruning rules.

You can also request a detailed JSON preview:

```bash
mf --json storage prune --testing
```

The JSON result can include the selected targets, eligibility information, and the managed preview path.

### Apply pruning interactively

In an interactive terminal, use:

```bash
mf storage prune --testing --apply
```

MarketForge displays the complete target selection and asks for confirmation before applying eligible cleanup.

The confirmation defaults to No.

Review the displayed targets carefully.

### Apply pruning from a saved preview

For scripted cleanup, use the exact preview file returned by the earlier preview operation:

```text
mf storage prune --testing --apply --preview PATH
```

Replace `PATH` with the actual managed preview path.

Do not guess or construct a preview filename manually.

The saved preview records the reviewed selection.

MarketForge checks it again before applying changes.

### What does pruning remove?

Eligible pruning actions can retire failed records and remove partial files positively owned by the corresponding managed operation.

The exact actions depend on the record's state and retained evidence.

A failed record does not automatically mean every associated file is safe to delete.

Active operations, dependencies, unsafe file paths, or changed files may block removal.

Pruning preserves the registry information needed to trace the historical operation.

<!-- SCREENSHOT PLACEHOLDER: storage-prune-preview.png

Suggested image:
Preview of failed download records eligible for pruning.

Capture:
- Selected targets
- Eligible and blocked counts
- Proposed actions
- Preview location

Purpose:
Demonstrate the review-before-delete workflow.

Suggested alt text:
"MarketForge previewing cleanup of failed historical archive downloads."

-->

---

## 12. Delete Managed Archive Copies

Sometimes you want to remove a successfully downloaded archive.

For example:

- You downloaded an incorrect market.
- You no longer need a large historical period.
- You want to reclaim disk space.
- You plan to download a verified replacement later.

MarketForge provides `storage delete` for removing managed raw-file copies.

**Deletion is separate from pruning.**

Pruning is mainly concerned with failed attempts and eligible partial files.

Deletion targets selected managed archive copies.

### Preview deletion of one file

First, find the archive:

```bash
mf storage list \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --wide
```

Then preview deletion of the selected local ID:

```bash
mf storage delete LOCAL_ID --testing
```

This does not immediately delete the file.

MarketForge creates a preview and evaluates whether the selected copy can be safely removed.

For example, a file may be blocked because another managed operation or registered dependency still requires it.

### Review the deletion preview

The preview describes the selected targets and whether they are eligible for removal.

Inspect it carefully.

A preview may create managed preview or lock files, but it does not remove the selected archive bytes or update the archive records as deleted.

The preview is therefore safer than immediate deletion, although it is not a completely side-effect-free filesystem operation.

### Apply a reviewed deletion

Deletion requires a saved preview.

Use the exact preview path returned by the previous operation:

```text
mf storage delete --testing --apply --preview PATH
```

Replace `PATH` with the actual managed preview path.

MarketForge checks the saved selection and its current safety conditions before applying the operation.

If the underlying state has changed or the operation is blocked, it may refuse to remove the file.

### Delete several files using filters

You can also select archives using storage filters.

For example, to preview a specific historical period:

```bash
mf storage delete \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --instrument-type perpetual \
    --market linear \
    --data-type trades \
    --start 2026-10-01T00:00:00Z \
    --end 2026-10-08T00:00:00Z \
    --all
```

This previews eligible managed copies selected by the supplied filters.

It does not immediately remove them.

Before applying any broad deletion, confirm that the preview includes exactly the intended files.

### Date filtering behaves differently during deletion

Normal storage browsing selects archives whose intervals overlap your requested date range.

Deletion is more restrictive by default.

A date-filtered delete selection requires the archive's recorded interval to fit completely inside the requested boundaries.

For example, suppose an archive covers October 1–2.

A deletion request covering only a few hours on October 1 will not select the entire archive under the default containment rules.

This helps prevent accidentally deleting a larger file when you only intended to target a small portion of its date range.

If you deliberately want to select any archive that overlaps the requested interval, add:

```bash
--overlap
```

This option requires a date boundary.

Use it carefully because it may select files covering more time than the requested interval.

### What happens to original external files?

MarketForge distinguishes managed copies from original user-supplied files.

Deleting an eligible managed copy does not delete the original external source file from which it was imported.

However, removing a managed copy can make that particular registered local archive unavailable for later processing until a suitable copy is restored or registered again.

### What happens to existing dataset registrations?

Storage cleanup checks relevant dependencies before removing files.

A managed archive that is still required by another operation or protected dependency may be blocked.

Registry lineage and historical operation information are retained.

Do not interpret successful raw-file cleanup as deletion of normalized Parquet datasets.

Those are separate managed artifacts.

<!-- SCREENSHOT PLACEHOLDER: storage-delete-preview.png

Suggested image:
Deletion preview for a selected historical archive or bounded group.

Capture:
- Exact selected files
- Disk-space information, if displayed
- Dependency checks
- Eligible and blocked targets
- Saved preview path

Purpose:
Make the deletion safety model easy to understand.

Suggested alt text:
"MarketForge safely previewing deletion of managed historical archive files."

-->

---

## 13. Understand Cleanup Safety

Both pruning and deletion use safeguards to prevent unsafe removal.

These safeguards matter because MarketForge may have active operations, registered dependencies, and files whose recorded state has changed since the last inspection.

### Previews are not deletion commands

Commands without `--apply` normally perform the review stage.

They do not remove the selected archive bytes.

However, they may write the managed preview and lock files needed for a later operation.

### Pagination does not limit deletion targets

This is especially important.

A display limit only controls how many results are shown.

It does not necessarily restrict the underlying cleanup selection.

For example, displaying the first twenty matching records does not mean only those twenty records will be affected by a later approved operation.

Always review the complete selection before applying cleanup.

Use `--all` or the full JSON preview for that review.

### Applying a preview checks the selection again

MarketForge does not blindly trust an old preview.

Before applying cleanup, it checks whether the saved target selection and relevant safety conditions are still valid.

This can prevent removal when files or dependencies have changed.

### Some targets cannot be removed

A target may be blocked because of:

- A dependency on the managed file.
- An active operation.
- Missing or changed ownership information.
- An unsafe or changed filesystem path.
- An incomplete or unverifiable operation.
- Another safety condition enforced by the cleanup service.

Do not bypass these protections through manual database edits or direct filesystem deletion.

Investigate the blocker first.

### Keep recovery information

Cleanup operations retain registry history and can produce receipts that help explain or recover interrupted work.

Keep preview and receipt files while recovery is still pending.

A partially completed cleanup should be investigated through its reported outcomes rather than repeated blindly.

---

## 14. Inspect Storage-Related Runs

Storage records tell you what MarketForge currently knows about the files.

Run reports tell you what happened during an operation.

These are related but different.

For example, a download might have found ten archives, reused six existing copies, downloaded three new files, and failed on one.

The storage list shows the resulting file records.

The acquisition run report explains the individual outcomes.

### List saved runs

Use:

```bash
mf runs list --limit 10
```

This displays recent retained run records.

Unlike registry-backed storage commands, these run-inspection commands read local run artifacts and do not require `--testing`.

### Inspect a run report

Use:

```bash
mf runs report RUN_UUID
```

Replace `RUN_UUID` with the identifier returned by the operation.

You can also use:

```bash
mf runs show RUN_UUID
```

For the supported UUID-based run records, `show` and `report` read the same saved report.

### Inspect recorded events

Use:

```bash
mf runs logs RUN_UUID --limit 50
```

This reads recorded events from the run.

It does not follow a running process in real time.

The command returns the first matching events within its bounded read limit.

### What information do runs preserve?

Acquisition runs retain information such as the submitted recipe, overall result, request outcomes, and recorded file events.

This helps explain:

- Which files were discovered.
- Which files were downloaded.
- Which copies were reused.
- Which requests failed.
- Whether an operation was interrupted.
- Where additional diagnostics were saved.

The retained run directory is generally located beneath the configured data root's `runs/` directory.

The exact path should be obtained from the operation result.

For additional details, see [Managing acquisitions](../../workflows/acquisition/managing-acquisitions.md).

---

## 15. Storage Command Reference

The following table summarizes the main storage commands.

| Command | Purpose | Can change managed state? |
|---|---|---|
| `mf storage --testing` | Show the default raw-data coverage overview. | No |
| `mf storage coverage --testing` | Inspect registered historical coverage. | No |
| `mf storage list --testing` | Browse individual raw-file records. | No |
| `mf storage show ID --testing` | Inspect one registered archive. | No |
| `mf storage verify --testing` | Check local files against their recorded evidence. | Yes, registry status/evidence |
| `mf storage reconcile --testing` | Recover supported incomplete managed-copy operations. | Yes |
| `mf storage prune --testing` | Preview cleanup of eligible failed attempts. | May create preview files |
| `mf storage delete ID --testing` | Preview deletion of a managed copy. | May create preview files |
| `mf storage prune --testing --apply` | Apply reviewed pruning under the supported confirmation rules. | Yes |
| `mf storage delete --testing --apply --preview PATH` | Apply a saved, reviewed deletion. | Yes |

### Read-only browsing

These commands read the registered file information without contacting providers or hashing the full archive contents:

```bash
mf storage --testing
```

```bash
mf storage list --testing --wide
```

```bash
mf storage coverage --testing --gaps
```

```bash
mf storage show LOCAL_ID --testing
```

### Verification and recovery

These commands inspect actual files or recover incomplete managed operations.

They can update managed state:

```bash
mf storage verify --testing --archive LOCAL_ID
```

```bash
mf storage reconcile --testing --archive LOCAL_ID
```

Neither command automatically redownloads missing archives.

### Safe cleanup

Preview first:

```bash
mf storage prune --testing
```

```bash
mf storage delete LOCAL_ID --testing
```

Then apply only after reviewing the complete target selection and satisfying the required confirmation or saved-preview rules.

### JSON output

Most storage commands support the global JSON output option.

For example:

```bash
mf --json storage list --testing
```

```bash
mf --json storage coverage --testing --gaps
```

```bash
mf --json storage show LOCAL_ID --testing
```

```bash
mf --json storage verify --testing --archive LOCAL_ID
```

The `--json` option must appear before `storage`.

JSON output is useful for scripting, inspecting detailed metadata, and reviewing cleanup selections.

For cleanup commands, review the returned preview path and target information carefully before applying changes.

---

## 16. Common Questions

### Does MarketForge store historical files inside PostgreSQL?

No.

MarketForge stores raw archive contents on the filesystem.

PostgreSQL keeps information about the managed files, including their identities, locations, statuses, and relevant verification evidence.

This lets MarketForge search and manage large collections without keeping the archive contents inside database rows.

### Does `storage list` check whether the files still exist?

Not by opening and verifying every file.

Storage browsing uses saved registry information.

Use `storage verify` when you need to check the actual local copy.

### Does `complete` mean the file is still valid?

It means the managed record was accepted as complete.

It does not mean MarketForge has just rehashed the file.

A later verification may discover that a file is missing or has changed.

### Can I see which days I have downloaded?

Yes.

Use `storage coverage` with the appropriate exchange, instrument, market, and data-type filters.

Add `--gaps` to include missing periods in the nominal coverage view.

### Does zero coverage gaps mean all market events are present?

No.

It means the registered completed archive intervals have no detected nominal gaps within the applicable coverage view.

It does not prove that every trade or order-book update is present inside those archives.

### Can I find files that came from external providers?

Yes.

Use:

```bash
mf storage list --testing --origin external
```

Externally supplied files participate in the same managed raw-data collection.

### Can MarketForge repair a corrupted archive?

Storage verification can detect applicable integrity problems.

Reconciliation can recover certain incomplete managed-copy operations when enough durable evidence exists.

It does not repair arbitrary corrupted archive contents.

A new download or import may be necessary.

### Can I resume an interrupted download?

Recovery depends on what was completed and recorded before the interruption.

MarketForge can recover some durable reservations and reuse verified completed copies.

An incomplete transfer without the required verified evidence may need to restart rather than resume from its previous byte position.

### Is deleting a managed file the same as deleting its normalized dataset?

No.

Raw archives and normalized datasets are different artifacts.

Storage cleanup manages raw-file copies and checks relevant dependencies.

Normalized dataset management is a separate responsibility.

### Can I delete several archives at once?

Yes, using a sufficiently specific selection.

Always preview and review the complete target set before applying deletion.

Broad deletion is guarded, and a status-only selector is not sufficient for unscoped bulk deletion.

### Does `--all` delete every archive?

No.

For storage cleanup, `--all` controls complete review of the selected targets.

It does not independently select the entire raw-data collection.

### Can I undo a deletion?

Do not assume deletion is reversible.

A remote archive may be downloadable again if the provider still offers it, and an external source may still exist independently.

However, neither is guaranteed.

Review the exact target files and their dependencies before applying deletion.

### Does `--testing` prevent deletion?

No.

`--testing` selects the guarded testing database.

A supported cleanup command can still remove real managed files associated with that environment.

---

## 17. Understanding Storage Errors

When a storage operation fails, the most useful next step depends on the type of problem.

| Problem | What to investigate |
|---|---|
| Archive not found | Check the local ID, selected data root, and registry. |
| File marked missing | Inspect the registered path and run verification if appropriate. |
| Checksum mismatch | Check whether the file was changed or corrupted; consider reacquisition. |
| Coverage gap | Review the selected interval and map the missing period through acquisition. |
| Unknown coverage | Check whether the source has reliable historical interval metadata. |
| Interrupted download | Inspect the run report and retained reservation information. |
| Reconciliation unsuccessful | Check whether the required durable verification evidence exists. |
| Cleanup blocked | Inspect dependencies, ownership, paths, and changed target state. |
| Database rejected | Verify the testing database URL, identity, and migrations. |

Avoid manually changing database statuses or moving managed files to make a failing operation appear successful.

Use the supported inspection and recovery commands so that the registry and filesystem remain consistent.

### Exit codes

Storage and cleanup commands use the common MarketForge CLI error handling.

| Code | Meaning |
|---|---|
| `0` | The operation or preview completed successfully. A successful preview does not mean deletion was applied. |
| `1` | An applicable verification, recovery, or cleanup operation was unsuccessful or incomplete. |
| `2` | A command, configuration, database, filesystem, ownership, or setup error was reported. |
| `130` | The command was interrupted through the handled cancellation path. |

Inspect detailed outcomes rather than relying only on the exit code.

For example, a cleanup preview with no eligible targets can complete successfully without deleting anything.

---

## 18. What Comes After Storage?

Once you know which raw files are available, you can decide what to do next.

If files are missing, return to acquisition and request the appropriate historical period.

If the files are present but have not been processed, continue to normalization.

If normalization has already completed, use the dataset tools to inspect the resulting Parquet data.

| Next task | Where to go |
|---|---|
| Find and download missing historical archives | [Historical data acquisition](acquisition.md) |
| Configure acquisition requests | [Acquisition configuration](../../configuration/acquisition.md) |
| Understand supported exchange archives | [Historical data sources](../../workflows/acquisition/historical-data.md) |
| Investigate interrupted acquisition runs | [Managing acquisitions](../../workflows/acquisition/managing-acquisitions.md) |
| Normalize registered raw files | [Historical normalization](../../workflows/normalize.md) |
| Inspect normalized Parquet datasets | [Dataset inspection](../../workflows/datasets.md) |
| Register your own market-data recordings | [Bring your own data](../../examples/bring-your-own-data/index.md) |

### The important distinction

**Acquisition** helps you obtain historical files.

**Storage** helps you understand and manage the raw files you already have.

**Normalization** transforms those files into consistent market-data records.

**Dataset inspection** helps you explore and verify the resulting data.

Together, these capabilities form the historical data workflow in MarketForge.
