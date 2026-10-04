MarketForge stores immutable exchange archives separately from processed datasets.

```text
data/
├── raw/
└── datasets/
```

Raw storage preserves the original exchange files. Processed storage contains canonical and merged Parquet datasets produced by `marketforge-process`.

## Dataset Layout

Each processed dataset receives one stable root directory based on its catalog dataset ID:

```text
data/datasets/{dataset_id}/
```

Example:

```text
data/datasets/42/
├── manifest.json
├── part-00000.parquet
├── part-00001.parquet
└── part-00002.parquet
```

The directory represents the complete dataset time range.

Calendar boundaries do not create mandatory directories:

```text
date=2026-09-01/
date=2026-09-02/
...
```

are not required.

Dataset semantics and lineage are stored in the MarketForge catalog rather than encoded into increasingly complex filesystem paths.

## Parquet Parts

Parquet files are split primarily by **target physical file size**, not by day or processing batch.

Example:

```text
Dataset range:
2026-09-01 → 2026-09-08

part-00000.parquet
    Sep 01 → Sep 03
    ~500 MiB

part-00001.parquet
    Sep 03 → Sep 06
    ~500 MiB

part-00002.parquet
    Sep 06 → Sep 08
    ~280 MiB
```

A small dataset may remain a single file:

```text
data/datasets/42/
├── manifest.json
└── part-00000.parquet
```

A very active instrument may require many files within a single day.

The target size is approximate. Files rotate at safe boundaries such as completed row groups rather than attempting to hit an exact byte count.

Initial target:

```text
~256–512 MiB per Parquet file
```

The final value should be benchmarked.

## Memory and File Size

Processing memory and Parquet file size are independent.

```text
Raw Input
    ↓
bounded processing batch
    ↓
Parse
    ↓
Normalize
    ↓
Sort / Merge
    ↓
Arrow RecordBatch
    ↓
Parquet writer
```

Multiple in-memory batches may be written into the same Parquet file:

```text
batch ─┐
batch ─┤
batch ─┼→ part-00000.parquet
batch ─┘

next batches
    ↓
part-00001.parquet
```

The core rule is:

> Memory pressure determines processing batch size. Storage policy determines Parquet file size.

A processing batch must never automatically become a separate Parquet file.

This avoids the small-files problem.

## Chronological Ordering

Every dataset part represents a contiguous chronological range.

For adjacent files:

\[
\max(T_i)
\le
\min(T_{i+1})
\]

Conceptually:

```text
part-00000
T0 ───────────── T1

part-00001
                 T1 ───────────── T2

part-00002
                                  T2 ───────────── T3
```

Files must not be split according to arbitrary worker completion if doing so mixes unrelated timestamp ranges.

For combined datasets, trades and depth remain in one chronological event stream:

```text
L2Snapshot
L2Update
Trade
L2Update
Trade
L2Update
...
```

Each event retains its exchange and instrument identity.

This allows downstream consumers such as `mlfindgen` to replay one ordered stream while maintaining current book state.

## Parquet Row Groups

Each Parquet file contains multiple row groups.

```text
part-00000.parquet
├── row group 0
├── row group 1
├── row group 2
└── row group 3
```

Row groups provide a smaller physical/query unit than files.

For chronological data they naturally contain timestamp ranges:

```text
row group 0
09:00 → 09:10

row group 1
09:09:10 → 09:20

row group 2
09:20 → 09:30
```

Parquet statistics can therefore expose:

```text
min(event_timestamp_ns)
max(event_timestamp_ns)
```

for efficient range pruning.

Initial targets may be approximately:

```text
Parquet file     ~256–512 MiB
Row group        ~64–128 MiB
Processing RAM   independently configurable
```

These are performance settings rather than canonical format requirements and should be benchmarked.

## Dataset Manifest

Every processed dataset contains:

```text
manifest.json
```

The manifest is a lightweight dataset-level index.

Example:

```json
{
  "dataset_id": 42,
  "schema_version": 4,
  "content_type": "combined",
  "start_timestamp_ns": 1790000000000000000,
  "end_timestamp_ns": 1790600000000000000,
  "row_count": 1842395012,
  "files": [
    {
      "path": "part-00000.parquet",
      "start_timestamp_ns": 1790000000000000000,
      "end_timestamp_ns": 1790184200123456789,
      "row_count": 621440123,
      "size_bytes": 528341221
    },
    {
      "path": "part-00001.parquet",
      "start_timestamp_ns": 1790184200123456790,
      "end_timestamp_ns": 1790412345987654321,
      "row_count": 608120442,
      "size_bytes": 531284991
    },
    {
      "path": "part-00002.parquet",
      "start_timestamp_ns": 1790412345987654322,
      "end_timestamp_ns": 1790600000000000000,
      "row_count": 612834447,
      "size_bytes": 491223810
    }
  ]
}
```

The manifest allows consumers to determine:

```text
dataset time range
file ordering
file time ranges
row counts
file sizes
```

without opening every Parquet footer.

A consumer requesting:

```text
T1 → T2
```

can first inspect the manifest:

```text
requested range
    ↓
manifest
    ↓
find overlapping parts
    ↓
read only required Parquet files
```

The manifest should remain small. Detailed physical statistics remain inside Parquet.

## Dataset Completion

`marketforge-process` writes the manifest only after all Parquet parts have been successfully finalized.

During processing:

```text
data/datasets/42/
├── part-00000.parquet
├── part-00001.parquet
└── manifest.json.tmp
```

After successful completion:

```text
finish Parquet files
    ↓
finalize manifest
    ↓
atomic rename
    ↓
manifest.json
```

Final state:

```text
data/datasets/42/
├── manifest.json
├── part-00000.parquet
└── part-00001.parquet
```

A dataset is considered physically complete only when its final:

```text
manifest.json
```

exists.

This makes interrupted or partially written datasets easy to detect.

The catalog status follows the processing lifecycle:

```text
pending
    ↓
processing
    ↓
complete
```

or:

```text
processing
    ↓
failed
```

The database should only mark a dataset `complete` after the final manifest has been committed.

## Storage Responsibilities

Storage metadata is divided between three layers.

### PostgreSQL Catalog

PostgreSQL is the authoritative MarketForge catalog.

It stores:

```text
dataset identity
dataset kind
content type
time range
storage root
processing status
dataset lineage
dataset_inputs
```

It answers:

> What is this dataset and where did it come from?

### Dataset Manifest

`manifest.json` is the portable dataset index.

It stores:

```text
dataset ID
schema version
content type
overall time range
file ordering
per-file time ranges
row counts
file sizes
```

It answers:

> What physical files make up this dataset and which ones do I need?

### Parquet

Each Parquet file remains independently self-describing.

It stores:

```text
physical schema
row groups
column statistics
compression metadata
min/max values
record data
```

It answers:

> What exactly is contained in this physical file?

The separation is:

```text
PostgreSQL
    → catalog + lineage

manifest.json
    → dataset-level file index

Parquet
    → physical data + file-level metadata
```

## Storage Model

The final storage model is:

```text
data/
├── raw/
│   └── immutable exchange-native archives
│
└── datasets/
    ├── 42/
    │   ├── manifest.json
    │   ├── part-00000.parquet
    │   ├── part-00001.parquet
    │   └── part-00002.parquet
    │
    ├── 43/
    │   ├── manifest.json
    │   └── part-00000.parquet
    │
    └── ...
```

Core rules:

- one directory represents one logical dataset
- the dataset directory may cover any time range
- calendar-day partitioning is not mandatory
- small datasets remain one Parquet file
- large datasets split into size-bounded chronological parts
- processing batches do not determine file boundaries
- row groups are independent from processing batches and file boundaries
- Parquet parts remain chronologically ordered
- combined trade + depth datasets remain one chronological event stream
- `manifest.json` indexes the physical parts
- PostgreSQL tracks dataset identity and lineage
- Parquet remains independently self-describing
- a final manifest marks successful physical dataset completion