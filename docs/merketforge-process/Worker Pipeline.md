A processing worker converts one raw `WorkTask` into validated, ordered canonical temporary runs.

```text
WorkTask
    ↓
Open Source
    ↓
Stream Decompress / Unpack
    ↓
Parse Raw Records
    ↓
Normalize
    ↓
Validate
    ↓
Bounded RAM Batch
    ↓
Sort if Required
    ↓
Temporary Arrow Run
    ↓
Repeat Until EOF
    ↓
TaskResult
```

### WorkTask

A task provides everything required for processing:

```text
input path
format code
exchange
instrument ID
instrument specs
normalization rules
memory budget
scratch path
```

Workers do not query PostgreSQL or perform metadata discovery.

### Bounded Processing

Records are processed incrementally rather than loading the complete input into memory.

```text
raw record
    ↓
canonical event/group
    ↓
bounded buffer
    ↓
run-00000.arrow

continue
    ↓
run-00001.arrow
...
```

Already ordered formats preserve source order. Sorting is performed only when required by the source format.

One native source event may produce multiple canonical L2 mutations; these remain one logical event group.

### TaskResult

The worker returns metadata rather than the processed records:

```text
task ID
temporary runs
record counts
timestamp range
input bytes
temporary bytes
integrity summary
status
```

The worker does **not** perform:

```text
cross-input merging
BookStore reconstruction
cross-market transformations
final Parquet writing
manifest generation
database updates
```

Those belong to the dataset-level pipeline.

> Worker responsibility: raw source → validated, ordered canonical run(s).