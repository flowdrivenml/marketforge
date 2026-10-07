MarketForge separates **execution state** from **dataset integrity** and never exposes partially generated output as a completed dataset.

## Quick Navigation

- [Execution Status](#execution-status)
- [Staging](#staging)
- [Successful Publication](#successful-publication)
- [Failure](#failure)
- [Retry](#retry)
- [Crash Recovery](#crash-recovery)
- [Integrity Status](#integrity-status)
- [Core Rules](#core-rules)

## Execution Status

Dataset execution uses:

```text
pending
processing
complete
failed
```

Normal lifecycle:

```text
pending
    ↓
processing
    ↓
complete
```

Failure:

```text
pending
    ↓
processing
    ↓
failed
```

Integrity is tracked separately and does not replace execution status.

## Staging

Incomplete output is never written directly into the final dataset directory.

During processing:

```text
data/
├── jobs/
│   └── 42.json
│
├── .work/
│   └── 42/
│       ├── tasks/
│       ├── runs/
│       └── output/
│           ├── part-00000.parquet
│           └── part-00001.parquet
│
└── datasets/
```

All incomplete processing state belongs under:

```text
data/.work/{dataset_id}/
```

The final:

```text
data/datasets/{dataset_id}/
```

does not exist until publication succeeds.

## Successful Publication

Rust completes:

```text
workers
    ↓
temporary runs
    ↓
merge
    ↓
Parquet parts
    ↓
integrity summary
    ↓
manifest.json
```

The completed staging output becomes:

```text
.work/42/output/
├── manifest.json
├── part-00000.parquet
├── part-00001.parquet
└── ...
```

Only after everything succeeds:

```text
.work/42/output/
        ↓
atomic publish
        ↓
datasets/42/
```

Rust then returns a successful `ProcessingResult`.

Python updates:

```text
processing
    ↓
complete
```

Temporary processing state is removed after successful publication.

## Failure

Fatal processing failure stops the job:

```text
processing
    ↓
failure
    ↓
cancel remaining work
    ↓
failed
```

Partial output remains only under:

```text
data/.work/{dataset_id}/
```

No final dataset is published.

During development, failed scratch data may be retained for debugging.

## Retry

Initial retry behavior is deliberately simple:

```text
failed job
    ↓
remove/reset .work/{id}
    ↓
load existing ProcessingJob
    ↓
process again from immutable inputs
```

V1 does not attempt:

```text
partial-task reuse
run caching
checkpointed merging
partial processing resume
```

These may be added later as execution optimizations without changing dataset semantics.

## Crash Recovery

A crash may leave:

```text
catalog status = processing
```

and:

```text
data/.work/{id}/
```

without an active Rust process.

This is treated as stale execution state.

The job can be reset and rerun from its persistent job specification.

A different case is:

```text
Rust publishes datasets/42/
    ↓
Python crashes before PostgreSQL update
```

If:

```text
datasets/42/
└── manifest.json
```

is complete and valid, MarketForge can reconcile the catalog and mark the dataset complete rather than regenerating valid output.

The published manifest therefore acts as the physical dataset commit marker.

## Integrity Status

Execution and integrity are independent:

```text
Execution:
    pending
    processing
    complete
    failed

Integrity:
    clean
    degraded
```

For example:

```text
execution = complete
integrity = degraded
```

means processing successfully produced a dataset, but known integrity problems were accepted under the configured policy.

PostgreSQL may therefore contain:

```text
catalog.datasets.status = complete
```

while the manifest contains:

```json
{
  "integrity": {
    "status": "degraded"
  }
}
```

## Core Rules

- incomplete datasets are never published under `data/datasets/`
- all temporary execution state belongs under `.work/`
- persistent job specifications remain under `jobs/`
- final Parquet files and manifest are completed before publication
- dataset publication is atomic where supported by the filesystem
- `manifest.json` acts as the physical completion marker
- PostgreSQL tracks control-plane execution state
- integrity status remains separate from execution status
- successful processing may produce either `clean` or `degraded` data
- failed processing never publishes a completed dataset
- scratch data is removed after successful publication
- failed scratch may be retained for debugging
- V1 retries processing from the beginning
- resumable processing and task caching are future optimizations
- valid published output can be reconciled if Python crashes before updating PostgreSQL

> Build privately under `.work`, publish only when complete, and keep execution success separate from data integrity.