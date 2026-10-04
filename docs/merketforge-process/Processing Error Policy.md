
MarketForge distinguishes between **fatal source failures** and **data-integrity problems**.

The processor must never silently convert known incomplete or invalid input into a clean dataset.

## Quick Navigation

- [Fatal Source Failures](#fatal-source-failures)
- [Preflight Validation](#preflight-validation)
- [Integrity Problems](#integrity-problems)
- [Fixed Correctness Rules](#fixed-correctness-rules)
- [Configurable Policy](#configurable-policy)
- [Duplicate Detection](#duplicate-detection)
- [Manifest](#manifest)

## Fatal Source Failures

Required source/archive failures always terminate the processing job.

Examples:

```text
missing required archive
unreadable archive
corrupt archive
missing required archive member
decompression failure
unsupported physical source structure
```

Policy:

```text
source failure
    ↓
FAIL JOB
```

An entire required archive is never silently skipped or treated as a degraded dataset.

## Preflight Validation

Before workers begin expensive processing, MarketForge performs cheap structural validation:

```text
ProcessingJob
    ↓
Preflight
    ↓
file exists
file readable
container/header valid
expected archive members present
basic source structure valid
    ↓
PASS
```

Preflight should avoid fully decompressing large archives solely for validation.

Some corruption can only be discovered while streaming the complete source:

```text
Preflight
    → structural checks

Worker
    → full streaming decompression
    → full integrity verification
```

If corruption is discovered during worker processing:

```text
decompression / archive integrity failure
    ↓
cancel processing
    ↓
FAIL JOB
```

## Integrity Problems

Record-level and market-data integrity problems are different from source failures.

Examples:

```text
malformed record
invalid canonical record
sequence gap
timestamp regression
missing required snapshot
invalid / crossed book
transformation gap
```

These may resolve to:

```text
DEGRADED
```

or:

```text
FAIL
```

according to the effective processing policy.

Known integrity problems are always recorded.

## Fixed Correctness Rules

Some behavior is not configurable.

```text
invalid canonical record
    → never write invalid canonical data

sequence gap
    → affected BookState becomes invalid

invalid BookState
    → never use it for state-derived transformations

missing required transformation input
    → never fabricate derived values

source/archive corruption
    → fail job

unknown or ambiguous market events
    → never fabricate missing information
```

Configuration may decide whether some integrity conditions terminate the job, but it cannot override these correctness guarantees.

## Configurable Policy

Integrity policy belongs to `ProcessingJob`.

Conceptually:

```json
{
  "integrity_policy": {
    "profile": "standard",
    "parse_failure": {
      "action": "degrade",
      "max_count": 100
    },
    "invalid_record": {
      "action": "degrade"
    },
    "sequence_gap": {
      "action": "degrade"
    },
    "timestamp_regression": {
      "action": "degrade"
    },
    "missing_snapshot": {
      "action": "degrade"
    },
    "invalid_book": {
      "action": "degrade"
    },
    "transformation_failure": {
      "action": "fail"
    }
  }
}
```

Initial actions:

```text
degrade
fail
```

MarketForge may provide default profiles such as:

```text
standard
strict
```

Python resolves defaults and overrides before launching Rust:

```text
defaults
    +
CLI/config overrides
    ↓
effective integrity policy
    ↓
ProcessingJob
    ↓
marketforge-process
```

## Duplicate Detection

MarketForge performs **no generic duplicate detection or deduplication**.

Do not infer duplicates from combinations such as:

```text
same timestamp
same price
same quantity
same side
```

Identical-looking records may represent legitimate independent market events.

Generic processing therefore has:

```text
no duplicate heuristic
no duplicate counter
no duplicate policy
no automatic deduplication
```

Format-specific sequence and continuity validation remains allowed where explicitly defined by the source semantics, but it is not treated as generic duplicate detection.

## Manifest

The effective integrity policy and resulting integrity summary are preserved in `manifest.json`.

Conceptually:

```json
{
  "integrity": {
    "status": "degraded",
    "policy": {
      "profile": "standard"
    },
    "results": {
      "parse_failures": 2,
      "invalid_records": 0,
      "sequence_gaps": 1,
      "timestamp_regressions": 0,
      "book_errors": 0
    }
  }
}
```

This preserves both:

```text
what integrity problems occurred
```

and:

```text
under which policy the dataset was accepted
```

Core rule:

> Fatal source failures stop processing. Data-integrity problems are explicit and policy-controlled. Known incomplete, invalid, or ambiguous data is never silently represented as clean.