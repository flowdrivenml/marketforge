`marketforge-process` may expose an optional live terminal dashboard for monitoring processing performance, resource usage, and dataset integrity.

## Quick Navigation

- [Metrics](#metrics)
- [Data Integrity](#data-integrity)
- [Architecture](#architecture)
- [Terminal Dashboard](#terminal-dashboard)
- [Monitoring Modes](#monitoring-modes)
- [Manifest Integration](#manifest-integration)
- [Core Rules](#core-rules)

## Metrics

Monitoring combines MarketForge processing metrics with system/process metrics.

### MarketForge Metrics

```text
overall progress
elapsed time
ETA
current stage

records processed
records/sec
input bytes
input throughput
output bytes
output throughput

active workers
completed tasks
active streams
active books

temporary runs
temporary storage

Parquet parts
Parquet output size
```

### System Metrics

```text
CPU utilization
memory / RSS
disk read throughput
disk write throughput
```

These metrics help identify whether processing is:

```text
CPU-bound
memory-bound
I/O-bound
writer-bound
```

## Data Integrity

Data integrity is a first-class processing metric and must be visible immediately while a dataset is being produced.

Track at least:

```text
sequence gaps
duplicate sequences
timestamp regressions
parse failures
invalid records
invalid prices
invalid quantities
missing required fields
missing initial snapshots
book reconstruction failures
crossed books
```

The monitor maintains both:

```text
total issue count
affected instrument count
```

and retains recent issue details for inspection.

Example clean state:

```text
INTEGRITY
────────────────────────────────────
Status              ✓ CLEAN
Sequence gaps       0
Timestamp regress   0
Invalid records     0
Parse failures      0
Book errors         0
Crossed books       0
```

If a problem is detected:

```text
INTEGRITY
────────────────────────────────────
Status              ✗ DEGRADED
Sequence gaps       3
Timestamp regress   0
Invalid records     17
Parse failures      2
Book errors         1
Crossed books       0

Affected instruments  2

Latest issue:
Binance BTCUSDT depth
expected sequence: 829173
received sequence: 829175
```

Integrity problems are reported by the processing validation layers:

```text
Parser ─────────────┐
Normalizer ─────────┤
Sequence Validator ─┼→ IntegrityMetrics
Stream Validator ───┤
BookStore ──────────┘
```

Known integrity problems must never be silently treated as clean data.

## Architecture

Processing components update shared metrics:

```text
Workers ──────┐
Merger ───────┤
BookStore ────┼→ ProcessingMetrics
Parquet ──────┤
Runs ─────────┘

Validation ───┐
Parser ───────┼→ IntegrityMetrics
BookStore ────┘
```

Simple counters should use lightweight atomics where practical.

A sampler reads the current state periodically:

```text
ProcessingMetrics
IntegrityMetrics
        ↓
sample every ~1 second
        ↓
MetricsSampler
        ↓
bounded history
```

Historical samples are stored in small ring buffers:

```text
CPU
memory
records/sec
input MB/sec
output MB/sec
disk read/write
```

Only the most recent monitoring window is retained, for example:

```text
60–120 seconds
```

Monitoring memory usage therefore remains negligible.

## Terminal Dashboard

A `ratatui`-based dashboard can display current values, short historical charts, and integrity status:

```text
MarketForge Process ───────────────────────── 47.3%
███████████████████████░░░░░░░░░░░░░░░░░░

Elapsed    00:18:42       ETA        00:20:51
Records    1.82B          Rate       9.7M/s
Input      182 GB         Output     61 GB
Workers    12/12          Books      37

INTEGRITY
Status              ✓ CLEAN
Sequence gaps       0
Invalid records     0
Book errors         0

CPU
100% │      ╭──╮ ╭─────╮
 75% │ ╭────╯  ╰─╯     ╰──╮
 50% │─╯                  ╰──
  0% └────────────────────────

MEMORY
8 GB │                 ╭─────
6 GB │          ╭──────╯
4 GB │    ╭─────╯
2 GB │────╯
0 GB └────────────────────────

THROUGHPUT
12M/s│       ╭────╮
 9M/s│ ╭─────╯    ╰────╮
 6M/s│─╯               ╰──
    └────────────────────────

Disk Read    780 MB/s
Disk Write   430 MB/s
Temp         28.1 GB
Parquet      61.4 GB / 123 parts
```

Integrity status should remain prominently visible throughout processing.

## Monitoring Modes

The same metrics layer may support multiple renderers:

```text
ProcessingMetrics
IntegrityMetrics
       │
       ├── simple progress line
       ├── terminal dashboard
       └── structured JSON progress
```

Example modes:

```text
none
simple
tui
json
```

The TUI is optional and must not be required for normal processing.

JSON progress is useful when Python launches `marketforge-process` and wants to display or record progress itself.

External tools remain useful for additional system inspection:

```bash
htop
iotop
iostat -xz 1
```

## Manifest Integration

Integrity results are preserved after processing rather than existing only in the live monitor.

`manifest.json` contains an integrity summary:

```json
{
  "integrity": {
    "status": "clean",
    "sequence_gaps": 0,
    "duplicate_sequences": 0,
    "timestamp_regressions": 0,
    "invalid_records": 0,
    "parse_failures": 0,
    "book_errors": 0,
    "crossed_books": 0,
    "affected_instruments": 0
  }
}
```

A dataset with known problems may instead report:

```text
status = degraded
```

The manifest therefore allows downstream consumers to determine whether the dataset passed MarketForge integrity checks without rerunning processing.

## Core Rules

- monitoring is independent from processing logic
- processing and integrity metrics use shared observable state
- integrity status is visible throughout processing
- known gaps and validation failures are never silently ignored
- recent integrity failures retain enough context to identify the affected source
- final integrity statistics are preserved in `manifest.json`
- all renderers consume the same metrics state
- monitoring must have negligible impact on processing throughput
- metric history uses bounded ring buffers
- sampling occurs periodically rather than on every rendered frame
- processing threads never block on dashboard rendering
- system and MarketForge metrics are shown together
- the TUI remains optional for scripts, CI, and non-interactive execution