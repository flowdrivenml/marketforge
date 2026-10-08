
**Observation:** MarketForge separates raw archive normalization from final dataset construction.

The existing processing protocol reserves `ProcessingJob.time_range` for merge operations. Raw processing jobs cannot define time-range clipping.

### Processing Policy

**PROCESS — Normalize complete source archives**

- Decode and normalize all records belonging to the requested instrument.
- Apply instrument identity filtering when necessary.
- Preserve the source archive's full timestamp coverage.
- Write canonical datasets without time-range clipping.

**MERGE — Construct the requested dataset**

- Read normalized canonical datasets.
- Apply the requested time interval.
- Establish deterministic timestamp ordering.
- Merge multiple streams when required.

### Time-Range Semantics

Merge operations use half-open UTC nanosecond intervals:

```text
start_timestamp_ns <= event_timestamp_ns < end_timestamp_ns
```

The start boundary is inclusive; the end boundary is exclusive.

This prevents overlapping records between adjacent time windows.

### Example — Gate.io Monthly Archives

```text
Gate.io September archive
          ↓
PROCESS
          ↓
Canonical September dataset
          ↓
MERGE
          ↓
Filter September 1–4
          ↓
Final time-bounded dataset
```

A monthly archive is normalized once and can subsequently support multiple time-window queries without repeating raw decoding.

### Decision

**Keep time-range filtering exclusively in the merge stage.**

Do not introduce time-range clipping into the trade worker or modify the existing processing-job protocol.

The trade worker remains responsible for complete source normalization, while the merge engine handles time boundaries, ordering, and dataset construction.