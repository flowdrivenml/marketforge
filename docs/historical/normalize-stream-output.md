# Normalized stream output

Production jobs and sessions schedule independent stream units concurrently, read participating sources forward, and retain long-lived per-stream writers. One reservation covers the largest source estimate plus stream bookkeeping and remains held through writer finalization. Estimates govern admission; they are not OS RSS limits.

Compatible sources have identical resolved logical stream, exchange, instrument identity/specification, normalization rules, ordering policy, profile fingerprint and feed options. Exact-profile compatibility is conservative. Each job/dataset owns separate units. Family expansion keeps concrete instruments independent; family routing shares a forward decode across concrete instruments. Encountered selected instruments require authoritative specifications.

Prepared schema 4 removes external source coverage. Original request-array order
supplies conflict priority; Rust collects bounded source observations and decides
chronological emission. Supported native trade identities use first-source-wins;
ambiguous depth overlap fails safely. Mixed family sources route once into
independent concrete workers. Interleaved/suppressed output uses manifest 4;
legacy contiguous output retains manifest 3. See
[the source/selection contract](normalization-source-selection.md) for exact
selection, safe-emission, resource and downstream compatibility rules.

```
dataset/
  manifest.json                   # protocol 3
  processing-job.json             # exact resolved interpretation and sources
  processing-job.sha256
  streams/<compatibility-sha256>/
    trades/part-NNNNNN.parquet
    trades/sources.json
    # or
    depth/part-NNNNNN.parquet
    depth/events.parquet
    depth/segments.json
    depth/boundary.json
    depth/sources.json
```

Logical stream IDs remain unchanged; filesystem keys never directly embed source IDs. Canonical columns are unchanged. Sources do not force flush, rotation or finalization. Existing trade file sizing uses encoded bytes; depth sizing uses accumulated Arrow batch memory. Targets are approximate. Parts are lazily opened, avoiding empty trailing files. Depth flushes complete outcome batches and event-index groups; rotation never changes reconstruction state.

Manifest3 adds ordered `streams` entries with stream identity, directory and `sources` containing `task_id`, `start_row`, `end_row`. Ranges are half-open within ordered canonical stream parts. A task can span files; a file can contain multiple tasks. Physical files are counted once. `sources.json` is the completion inventory read to build the authoritative manifest; source archive/member/profile identities remain in the checksum-protected resolved job.

Depth retains the existing event-index schema: `event_ordinal` is source-local, not a globally unique event number. The zero-based physical row position in `events.parquet` is the stream-global output event ordinal, including zero-change events. `canonical_row_offset` addresses stream-global canonical level rows; zero-change events have empty ranges.

Depth `boundary.json` version2 contains `continuity: independent_sources` and ordered `sources`. Each source has task ID, half-open canonical row and event-index ranges, and its full version1 `boundary` evidence. Original archive member and source-task identity are resolved through processing-job.json. Boundary objects are serialized incrementally so completed source books are released. No cross-source book-state transfer or continuity claim occurs.

`segments.json` retains version1 and its established meaning: offsets address the owning writer's canonical row space. Initialization and recovery segments are globally rebased automatically by the stream writer. Source ranges identify their owner; IDs are stream-global consecutive integers. Metadata limits reject excessive segment accumulation rather than allowing unbounded growth. Task reports add optional `accepted_depth_events`; older reports deserialize with null. Existing canonical row counters retain their meanings.

Merge readers accept manifest2 task output and manifest3 stream output. New depth inventories project source-local views of shared canonical files, events and segments. Materialization uses physical row slices and source-scoped event cursors. Low-level `DepthEventCursor::open` exposes the whole index; consolidated consumers should use `for_task`/`open_source`. Unified merge still supports one logical instrument stream per input dataset and rejects multi-instrument inputs explicitly. Conversion consumes merge output and resolves instrument interpretation from checksum-validated committed jobs referenced by the merge manifest, retaining legacy private-job fallback. Its algorithms are unchanged.

Failed source, writer or metadata operations prevent dataset commit. Finalization checks source contributions, persisted footer rows, event-index coverage, local ordinal monotonicity, initialization segments, source boundaries and accepted-event metrics. Existing committed destinations are refused and commit remains same-filesystem rename. No stronger crash-durability guarantee is claimed.

When trade timestamps tie, captured original request priority and source-record
ordinals determine output order. Task IDs do not establish priority. Ambiguous
cross-source equal-time depth books fail; retired partitions do not establish
synchronization. `CanonicalReferenceResolver::register_source_task` accepts physical
source slices; legacy `register_task` retains complete-file validation.
