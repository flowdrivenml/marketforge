# Historical normalization execution and performance

Normalization selects a capability-based stream path after source-local processing. Exchange names and archive dates do not select fast paths. Canonical Parquet schemas, identity namespaces, original timestamps, policy decisions and transactional publication remain unchanged. The [November 17 measurements](normalization-performance-20261117.md) use exactly two trade and two depth archives.

## Execution paths

| Diagnostic path | Eligibility and work |
| --- | --- |
| `chronological_append` | Existing explicit source-local trade identity contract, strict policies and validated strictly disjoint source time intervals. One sealed source-spool write/read; stream directly to the long-lived Parquet writer. |
| `ordered_run_append` | Compatible checked `ordered_run` domain, strict timestamp processing, no rejected observations, every source's timestamps nondecreasing and decimal IDs strictly increasing, and ordered source ID ranges strictly disjoint. Time boundaries may be equal. One spool write/read; no sort or priority rewrite. |
| `ordered_run_merge` | Same complete ingress ordering evidence, overlapping ranges and enough admitted cursors. A bounded chronological heap merge verifies the entire unique native-ID union, checks complete duplicate contents and rejects resets/conflicts. One survivor spool seals duplicate accounting before publication; no full sort. |
| `identity_reconciliation` | General external fallback for other contracts, repeated/missing/unbounded evidence IDs, timestamp policies, unordered sources or insufficient cursors. Frozen source ranks attach during initial identity-run decoding, avoiding a standalone spool rewrite. Existing identity, chronological-union, native-order/loss and output-order validations remain. Final admitted merge cursors feed Parquet directly. |
| `independent_depth_continuation` | Each source owns one native-order interval. Each initializes from its own authoritative snapshot; applicable continuity/recovery requirements still run. Full payload replay occurs once per source. Independent histories can record unproven continuity. |
| `checkpointed_depth_selection` | A source owns multiple intervals. One complete native-order traversal creates sealed forward partitions with exact pre-event checkpoints. Replay restores each checkpoint and consumes its partition instead of reopening the original prefix for every return. |

Ingress native-ID evidence retains at most 128 bytes each for first/last IDs. This is an evidence-cache bound: larger IDs retain their exact canonical values and take the general path. All source records validate the ordering claim; min/max alone never authorizes continuation. Cross-source domain/profile/instrument compatibility is checked first. Numeric comparison never uses floating point or lexical number ordering. Scoped/global identities still require conflict checking across disjoint time ranges. Timestamp discard cannot remove evidence before identity validation.

Duplicate ownership in checked merge and general reconciliation uses the same actual coverage-start order and stable source/member tie-break. Native IDs order executions; they do not rank source quality. The shared admitted coverage sweep builds source-sized ownership intervals in O(S log S); event ownership queries use binary lookup. No event-sized global identity map is introduced.

## Exact depth invariant

Replay maintains a source book and a selected book. An exact full-state comparison establishes equivalence at initialization or a required transition. Applying the same absolute changes to equal states changes only the declared keys; ordinary uninterrupted events validate those keys in O(changed levels × log book size). Untouched keys remain equal by induction. Zero-row events preserve this invariant.

Excluded state-changing observations, recovery and source switches invalidate equivalence. Validated initialization or an independently proved transition must re-establish it. Full snapshots and genuine boundary proofs may process the complete book. `BookStore::replace_snapshot` validates and installs a full state atomically without allocating an unused diff. Replay never sorts stateful deltas by timestamp and never substitutes a probabilistic book fingerprint for exact equality.

Fragmented ownership partitions retain intervening native events, including events outside an interval's timestamp bounds. Regressing timestamps therefore cannot make a timestamp-based early stop skip a later source ordinal. A return that would reorder native source segments still fails. One original payload traversal plus forward partition replay bounds payload decoding by twice the source event count; checkpoints cost O(book levels × switches) and remain subject to existing frame, scratch, memory and inventory limits. This does not promise constant work for arbitrarily many switches.

Depth accounting reads a compact sidecar containing original index, source metadata, operation, boundary, expanded row count and payload-frame checksum. Sealed counts, monotone indexes, frame checksums, completion footers and a writer-held metadata-chain value bind it to its payload. Used payloads are paired with metadata; wholly unselected payloads receive checksum/footer validation without level decoding. Accounting retains native-order contiguous loss spans and never decodes full snapshot levels solely to count them.

## Shared preparation and ownership

Independent concrete sources can use up to two preparation pipelines per stream, limited by `resources.workers - 1`, memory/file admission and **spare** permits from the existing session compute budget. A parent never waits for a permit needed by a child. Source parsing, normalization, sequence/book validation and decompression run on their owning source worker; canonical publication remains deterministic on the stream worker.

Each pipeline has a one-event bounded queue. Admission includes the additional source workspace, cloned prepared task, observation maps, queued/blocked atomic outcomes, boundary data and handle headroom. Memory/file pressure falls back to serial preparation; it does not raise budgets. Queues are disconnected before joins on cancellation. Owned canonical events and unsealed metric maps cross threads; `Rc` scratch/retained handles and books do not. Observation spills remain on the consuming worker. No separate scratch budget or unsafe `Send` implementation is added.

Family or shared-physical-path groups retain the existing bounded archive-routing path instead of reopening one physical archive in independent pipelines. Multi-member and mixed-instrument synthetic tests cover this contract. Discovery and routing remain separate validated phases; combined discovery/routing caching is not implemented.

Successful integrity categories from an event share common scope/window resolution and checked statistics copies. Immediate fatal observations remain immediate. Old/unattributed windows use the checked fallback. Provenance validation reuses its frame buffer and indexes borrowed source spans by task, then binary-searches their existing sorted row ranges. It does not clone a report-wide range inventory.

## Scratch options and bounds

```toml
schema_version = 1
merge_fan_in = 8                 # 2..64, admitted upper bound
scratch_format = "json"         # json | typed_trades | typed_events
scratch_codec = "zlib"          # zlib | uncompressed

[diagnostics]
mode = "disabled"               # disabled | basic | detailed
interval_ms = 1000               # 100..60000
```

Defaults and existing user configuration are unchanged. `typed_trades` retains its existing meaning. Opt-in `typed_events` adds shared-envelope typed depth payloads and also enables typed trades; dynamic routing, boundaries and reports remain JSON. Depth tag/version `[0,2]` is distinct from trade `[0,1]` inside the existing `MFSP0002` framed container. Options, integer widths, exact decimal mantissa/scale/sign, original source positions and zero-row outcomes round-trip. Mixed event envelopes use exact JSON fallback. Unknown tags, bad lengths/options, trailing data, corrupt frames and incomplete footers fail.

Encoding and compression are independent. Typed decoded-memory admission includes the level vector and repeated envelope strings before allocation; encoded buffers retain separate leases. JSON's conservative reservation remains. The configured frame cap remains `reconciliation_bytes / 128`. No chunking, truncation, frame-size switch or budget reduction is introduced. Source spools remain disk-backed, including small workloads: a bounded in-memory/spill representation is future work, not a completed optimization.

## Diagnostics and interpretation

Enabled diagnostics retain `effective-config.json`, coalesced `live.jsonl` and final `performance.json` in a unique `performance-*` directory beside staging. Source preparation, routing, replay and finalization use existing worker-side phases. Source snapshots expose active preparation, records and validated completion; stream snapshots explain execution eligibility/fallback. Manifest source processing summaries add path, full-book comparisons/levels visited, changed-level checks, payload/metadata event reads and checkpoint restores.

Existing scratch counters expose frames, serialization/codec calls, encoded/decoded bytes, runs, merge passes, opens and workspace peaks. Normalized, spooled, staged and committed counts remain separate, including for prefetched sources. Optional diagnostic I/O remains on the reporting thread, never the renderer. No per-record clock/process probe or event backlog is added.

CPU is process CPU, not elapsed time. Phase wall times overlap and must not be summed. Accounted workspace memory is not RSS or whole-stream admission. Logical process I/O includes source, scratch, output and reports; physical device I/O is separate. Disabled diagnostics do not retrospectively provide scratch traffic or exact phase timing. Raw archive-pass counts and fine-grained parse/normalize/encoding CPU attribution are not comprehensively instrumented; source-local read-once structure and operation tests supply structural evidence, not measured phase percentages.

Late identity/sequence, framing, footer, accounting, integrity and publication failures still prevent commit. An interrupted or timed-out job is incomplete, even if it has staged Parquet bytes. Immutable baseline/candidate binaries, exact configurations, four-source allowlists and independent output verification are required for comparisons. A timeout is censored evidence, not a throughput baseline. See the [bounded reproduction report](normalization-performance-20261117.md).
