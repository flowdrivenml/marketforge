MarketForge — Normalization I/O Refactor Design Note 
Recorded: November 6, 2026
Status: Design proposal for a later implementation discussion. No implementation is authorized by this note.
Scope: Historical normalization output ownership, bounded memory use, ordering, depth metadata, and reproducible tests. 
Context and agreed direction 
The user reports a machine with 128 GB RAM and benchmark observations suggesting that I/O, particularly excessive file
counts, is the main performance bottleneck. Those measurements have not been independently reproduced in this review.
MarketForge already has a separate merge implementation; this refactor should not replace it or duplicate its correctness
rules. 
The agreed direction is to decouple source-task boundaries from output-file boundaries. Compatible archives for the same
concrete instrument and data type, inside the same job/dataset, should contribute to shared, size-targeted Parquet output.
Tasks remain independently identifiable for provenance, diagnostics, and progress. 
The schema-version-3 prepared normalization batch remains the external input contract. Profiles are self-contained TOMLs
with IDs of the form exchange.instrument_type.market_category.data_type and filenames of <id>.toml. Runtime
resource settings, including workers and Parquet sizing, are moving to data/.config/historical/general.toml. The
actual contents of general.toml and the current unified-profile loader need inspection before defining new settings. 
The detailed interfaces and policies below are recommendations, not claims that these changes already exist or that the user
has approved every implementation detail. 
What the supplied code establishes 
The latest export is not the Parquet writer 
repomix-output(20261106-194235).xml contains config.rs, decimal.rs, mod.rs, quantity.rs, side.rs, and
timestamp.rs: normalization encodings and conversion helpers. It does not contain the Parquet writer implementations. Its
functions are relevant to value correctness, but not the primary place to change output-file ownership. [S1] 
The earlier normalization export re-exports ParquetTradeWriter and ParquetDepthWriter from
shared::persistence::parquet. Their implementations, buffer measurements, rollover decisions, and event-index
persistence still need inspection. [S2] 
Output ownership currently follows tasks 
normalize/task.rs creates output paths beneath tasks/task-<id>/trades or tasks/task-<id>/depth. Each task
constructs its own writer and finishes that writer before returning. Consequently, separate tasks cannot continue filling the
same file even when they describe the same stream. [S3] 
session.rs schedules independent tasks globally, then groups their results back into jobs. finalize_processing_job
aggregates task metrics, checks integrity, discovers output files, writes the manifest, and commits staging. It explicitly expects
paths beneath tasks/. This path is not an instrument-level compaction pass. [S4] 
Useful abstractions already exist 
The trade and depth workers already accept generic TradeSink and DepthSink implementations. A depth sink accepts a
complete DepthProcessingOutcome, including its source-event boundary information, rather than only isolated level rows.
These interfaces provide a useful starting point for changing persistence without rewriting value conversions. [S5] 
Depth processing currently constructs a new DepthProcessor inside each task’s source-reader invocation, assigns source
ordinals within that task, and emits a completed boundary manifest at task end. Discovery expects events.parquet,
segments.json, and boundary.json alongside each depth task’s canonical parts. [S6] 
The current resource estimator is a scheduling heuristic based on source-file size and Parquet row-group targets. Scheduled
task reservations end when their worker closures return. This is not sufficient accounting for a future pipeline that retains
decoded batches after the producing task returns. [S7] 
Primary design decision: source tasks do not own output filesA task should describe an input archive and its interpretation. A stream execution context should own the output writer. 
Prepared batch + resolved profiles + effective runtime configuration
|
v
Job execution plan
|
+-- Stream group A: source tasks A1, A2, A3
| |
| +-- Decode/process in the permitted source order
| +-- Keep task-specific reports and provenance
| +-- One long-lived output writer
| +-- part-000001.parquet
| +-- part-000002.parquet
|
+-- Stream group B: source tasks B1, B2
|
+-- Separate processing state and output writer

Finishing source A1 should finalize A1’s source accounting, not automatically finalize the writer, flush a short row group, or
create a new output file. Processing can continue with A2 using the remaining capacity of the current output part. 
A row-group flush, output-part rollover, source completion, depth reconstruction boundary, and dataset commit must be
treated as distinct events. 
The intended common path is to write canonical output once into job staging. It should not produce many small per-task
Parquet files merely to read and compact them immediately afterward. 
This targets avoidable output fragmentation. It does not eliminate the need to open and read the original source archives, and
it does not establish a speedup before benchmarking. 
Stream identity and grouping 
Group only compatible data within a dataset 
Use a typed grouping identity that includes the job/dataset, exchange, instrument type, market category, concrete instrument
identity, and canonical data type. Account for feed distinctions and processing semantics when determining compatibility. 
Do not group by symbol alone, profile ID alone, or base/quote assets alone. A profile can cover many symbols. Two
instruments with the same displayed symbol on different exchanges or market categories must not share an output stream. 
Do not combine different jobs merely because their instruments match. Each job retains its own output and failure boundary. 
Snapshot and incremental feeds must not be combined simply by removing a stream-ID suffix. They may be combined only
through a supported, explicit reconciliation policy. Otherwise retain separate groups. 
Resolve family-scoped inputs before output routing 
A family-scoped archive is not necessarily one concrete instrument. Resolve record/member identity and required instrument
metadata before routing records to writers. Do not substitute one task-wide instrument specification for every member of a
family. 
Where a family archive legitimately contains several instruments, prefer one source read with bounded routing to the relevant
stream contexts over reopening the same archive once per instrument. The feasibility and necessary matcher changes require
inspection of the actual resolver and archive reader; this is not established by the supplied helpers. 
Keep interpretation provenance 
Resolve each source against its actual profile and instrument specification. Different physical source formats can contribute to
the same canonical stream if their semantics are compatible, but they still retain their own profile identities/fingerprints and
source lineage. 
A changed contract specification, tick size, or feed meaning requires an explicit compatibility decision. Do not normalize later
archives using cached metadata from the first task without validation.Execution and concurrency 
First implementation: persistent writers, simple ordering 
Start with parallelism across stream groups and sequential processing within each group. This directly removes archive-driven
writer finalization while avoiding an immediate rewrite of the scheduler into an asynchronous dataflow system. 
A proposed StreamExecutionContext owns the writer, output ordering state, metadata accumulators, and resource
reservation. Task-specific decoders and reports are created as needed. The exact Rust type names are illustrative. 
Both single-job and multi-job entry points should use the same underlying execution path. Do not fix consolidation in one
entry point while retaining task-owned writers in another. 
For trades, the existing generic sink parameter should make it possible to process successive archives into a shared writer.
Finalization and aggregate writer validation move above the individual task wrapper. 
For depth, use the explicit metadata and reconstruction design below rather than mechanically reusing the writer. 
Later optimization: bounded decode-ahead 
If benchmarks justify it, introduce bounded prefetch/decode work for upcoming sources while the current source is processed
or written. More RAM can support a larger look-ahead window; it should not require holding the whole batch in memory. 
Use batches of typed records, source messages, or canonical outcomes as appropriate. Do not create a global collection of
individually heap-allocated JSON values for all archives by default. Prefer ownership transfer and avoid retaining both the raw
representation and all derived representations after they are no longer needed. 
Each batch carries source identity, its deterministic position, and memory accounting. A coordinator accepts batches in the
planned order, never completion order. Large or slow later tasks must not prevent the earliest needed task from progressing. 
The existing scheduler returns results only after the scoped workers finish. Merely changing a task result to contain all its
canonical rows would retain the entire session’s outputs before consumption. A real producer/consumer implementation
would need concurrently running consumers and bounded queues, not just a larger result vector. [S7] 
The first implementation need not introduce this additional concurrency. Persistent writer ownership is the correctness and
fragmentation change; decode-ahead is a separately measurable optimization. 
Memory design for a high-RAM machine 
A global budget, not one budget per worker 
The configured memory allowance must cover all concurrently live pipeline allocations: 
source/decompression buffers
+ workbook and sortable-source materialization
+ queued decoded batches
+ processing/book state
+ canonical/Arrow builders
+ encoder/compression workspace
+ event-index and provenance metadata
+ bookkeeping and explicitly reserved safety headroom

The reported 128 GB capacity is not a default allocation and is not authorization to consume all available memory. Configure a
smaller workload budget and leave room for the operating system, filesystem caching, and other applications. Select actual
production values after inspecting the writer and measuring peak use. 
Keep memory limits, row-group targets, and file-size targets separate. A large output part should not require the complete
part to remain in RAM. 
Account for retained data until consumption 
If a worker sends a batch to a queue, its memory reservation travels with that batch and is released only when the backing
allocation is released or transferred to another accounted owner. Do not return the credit when the producing task finishes
while its output remains queued.Avoid double-counting shared backing buffers. Conversely, account for temporary expansion during decoding, sorting, Arrow
conversion, and encoding instead of charging only the final serialized size. 
Use byte limits as well as any batch-count limits. Bound archive members, record/event size, active workbooks, queued
batches, and live writers. A single unusually large record must trigger an explicit supported path or resource error rather than
bypassing the budget. 
The existing estimator may remain an admission heuristic, but must not be presented as an exact process-RSS limit. Track
admitted/accounted memory and observed peak process memory separately. 
Progress under pressure 
When the memory allowance is approached, stop speculative prefetch and drain ready data. Flushing a completed row group
should not automatically close the current output part. 
Reserve enough memory and execution capacity for the next required producer and consumer to progress. Otherwise later
source buffers can fill the budget while the earlier source needed to drain them is waiting for memory. 
With synchronous stream execution this risk is simpler to control. If queues are added later, cancellation and producer failure
must wake blocked operations and release credits. 
Do not silently reintroduce one temporary file per task as the default pressure response. Use an existing, explicitly supported
spill path only when its semantics and budget are verified, or fail with an actionable resource error. 
Ordering and the existing merge subsystem 
Consolidation is not an implicit replacement for merge 
The normalization fast path should append compatible ordered source ranges into shared output. The existing merge
subsystem remains responsible for its established reconciliation duties. 
Before adding overlap resolution, inspect that implementation and reuse its ordering, continuity, and duplicate policies where
appropriate. If it accepts only on-disk datasets, a future in-memory adapter may be useful, but its availability must not be
assumed. 
Do not invoke a post-normalization disk merge as the default way to hide unchanged per-task file fragmentation. 
Establish input order from evidence 
Do not sort full input paths containing download IDs/hashes and call that chronological order. Task IDs and task-array order
are also not proof of temporal order. 
Use trustworthy source-period metadata, profile-defined archive conventions, or verified source inspection to plan order.
Validate the actual accepted records against that plan while reading. An advertised period is not a substitute for record-level
validation. 
If necessary ordering cannot be established without wider inspection, report that requirement rather than assuming the first
record is a lower bound for all unseen records. 
Simple append is appropriate only where each source is ordered under the relevant comparator and adjacent source ranges
are compatible. Equal timestamps need a deterministic tie policy that preserves valid source order. stream_rank does not
distinguish records from the same stream. 
Source min/max timestamps alone do not prove that records inside a source are sorted. 
Overlaps and regressions 
Unexpected overlap or regression must not be followed by a successful claim of chronological output. Either use a specifically
supported merge path or fail the affected job before publication with the relevant source identities and boundaries. 
Do not silently discard duplicates, rewrite timestamps, truncate an archive, or sort stateful depth level rows to make the
output appear ordered. 
Independently sortable snapshots can be sorted before stateful processing when the profile permits it. Delta feeds must
preserve their valid event/sequence semantics. If timestamp order and valid source order conflict, the integrity policy andoutput-ordering claim need an explicit resolution, not a generic timestamp sort. 
No new interval clipping policy is introduced here. Preserve the batch’s existing requested-interval semantics until separately
agreed. 
Depth: consolidate storage without silently changing reconstruction 
Writer lifetime and processor lifetime are separate decisions 
A long-lived writer is required for consolidation. A long-lived order book across every archive boundary is not automatically
required. 
For the first storage refactor, preserve independently normalized archives as explicit reconstruction segments. Their complete
initializations, recovery events, and gap information must survive even when several archives share one physical Parquet file. 
This deliberately narrows the earlier suggestion of carrying all processing state across archives: persistent writer ownership is
the immediate change; cross-archive book-state carry is a separately verified semantic change. 
Do not silently accept delta-only archives that previously lacked initialization by borrowing the preceding archive’s final state.
Such continuation is permitted only after identity, ordering, continuity, and profile compatibility have been validated by an
explicit policy, preferably reusing the existing merge/boundary logic. 
Similarly, do not remove a replacement snapshot merely because the preceding final book looks similar. Preserve the existing
interpretation until equivalence and reconciliation rules are established. 
Rework source-end metadata 
The current worker emits write_boundary for each task. A shared writer cannot simply persist every task’s boundary to one
path as though there were only one source. [S6] 
Introduce a source-completion result carrying the source’s boundary information and task metrics. The stream coordinator
collects those results, preserves source-level evidence, and creates aggregate stream metadata at finalization. 
Retain the information, but avoid recreating the small-file problem with one JSON file per source when a consolidated indexed
representation suffices. Metadata buffering itself must be bounded. 
A stream-level continuity verdict must account for every relevant source segment and cross-source edge. It cannot claim
verified continuity just because the final archive ended in a healthy state. 
Event identity, row offsets, and zero-change events 
Source ordinals currently restart within tasks. Preserve source identity as a tuple such as: 
(job_id, task_id, archive_member, source_record_ordinal)

Separately maintain a monotonic output event ordinal and stream-global canonical row offsets. Do not replace source lineage
with a global counter or allow repeated local ordinals to collide. 
Assign/rebase event offsets and reconstruction segment offsets in the writer/coordinator that knows the final output order.
Part numbering should likewise be deterministic within the stream. 
An accepted depth source event may produce zero canonical level changes. Preserve its event-index entry and source
metadata where required by the existing contract; do not drop it because its row count is zero. Separate source-record,
accepted-event, and canonical-level accounting. 
File boundaries must not change market meaning 
An output-part rollover must not reset the book, fabricate a snapshot, reset a source sequence, or alter how a deletion is
interpreted. 
Prefer part rollover between complete source-event outcomes. If an event exceeds the target, permit a documented target
overrun only if the supported resource limits permit it. Otherwise return a specific resource error. Do not split the event
without an explicit index/replay contract that represents the split safely.Preserve source-archive boundaries as logical metadata, not mandatory file boundaries. The actual on-disk format must be
checked against the existing merge and replay consumers before being finalized. 
Parquet sizing and output metadata 
Inspect the real writer before setting defaults 
Confirm how row_group_target_bytes and file_target_bytes are measured: estimated in-memory bytes, encoded
bytes, or persisted bytes. Confirm when rollover occurs, whether event and row-group boundaries affect it, and how partial
final parts are handled. 
Specify file size as a target, not a claim that every file has identical bytes. The final part of a stream may be smaller. Avoid
creating empty trailing parts. 
Do not solve excessive files by simply making every worker’s buffer enormous. First remove mandatory closure at source EOF;
then tune buffering, writer concurrency, and targets using measurements. 
Make discovery and manifests stream-aware 
A candidate layout is: 
dataset/
manifest.json
processing-job.json
processing-job.sha256
streams/
<stable-stream-key>/
part-000001.parquet
part-000002.parquet
<depth event index and reconstruction metadata, when applicable>

This is provisional. Prefer a layout compatible with existing consumers where practical. Do not guess that merge, convert, or
replay already supports it. 
The manifest should identify each output part’s stream, ordering position, row count, time range, and lineage. Preserve
source-to-output spans or event-index mappings without copying all provenance into every row unnecessarily. 
A task can span several parts, and one part can contain several tasks. The old assumption that a task owns one exclusive
output directory must not survive in accounting, progress, or readers. 
Update discovery, path validation, auxiliary-file discovery, and consumer tests together. Use a versioned manifest change
when its contract changes; do not change canonical value schemas merely to rename directories. 
Failure and accounting behavior 
All output remains inside staging until the job’s sources, writers, metadata, and integrity checks have completed successfully.
Preserve the current no-overwrite checks and same-filesystem staging-to-dataset rename. Do not claim additional crash
durability guarantees without inspecting the persistence implementation. [S4] 
A failed source or writer fails its job; other independent jobs may retain their existing independent finalization behavior. Do
not publish the successfully processed prefix of a failed stream as a complete dataset. 
For a shared writer, compare per-task accepted/written deltas or explicit acknowledgments against task metrics, and compare
final aggregate totals against persisted rows. Do not compare every task’s row count with the writer’s lifetime cumulative
count, as the current isolated-task wrappers do. [S3] 
Distinguish processed, queued, accepted by the output sink, writer-finalized, and dataset-committed states. RAM buffering
does not make data durable. A later write failure must not leave a misleading completed-dataset result. 
Keep source-local integrity scope and task diagnostics. Cross-source continuity findings should be attributable to both
relevant source boundaries. Rate/count aggregation must not count previous task observations twice when a context is
reused. 
On cancellation or failure, stop admission, unblock consumers/producers if present, release resources, and write theestablished failure report where possible. Never remove raw inputs or an existing committed dataset. 
Intended code changesAreaBatch/profile resolutionnormalize/session.rsnormalize/task.rsnormalize/worker/trades.rsnormalize/worker/depth.rsnormalize/worker/sink.rs and sink adaptersShared Parquet writersResource budget/estimatorManifest/discovery/finalizationMerge/convert/replay consumersRuntime loadingHistorical testsProposed responsibility/changeProduce fully resolved tasks and explicit source-format information;
preserve the version-3 input contract and profile provenance.Build compatible per-job stream groups and route both single-job and
multi-job execution through the same group path.Separate source processing/reporting from exclusive writer creation and
finalization.Reuse generic sink processing; preserve source identity and task-local
counters.Return source-boundary metadata and retain complete event outcomes;
make processor lifetime explicit.Distinguish source completion from stream finalization; add bounded batch
transport only when justified.Own stream-level part rotation, cumulative offsets, and durable output
finalization; exact edits pending source inspection.Account for stream lifetime, live buffers, decoding expansion, open writers,
and optional queues.Record grouped outputs and lineage, validate new layout, and commit only
after all grouped output is finalized.Verify and adapt discovery/metadata interpretation without rewriting their
algorithms unnecessarily.Load the user’s general.toml contract; do not guess its sections or copy
resource settings into batch tasks.Replace private-machine prerequisites with explicit fixtures and
reproducible real-data setup. 
These are proposed edit locations, not a completed audit of all callers. 
Standardized tests 
Small fixtures for routine correctness 
Use tests/historical/fixtures/ under the appropriate engine test tree for small, portable source fixtures and a batch
fixture. Tests should instantiate the production schema-version-3 document, resolve temporary/input roots explicitly, and
write only to isolated temporary outputs. 
A large batch fixture is useful for orchestration coverage, but it should not become a requirement for every unit test. Synthetic
failure, rollover, and accounting tests should not load private archived jobs just to obtain configuration. The supplied tests
currently do this in several places. [S8] 
Keep small test runtime settings deterministic. Ordinary tests should not inherit a local 128 GB machine’s production budgets
or require live acquisition/database access. 
Required consolidation casesCaseSeveral small ordered archives, one stream, generous targetOne stream larger than the configured targetTwo instruments or market categoriesSame instrument in two different jobsEqual timestampsUnsupported overlap/regressionRequired resultOne shared output part where the measured target permits;
source boundaries remain attributable.Multiple correctly ordered parts; no empty trailing part.Separate output streams.Separate datasets and independent results.Deterministic tie handling preserving the permitted source
order.Repeated local source ordinals Unique output indexing with preserved task/member lineage.Depth initialization and replacement snapshot across archives Correct replay and preserved reconstruction boundaries.Depth gap followed by recovery Gap/recovery information survives consolidation.Accepted zero-change depth event Event metadata remains indexed even with no new level rows.Different Parquet targets and worker counts Same canonical semantics and replay states under the
declared ordering contract.Writer failure after an earlier task was processed No committed dataset and accurate source/output failure
attribution.Low memory, cancellation, or queue pressure Bounded behavior, released reservations, no deadlock. 
Test both semantic equivalence and file-layout behavior. Equal Parquet file bytes are not required when buffering/layout
changes; compare canonical records, source-event mappings, integrity outcomes, and depth replay at relevant boundaries. 
For the initial storage-only depth change, preserve existing independent-segment semantics. If later continuity reuse changes
emitted snapshot/delta representations, validate replay equivalence separately instead of pretending row-count equality
remains the only valid oracle. 
Real-data integration and performance 
Use Forge’s documented preparation workflow as the prerequisite, as requested. Keep real-data tests opt-in and record exact
input identities/checksums, instrument metadata, profile fingerprints, and effective runtime settings. A missing input must
produce a clear unmet-prerequisite result, not a misleading pass. 
Include multi-archive streams, not just one representative file per profile. Test reads/writes using the actual production loader
and grouped execution path. 
Benchmark requirements 
Measure before and after with the same input corpus, build settings, validation policy, and output correctness checks. Treat
the reported I/O bottleneck as the investigation’s starting point, not as a proven explanation for every slowdown. 
Separate source read/decode/normalization with a counting or hashing sink, serialization/persistence of a bounded prepared
workload, and complete end-to-end execution. This distinguishes parsing/validation cost from output persistence cost without
removing correctness checks from the production comparison. 
Record wall time, source bytes read, output bytes, canonical rows/events, output-file count, file-size distribution, row-group
counts, peak process memory, admitted/accounted memory, active writers, queue high-water marks if applicable, and
finalization/manifest time. Record cache conditions and the completion/durability boundary being timed. 
Fewer output files is an acceptance criterion for the fragmentation change. A speedup is a measured outcome, not a promised
percentage. The same input archives still need to be read unless a separate, verified repeated-read problem is fixed. 
Review gates before implementation 
The next targeted code review needs the actual shared::persistence::parquet implementation and its configuration,
including the trade/depth writers, batch builders, event-index writer, segment metadata, and replay readers. The
normalization facade identifies that module, but the implementation is absent from the supplied export. [S2] 
Also inspect the existing historical merge implementation and its input-discovery/continuity contracts, relevant depth
processor and book-boundary types, source readers/decoders, the current batch/profile resolver, and the actual
general.toml plus representative unified profiles. 
Before writing code, settle the file-target measurement, grouped layout compatibility, source-order planning, overlap
behavior, and source-to-output lineage representation. Retain independent depth reconstruction segments initially unless
evidence supports a separately tested continuity change. 
Implementation should then progress from deterministic grouping and shared trade writers, to depth metadata-safe
consolidation, to optional decode-ahead, with portable fixtures established alongside each change. Benchmark before adding
concurrency complexity or larger default buffers.Explicit failure or verified existing merge path; never silent
repair.Source references 
These references identify supplied code snapshots, not a live repository audit. 
[S1] repomix-output(20261106-194235).xml: directory structure and config.rs, decimal.rs, quantity.rs, side.rs,
timestamp.rs. 
[S2] repomix-output(20261106-192054).xml: parquet/mod.rs and parquet/sinks.rs; imports/re-exports from
crate::shared::persistence::parquet. 
[S3] Same normalization export: task.rs, particularly execute_trade_task, execute_depth_task, their progress variants,
writer construction, finish, and cumulative metrics comparisons. 
[S4] Same normalization export: session.rs, executor.rs::finalize_processing_job, manifest/discovery.rs, and
executor.rs::commit_dataset. 
[S5] Same normalization export: worker/sink.rs, worker/trades.rs, worker/depth.rs, and parquet/sinks.rs. 
[S6] Same normalization export: worker/depth.rs::process_depth_task_with_metrics_impl, accept_outcome,
source ordinal assignment, DepthBoundaryTracker, and manifest/discovery.rs::inspect_auxiliary_files. 
[S7] Same normalization export: resources/budget.rs, resources/estimate.rs, scheduler.rs::execute_parallel,
and execute_parallel_budgeted. 
[S8] repomix-output(20261106-192131).xml: depth/depth_formats/common.rs, depth/depth_csv_pipeline.rs,
and associated real-data/execution tests. repomix-output(20261106-192147).xml contains the grouped historical
normalization test entry point. 
Design summary: Keep task identity, remove task-owned file boundaries, preserve depth semantics, use globally bounded
RAM, avoid unnecessary intermediate writes, and verify compatibility with the existing merge implementation before
changing the storage contract
