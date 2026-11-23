# Prepared merge job

Forge writes one pinned job consumed by `marketforge-merge`. Its identity, input file references and half-open event-time interval are independent of editable recipe selectors. [Workflow](../workflows/merge.md)

## Preparation evidence

Forge dry-run preparation resolves selection, local provenance and ordering. It reports `planned` with prospective paths and `compatibility = "not_validated"`. Persisted preparation writes `merge-job.json` and `prepare-report.json`, retains the selected file and publication evidence, source recipe fingerprint where applicable, registry identity, and checksums for the submission and retained [runtime](../configuration/merge-runtime.md). Rust validation checks the generated job, input pins and compatibility, and the output destination. A `prepared` result with `compatibility = "validated"` records that check. A `generated` result retains the job and validation diagnostic. It can result from a missing Engine as well as a validation rejection.

The preparation report is Forge evidence. It binds the registry, selection and runtime to the job. The Rust input is the job JSON itself, which has no top-level schema version field. An available equivalent result can instead produce a reuse receipt with the existing physical path and registry identities. See [completed-work reuse](cli/merge.md#completed-work-reuse).

A retained job and its report must agree at submission. Execution verifies registered inputs again and checks the job, binary and runtime fingerprints when pinned. Review the [CLI reference](cli/merge.md#mf-merge-run) for recipe and prepared-job submission.

## Prepared Merge Jobs

## Why Does Merging Use a Different Contract?

Normalization reads registered raw archives.

Merging reads registered normalized Parquet files.

These operations require different input information.

### Normalization input

For example:

```text
Raw ZIP Archive
    |
    v
Source Profile
    |
    v
Canonical Trades
```

### Merge input

For example:

```text
Normalized Trades ──┐
                    |
                    v
                  Merge
                    ^
                    |
Normalized Depth ───┘
```

The merger does not need to decode the original exchange archive format again.

Instead, it needs to know:

- Which exact normalized primary files were selected.
- Which publications own them.
- Which streams they represent.
- Their content types.
- Their retained checksums.
- The required timeline ordering.
- The requested interval.

### Why pin files rather than only instruments?

An instrument may have several normalized publications.

For example, the same BTCUSDT period may have been normalized more than once.

A merge must use the exact selected files rather than silently switching between publication revisions.

---

## Merge Job Structure

The prepared merge job contains:

| Field | Purpose |
|---|---|
| `job_id` | Merge job identity. |
| `operation` | Processing operation. |
| `inputs` | Exact registered normalized primary files. |
| `time_range` | Requested historical interval. |
| `timeline_ordering` | Chronological or causal timeline behavior. |
| `output` | Merge publication destination. |

### Example structure

```json
{
  "job_id": 123456,
  "operation": "merge",
  "timeline_ordering": "chronological",
  "inputs": [],
  "time_range": {
    "start_ns": 1793059200000000000,
    "end_ns": 1793318400000000000
  },
  "output": {
    "dataset_path": "/absolute/path/to/data/datasets/merged/123456"
  }
}
```

The empty input array illustrates the wire shape. The loader requires at least one eligible pinned input, a positive job ID and an absolute output destination. Unknown fields are rejected.

### Why is the merge interval numeric?

The merge contract uses integer Unix nanoseconds.

This allows precise event-time boundaries.

---

## Merge Input Pinning

## What Does Each Merge Input Identify?

Each merge input represents one selected registered normalized primary file.

Important fields include:

| Field | Meaning |
|---|---|
| `dataset_id` | PostgreSQL primary file dataset ID. |
| `stream_id` | Exact concrete logical stream identity. |
| `stream_rank` | Merge ordering priority. |
| `content_type` | `trades` or `depth`. |
| `path` | Absolute primary Parquet file path. |
| `sha256` | SHA-256 of the selected primary file. |
| `publication` | Owning normalized publication identity and manifest evidence. |

### Why is `dataset_id` important?

It identifies one registered physical primary file.

It is not the same as the Engine publication ID.

### Why is `stream_id` important?

A merged file may combine multiple instruments and content types.

The stream identity identifies the logical source of an event.

### Why is `content_type` required?

Trades and depth have different event semantics.

The merger must know which kind of normalized data the selected file represents.

---

## Merge Input Example

A merge input has the following structure:

```json
{
  "dataset_id": 119,
  "stream_id": "binance.linear.future.BTCUSDT_261225.trades",
  "stream_rank": 0,
  "content_type": "trades",
  "path": "/absolute/path/to/part-000000.parquet",
  "sha256": "<primary-file-sha256>",
  "publication": {
    "engine_dataset_id": "7868739277080297142",
    "manifest_path": "/absolute/path/to/manifest.json",
    "manifest_sha256": "<manifest-sha256>"
  }
}
```

This is a structural illustration.

Replace the paths and checksums with real Forge-generated values.

Do not submit placeholder fingerprints.

### What does this input mean?

It identifies:

```text
Registered primary file:
119

Logical stream:
Binance BTCUSDT futures trades

Primary file:
Specific Parquet part

Owning publication:
Specific normalized Engine publication

Verification:
Pinned file and manifest checksums
```

### Why include both file and manifest checksums?

The file checksum identifies the selected Parquet bytes.

The manifest checksum identifies the corresponding publication metadata.

Together, they help establish that the input still matches the intended committed publication.

---

## File Checksum Pinning

The merge input contains:

```json
{
  "sha256": "<primary-file-sha256>"
}
```

### What is SHA-256?

It is a cryptographic checksum used to identify file contents.

For example, two files with different bytes should ordinarily produce different SHA-256 values.

### Why pin the checksum?

Suppose you prepare a merge today.

Tomorrow, someone modifies one of the selected Parquet files.

The prepared job should not silently process the changed content as though it were the original input.

The checksum helps detect that mismatch.

### Does a matching checksum prove the market data is historically complete?

No.

It establishes consistency with the pinned file bytes.

It does not prove that the original exchange supplied every event.

### What happens if the checksum changes?

The applicable validation or execution check can reject the input.

Prepare a new job only after investigating the reason for the change.

Do not manually replace the fingerprint merely to bypass the mismatch.

---

## Publication Pinning

The merge input contains:

```json
{
  "publication": {
    "engine_dataset_id": "7868739277080297142",
    "manifest_path": "/absolute/path/to/manifest.json",
    "manifest_sha256": "<manifest-sha256>"
  }
}
```

### What is `engine_dataset_id`?

It identifies the owning Engine normalization publication.

### Why is it a string?

Engine dataset identities may use unsigned 64-bit integer values.

Representing them as canonical decimal strings avoids precision loss in tools that cannot exactly represent all such integers.

### What is `manifest_path`?

It identifies the owning publication manifest.

### What is `manifest_sha256`?

It pins the expected manifest bytes.

### Why is the manifest necessary?

The manifest provides publication information required to interpret the selected normalized file.

For example:

- Content type.
- Stream membership.
- Physical layout.
- Ordering information.
- Source ownership.
- Applicable auxiliary data.

The merger cannot safely infer all these properties from the Parquet filename.

---

## Why Independent JSON Must Be Verified

A user can construct a JSON document with plausible field names.

However, that does not establish that its inputs are valid.

For example:

```json
{
  "dataset_id": 119,
  "stream_id": "some-stream",
  "content_type": "trades"
}
```

could refer to an incorrect stream.

### What does Rust check?

The merge contract validates the selected file against its owning publication information.

This includes the applicable:

- Registered file identity.
- Stream ownership.
- Content type.
- Publication fingerprint.
- Physical input compatibility.

### Why not trust the supplied values?

Because a JSON document is a request, not independent evidence.

A user could accidentally supply a depth file while labeling it as trades.

The Engine must reject inconsistent input contracts.

### Recommended practice

Use Forge preparation.

It resolves the registered file and publication evidence automatically.

---

## Merge Stream Ranks

## Why Merge Inputs Need Stream Ranks

A merged timeline may contain several logical streams.

For example:

```text
BTCUSDT Trades
BTCUSDT Depth
ETHUSDT Trades
ETHUSDT Depth
```

Events can have identical timestamps.

The merger needs deterministic ordering information.

### Example

```text
12:00:00.100 BTCUSDT Trade
12:00:00.100 ETHUSDT Trade
```

Both events have the same event timestamp.

Stream rank can provide the applicable tie-breaking order.

---

## Merge Rank Rules

The prepared merge contract uses unsigned 32-bit stream ranks.

### Lower ranks first

For applicable equal-time ordering:

```text
Rank 0
```

comes before:

```text
Rank 1
```

### Same stream

Files representing the same concrete stream repeat its rank.

For example:

```text
BTCUSDT Trades
    Part 1 → Rank 3
    Part 2 → Rank 3
    Part 3 → Rank 3
```

### Different streams

Distinct concrete streams use distinct ranks across the selected content types.

For example:

```text
BTCUSDT Trades → Rank 3
BTCUSDT Depth  → Rank 4
ETHUSDT Trades → Rank 5
```

### Can ranks contain gaps?

Yes.

For example:

```text
Rank 1
Rank 5
Rank 9
```

can remain valid in a filtered prepared job.

### Why not require dense ranks?

Filtering a job may remove selected streams without changing the retained ordering identities.

The merge contract allows rank gaps.

### Are normalization and merge rank rules identical?

Not necessarily.

Normalization preparation can generate dense ranks from retained ranking positions.

Merge preparation can preserve selected normalized stream ranks and applies its own distinct-stream consistency requirements.

---

## Merge Time Ranges

## Half-Open Merge Intervals

A prepared merge job contains:

```json
{
  "time_range": {
    "start_ns": 1793059200000000000,
    "end_ns": 1793318400000000000
  }
}
```

The start is included.

The end is excluded.

This is:

```text
[start_ns, end_ns)
```

### Why use nanoseconds?

High-frequency market events can occur within microseconds or nanoseconds of one another.

Integer nanosecond boundaries preserve the requested time precision.

### Does this interval prove the source contains every event?

No.

It describes the requested processing range.

The actual input files must satisfy the applicable selection and overlap requirements.

### What if an input does not overlap the interval?

Preparation can reject the selection.

### Can the merger invent missing observations?

No.

It combines available supported normalized events.

It does not recreate missing exchange history.

---

## Timeline Ordering

## Why Timeline Ordering Matters

Merging trades and order-book data is not simply concatenating files.

Consider:

```text
Trade:
12:00:00.100

Depth:
12:00:00.050
12:00:00.150
```

A chronological timeline might place them as:

```text
12:00:00.050 Depth
12:00:00.100 Trade
12:00:00.150 Depth
```

However, depth events can depend on earlier book state.

For example:

```text
Snapshot
    |
    v
Update
    |
    v
Update
```

Arbitrarily rearranging these events can corrupt reconstruction.

### Why does this require an explicit contract?

Different normalized publications may use different physical ordering and reconstruction layouts.

The merger must apply the supported ordering semantics for the selected inputs.

---

## Chronological Ordering

The merge job can contain:

```json
{
  "timeline_ordering": "chronological"
}
```

### What does this mean?

The merger constructs the supported chronological timeline using event timestamps and the applicable tie-breaking rules.

### When is it appropriate?

For compatible inputs whose physical layout and event semantics support chronological merging.

### Does it mean arbitrary depth sorting is permitted?

No.

Depth dependencies remain subject to the supported input-layout and reconstruction requirements.

### What if `timeline_ordering` is omitted?

The prepared merge job contract defaults to chronological ordering.

However, Forge may explicitly resolve a different mode when the selected recipe and inputs require it.

---

## Causal Ordering

The merge job can also contain:

```json
{
  "timeline_ordering": "causal"
}
```

### Why is causal ordering necessary?

Some depth publications contain source-order or reconstruction dependencies that timestamp sorting alone cannot safely represent.

For example:

```text
Initialize book
    |
    v
Apply update
    |
    v
Apply next update
```

The causal timeline preserves the applicable processing dependencies.

### Does causal mean economic causation?

No.

It does not prove that one trade caused another event.

Here, causal ordering describes the required event-processing dependencies.

### Why is this distinction important?

A trade occurring before a depth update does not automatically prove that the trade caused the update.

The merger preserves event order and source semantics.

Market-impact analysis remains a separate research task.

---

## Causal Timeline Physical Schema

The documented committed causal timeline uses physical schema version 2.

Important fields include:

```text
timeline_sequence
reconstruction_only
```

### What is `timeline_sequence`?

It records the physical ordering information required by the causal timeline representation.

### What is `reconstruction_only`?

It identifies rows that are present for supported reconstruction purposes rather than ordinary event interpretation.

### Why retain reconstruction rows?

Some order-book histories require initialization or dependent observations to establish valid state.

Those rows may be necessary even when they are not ordinary user-visible market events.

### Should they be removed manually?

No.

Removing reconstruction dependencies can make a historical book impossible to replay correctly.

### Can I sort the resulting file by timestamp?

Do not assume that independently sorting causal timeline rows preserves the required event semantics.

Use the supported timeline sequence and reconstruction information.

---

## Chronological vs. Causal

| Property | Chronological | Causal |
|---|---|---|
| Main objective | Supported timestamp ordering | Preserve required event dependencies |
| Typical consideration | Compatible independent events | Stateful segmented depth |
| Ordering information | Timestamp and stream-rank contract | Physical causal sequence and reconstruction evidence |
| Can arbitrarily sort depth levels? | No | No |
| Proves economic causation? | No | No |

### Which should I choose?

Use the ordering mode appropriate to the selected publication layouts and processing requirements.

For ordinary workflows, allow Forge preparation to resolve the supported behavior.

### Does every normalized publication support both?

No.

The selected inputs must satisfy the merger's current compatibility rules.

---

## Merge Compatibility

## Why Valid Parquet Files Can Still Be Incompatible

A Parquet file can be structurally valid while using a physical layout that the merger does not support.

For example, a normalized depth publication may preserve native-order reconstruction segments.

A particular merger path may require additional supporting files or a specific physical ordering.

### What does compatibility depend on?

It can depend on:

- Manifest protocol version.
- Content type.
- Physical timestamp ordering.
- Stream ownership.
- Source spans.
- Reconstruction dependencies.
- Selected primary file set.
- Supporting publication evidence.

### Why not merge every valid file?

Because combining files under unsupported assumptions could create an incorrect timeline.

Rejecting an incompatible selection is safer than silently inventing ordering or reconstruction rules.

---

## Normalization Manifest Versions

Normalization publications use versioned manifest protocols.

The documented range includes:

```text
3–5
```

These protocols can describe different publication capabilities and evidence.

### Why are manifests versioned?

As the Engine evolves, it can add information about:

- Source ownership.
- Reconstruction boundaries.
- Processing quality.
- Physical ordering.
- Publication integrity.

### Does a newer manifest automatically work with every downstream operation?

No.

For example, a newer normalization publication may contain a valid native-order depth layout that is not supported by a particular merge reader.

### What should users inspect?

Check the actual publication manifest and the current merge compatibility report.

Use:

[Supported Merge Layouts](../workflows/merge.md#supported-layouts)

for the detailed compatibility matrix.

---

## Source Ownership and Merge Compatibility

Some normalized trade publications retain information about which source records contributed to each output row.

For example, a trade publication may contain:

```text
sources.jsonl
```

This provides source-ownership evidence.

### Why is source ownership important?

A publication can contain events from overlapping archives.

The merger may need to know whether a selected physical part has contiguous source ownership.

### What does contiguous ownership mean?

Conceptually, a compatible file might contain:

```text
Source A rows
Source B rows
Source C rows
```

with each source represented in one contiguous span.

An interleaved layout might contain:

```text
Source A
Source B
Source A
Source C
Source B
```

The latter can require different processing support.

### Does the merger support every ownership layout?

No.

The documented timestamp-first merge contract supports certain v4/v5 trade layouts with contiguous source ownership and no repeated source spans.

More complex layouts may be rejected.

### What about native-order depth?

The timestamp-first merger's supported input set is narrower than the complete set of valid depth publications.

Inspect the current supported-layout rules rather than assuming every depth publication is eligible.

---

## Wire identity

The public document has no `schema_version`. It contains `operation:"merge"`, positive uint64 `job_id`, nonempty `inputs`, `time_range`, `output`, and optional `timeline_ordering` defaulting in the Engine to `chronological`. `time_range.start_ns` and `end_ns` are signed int64 and ordered. `output.dataset_path` is absolute.

Each input has positive uint64 registry `dataset_id`, nonempty `stream_id`, uint32 `stream_rank`, `content_type` of `trades` or `depth`, absolute `path`, lowercase SHA-256 `sha256`, and a `publication` object. The latter contains positive canonical decimal text `engine_dataset_id`, absolute `manifest_path` ending in `manifest.json`, and lowercase SHA-256 `manifest_sha256`. IDs and file paths cannot repeat in one submission. A repeated Engine publication ID must have identical manifest path and hash. Forge's editable database selectors are further constrained to signed 64-bit IDs. Unknown fields are rejected. [Engine wire definition](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/job/merge.rs)
