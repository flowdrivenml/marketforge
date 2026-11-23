# Prepared normalization batch

Forge writes the schema-4 normalization batch. The Rust normalizer validates and resolves it; a TOML recipe remains a separate input. [Workflow](../workflows/normalize.md)

## Manual batch submission

Use the recipe-first [normalization workflow](../workflows/normalize.md) for ordinary runs. Use separate preparation and submission when you need to inspect and retain the exact job inputs before execution.

```bash
mf --json normalize prepare my_dataset --testing > prepare.json &&
batch=$(python3 - <<'PYTHON'
import json
from pathlib import Path

report = json.loads(Path("prepare.json").read_text())
if report["status"] != "prepared" or not report["engine_ready"]:
    raise SystemExit("Batch is not ready. Inspect prepare.json.")
print(report["batch_path"])
PYTHON
) &&
mf normalize run "$batch" --testing
```

The guarded command chain submits only a persisted plan with `status = "prepared"` and `engine_ready = true`. Keep `normalize-batch.json`, `prepare-report.json`, and their sibling profile and runtime copies together. Execution checks the retained batch, runtime, executable and profile hashes, verifies the registry identity and raw inputs, and validates the batch again with Rust. Reprepare from the TOML recipe if pinned evidence has changed. See [execution and registration](cli/normalize.md#execution).

## Prepared Normalization Batches

## What Is a Normalization Batch?

A normalization batch describes one or more concrete Rust normalization jobs.

For example:

```text
                  Normalization Batch
                          |
              +-----------+-----------+
              |                       |
              v                       v
           Trades Job              Depth Job
              |                       |
         +----+----+              +---+---+
         |         |              |       |
         v         v              v       v
       Task 1    Task 2         Task 3  Task 4
```

A task identifies source work assigned to a job.

A job produces the applicable normalized publication.

### Why use batches?

One normalization recipe may select several archives and content types.

For example:

```text
BTCUSDT Trades
BTCUSDT Depth
ETHUSDT Trades
ETHUSDT Depth
```

Instead of submitting each archive manually, Forge groups the resolved processing work into jobs.

### Are trades and depth processed identically?

No.

They have different processing contracts.

For example, depth normalization may require book reconstruction and sequence validation.

Trade normalization uses its own event-identity and reconciliation rules.

The batch groups these jobs without pretending they are the same content type.

---

## Current Normalization Batch Version

The current normalization batch protocol uses:

```json
{
  "schema_version": 4,
  "kind": "normalization_batch",
  "status": "prepared"
}
```

This is an illustrative fragment.

A complete batch contains additional required information.

### Why does the batch use schema version 4?

The batch protocol has evolved independently of user-facing recipes.

For example:

```text
Normalization recipe:
version = 1

Prepared Engine batch:
schema_version = 4
```

These are different contracts.

### Does recipe version 1 mean batch version 1?

No.

Forge converts the supported recipe format into the current Rust Engine contract.

### Where is the implementation?

Forge generates normalization batches in:

```text
forge/historical/normalization/batch.py
```

Rust parses and validates them in:

```text
engine/src/historical/config/batch.rs
```

The maintained normalization fixture is:

```text
engine/tests/historical/fixtures/normalize.json
```

### Why does the fixture matter?

It provides a concrete reference for the maintained parser workload.

However, the fixture may contain environment-specific file paths and registered identities.

It should not be assumed executable in every workspace without its required dependencies.

---

## Normalization Batch Structure

## Batch-Level Parameters

The prepared batch contains information shared by the normalization jobs.

| Parameter | Purpose |
|---|---|
| `schema_version` | Prepared-batch protocol version. |
| `kind` | Batch document kind. |
| `status` | Preparation status. |
| `run_id` | Logical preparation run identifier. |
| `run_directory` | Associated run directory. |
| `batch_path` | Prepared batch location. |
| `ranking_snapshot` | Selected ranking snapshot identity, when available. |
| `ranking_policy` | Ranking policy used for stream-ordering resolution. |
| `requested_interval` | Original requested historical interval. |
| `instrument_namespace` | Canonical instrument identity namespace. |
| `instruments` | Resolved authoritative instrument definitions. |
| `families` | Supported family authority information. |
| `jobs` | Concrete normalization jobs. |

### Why does the batch need these fields?

The Engine needs a complete description of the requested work.

For example, the jobs identify source processing tasks.

The instrument definitions provide the relevant economic interpretation.

The namespace establishes which canonical identity system the numeric instrument IDs belong to.

---

## Example Batch Header

A prepared batch can begin with:

```json
{
  "schema_version": 4,
  "kind": "normalization_batch",
  "status": "prepared",
  "run_id": "normalize-example",
  "run_directory": "/absolute/path/to/data/runs/normalize-example",
  "batch_path": "/absolute/path/to/data/runs/normalize-example/normalize-batch.json",
  "ranking_snapshot": null,
  "ranking_policy": "turnover",
  "requested_interval": {
    "start": "2026-10-27T00:00:00Z",
    "end": "2026-10-30T00:00:00Z"
  }
}
```

This is a structural illustration, not a complete executable batch.

Actual prepared batches include instrument authority and job definitions.

### Why preserve `requested_interval`?

The requested interval describes what the user selected.

The actual raw archives can extend beyond that interval.

Preserving the original request makes the selection traceable.

### Why preserve `ranking_policy`?

The selected ranking policy can provide ordering evidence for the generated streams.

The prepared batch retains that policy identity.

### Why can `ranking_snapshot` be null?

A suitable published ranking snapshot may not exist.

Forge can use deterministic stream identity ordering when the required ranking positions are absent.

---

## Preparation Status

The normalization batch contains:

```json
"status": "prepared"
```

The preparation system can also report a partial selection.

### Prepared

A prepared status indicates that Forge resolved the applicable complete selection and constructed the corresponding batch.

A persisted prepared plan runs the production Rust batch validator and records `engine_ready = true` after validation passes. Dry-run `prepared` describes successful Forge planning. It has `engine_ready = false`, skips Rust batch validation and writes neither the batch nor its report. Resolving registered external inputs may still compile profiles to verify fingerprint compatibility.

### Partial

A partial status can indicate that some selected work was resolved while other requirements remain blocked.

For example:

```text
Requested archives: 100
Resolved archives:  95
Blocked archives:    5
```

These numbers are illustrative.

### Does partial mean the complete workload is ready?

No.

A partial preparation does not establish that the complete requested selection is executable.

### What should users inspect?

The preparation report distinguishes selection evidence from generated work.

| Evidence | Meaning |
| --- | --- |
| `requests[]` | One entry per source request or legacy input selection. Each source request includes its matching scope and content. |
| `matched_archives` | Candidate registry records returned for a source request, including records that can subsequently fail resolution. |
| `selected_archives` | Candidates that resolved into usable input evidence for the request. The same archive can appear in multiple requests. |
| `blockers`, `coverage` | Input or metadata issues and nominal archive interval evidence. Gaps can block the plan. Missing evidence stays unknown. |
| `prepared_job_count`, `prepared_task_count`, `prepared_stream_count` | Generated jobs by content type, deduplicated source work within each job, and distinct planned streams summed across jobs. Family expansion can resolve additional concrete streams in the Engine. |
| `job_summaries` | Per-job Engine IDs, content type, planned destinations, and task and stream counts. |
| `ranking_resolution` | Policy, retained snapshot and ordering or fallback evidence. |
| `batch_path`, `report_path` | Batch and preparation report destinations. These paths are prospective in dry runs. |

The requested interval selects overlapping whole archives. Nominal archive bounds and gaps do not establish market-event completeness or final row bounds.

### Can the batch exist without being executable?

Yes.

A generated artifact is not automatically equivalent to a successfully validated processing request.

For executable results, use the reported preparation and Engine validation outcomes together. A persisted `partial` batch can be retained for diagnosis; the Rust loader accepts only `prepared` batches.

Runtime controls such as resource limits and Parquet writer settings come from the [normalization runtime configuration](../configuration/normalization-runtime.md). Persisted preparation retains the effective runtime file and profile copies. They are not per-recipe overrides.

---

## Job-Level Parameters

## What Is a Normalization Job?

A job groups concrete source tasks belonging to one normalization operation.

For example:

```text
Trades Job
    |
    +-- BTCUSDT October 27
    +-- BTCUSDT October 28
    +-- ETHUSDT October 27
```

The corresponding job contains a unique identity, output configuration, and task list.

### Job parameters

| Parameter | Purpose |
|---|---|
| `operation` | Requested processing operation. |
| `job_id` | Engine job identity. |
| `dataset_id` | Engine dataset identity. |
| `tasks` | Source tasks assigned to the job. |
| `ordering.primary` | Primary output ordering declaration. |
| `ordering.tie_break` | Tie-breaking rule. |
| `output.content_type` | `trades` or `depth`. |
| `output.staging_path` | Intermediate publication destination. |
| `output.dataset_path` | Final publication destination. |

---

## Job Identity

A prepared job contains:

```json
{
  "operation": "normalize",
  "job_id": 123456,
  "dataset_id": 123457
}
```

These are illustrative identifiers.

### What is `job_id`?

It identifies the normalization processing job.

### What is `dataset_id`?

It identifies the Engine dataset produced by the job.

### Why are they different?

A job describes processing work.

A dataset identifies the publication produced by that work.

These are related, but they serve different purposes.

### Are these PostgreSQL file IDs?

No.

Engine dataset IDs are separate from PostgreSQL's registered primary file dataset IDs.

For example, one normalized publication may contain several Parquet parts.

Each primary file can receive its own PostgreSQL dataset ID after registration.

### Does Forge reserve the IDs during preparation?

The current normalization preparation allocates collision-checked testing identifiers.

The preparation report identifies them as unreserved.

They are not automatically database registrations.

### Why does this matter?

A prepared job should not be mistaken for an already published and registered dataset.

Registration occurs after successful processing and verification.

---

## Job Ordering

The maintained normalization fixture uses:

```json
{
  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank"
  }
}
```

### Primary ordering

```json
"primary": "event_timestamp_ns"
```

identifies event time as the primary output-ordering field.

### Tie-breaking

```json
"tie_break": "stream_rank"
```

provides an ordering rule for applicable equal-time events.

### Why are tie-breakers necessary?

Different streams can contain events with identical timestamps.

For example:

```text
12:00:00.100 BTCUSDT Trade
12:00:00.100 ETHUSDT Trade
```

A deterministic tie-breaker helps establish a reproducible ordering.

### Does stream rank change timestamps?

No.

Stream ranks are ordering metadata.

They do not rewrite event timestamps.

### Can the ordering fields arbitrarily sort depth events?

No.

The Engine still applies its source-order, reconciliation, and reconstruction rules.

Stateful depth updates cannot be safely rearranged merely because the batch declares timestamp-based ordering.

The actual physical layout and required reconstruction semantics must remain valid.

---

## Job Output Configuration

Each job identifies its output content type and publication destinations.

Conceptually:

```json
{
  "output": {
    "content_type": "trades",
    "staging_path": "/absolute/path/to/staging",
    "dataset_path": "/absolute/path/to/normalized-publication"
  }
}
```

This is an illustrative fragment.

### Content type

The supported normalization job content types are:

```text
trades
depth
```

### Staging path

The Engine writes intermediate publication output to its staging destination.

### Dataset path

The final destination identifies where the committed publication will be stored.

### Why separate staging and final output?

Processing can fail before publication is complete.

For example:

```text
Read Source
    |
    v
Normalize Events
    |
    v
Write Staging Files
    |
    v
Verify Publication
    |
    v
Commit Dataset
```

The staging directory is not automatically a valid committed publication.

### Does the Engine overwrite existing datasets?

The managed workflow uses fresh output destinations.

Existing committed publications should not be silently replaced.

### Why does this matter for recovery?

A valid committed publication can sometimes be registered without rerunning the complete normalization workload.

An incomplete staging directory cannot be treated as a completed publication.

---

## Normalization Tasks

## What Is a Task?

A task represents concrete source work assigned to a normalization job.

For example:

```text
Task 1:
Read one registered BTCUSDT archive.

Task 2:
Read the next BTCUSDT archive.

Task 3:
Read one ETHUSDT archive.
```

A task identifies the source and its applicable processing metadata.

### Why use tasks?

One job may need to process many archives.

Tasks provide concrete units of source work.

### Does every task correspond to one instrument?

Not necessarily.

A concrete instrument task can identify one instrument.

A family-scoped task may refer to source data containing several concrete instruments.

Those cases require different identity-resolution information.

---

## Task-Level Parameters

The prepared normalization contract supports task fields including:

| Parameter | Meaning |
|---|---|
| `task_id` | Task identifier within the job. |
| `input_path` | Source archive path. |
| `exchange` | Execution venue. |
| `instrument_type` | Financial instrument type. |
| `market_category` | Market classification. |
| `base_asset` | Base asset when applicable. |
| `quote_asset` | Quote asset when applicable. |
| `symbol` | Concrete source instrument symbol when applicable. |
| `instrument` | Concrete economic specification. |
| `instrument_id` | Canonical instrument identity when applicable. |
| `stream_id` | Logical canonical stream identity. |
| `stream_rank` | Stream ordering metadata. |
| `scope` | Optional source-family selection. |
| `source_options` | Supported source interpretation options. |
| `instrument_selection` | Concrete instrument-selection rule. |

Some task forms use additional family authority information.

Not every field has the same meaning for concrete instrument and family-scoped tasks.

---

## Concrete Instrument Task Example

The following example illustrates a prepared Bybit BTCUSDT depth task:

```json
{
  "task_id": 15,
  "input_path": "/absolute/path/to/BTCUSDT-depth.zip",
  "exchange": "bybit",
  "instrument_type": "perpetual",
  "market_category": "linear",
  "base_asset": "BTC",
  "quote_asset": "USDT",
  "symbol": "BTCUSDT",
  "instrument": {
    "tick_size": "0.10",
    "contract_value": null,
    "contract_value_asset": null
  },
  "stream_id": "bybit.linear.perpetual.BTCUSDT.depth",
  "stream_rank": 5,
  "instrument_id": 9
}
```

This is an illustrative task fragment.

The task must still belong to a valid complete batch with the required job, runtime, profile, and authority information.

### What does this identify?

| Field | Example |
|---|---|
| Source venue | Bybit |
| Instrument | BTCUSDT |
| Product | Linear perpetual |
| Content | Depth |
| Tick size | 0.10 |
| Stream ID | `bybit.linear.perpetual.BTCUSDT.depth` |
| Canonical instrument ID | 9 |
| Stream rank | 5 |

### Why does this contain more information than the recipe?

Because Forge has resolved the concrete source identity.

The recipe only needs to describe the intended market selection.

---

## Source Paths

The prepared task contains:

```json
"input_path": "/absolute/path/to/BTCUSDT-depth.zip"
```

### Why use a concrete path?

Rust needs to know which physical managed file to read.

### Does Rust search the exchange archive server?

No.

Normalization uses registered raw inputs.

Remote acquisition is handled separately.

### Why not use an arbitrary filename?

A filename alone may not identify the correct registered source.

For example, several managed archives may share similar names.

Forge resolves the applicable managed source and its registered metadata.

### Can I replace the path manually?

Do not change the path merely to point to another file with a similar name.

The selected file must agree with the registered source identity and applicable preparation evidence.

For ordinary workflows, change the selection recipe or registration and prepare a new batch.

---

## Instrument Authority

## Why Does the Batch Contain Instrument Definitions?

A source archive may contain prices and quantities without all the economic information required to interpret them.

For example:

```text
Price: 80,000
Quantity: 100
```

For a spot instrument, quantity might represent base units.

For a derivative, it might represent contracts.

The corresponding historical instrument specifications establish the correct interpretation.

### Why include authority in the batch?

Rust Engine should not need to guess the economic specifications from the source filename.

Forge resolves the applicable historical evidence and includes the required definitions.

### Instrument definition example

```json
{
  "instrument_id": 9,
  "exchange": "bybit",
  "instrument_type": "perpetual",
  "market_category": "linear",
  "symbol": "BTCUSDT",
  "base_asset": "BTC",
  "quote_asset": "USDT",
  "instrument": {
    "tick_size": "0.10",
    "contract_value": null,
    "contract_value_asset": null
  }
}
```

This is an illustrative definition based on the maintained task structure.

### Why is contract value nullable?

Not every instrument requires contract-value interpretation in the same way.

For example, a base-quantity instrument may not use a separate contract multiplier in its applicable definition.

For contract-denominated quantities, the required value and denomination must be available through the supported historical specifications.

---

## Instrument Namespace

The batch includes:

```json
"instrument_namespace": "marketforge:testing:historical:v1"
```

This identifies the canonical historical instrument namespace.

### Why is a namespace necessary?

Numeric instrument IDs are not universally meaningful.

For example:

```text
instrument_id = 9
```

may identify one instrument in a particular registry namespace.

Another independently managed catalogue could assign the same number to something else.

### What does the namespace establish?

It identifies the authority under which the instrument IDs were assigned.

### Can I copy numeric IDs between databases?

Do not assume numeric IDs are interchangeable between independent registries.

A concrete instrument identity must agree with its namespace and retained authority evidence.

### Does the namespace replace the symbol?

No.

The prepared contract retains the relevant instrument information and concrete identifiers.

The namespace helps interpret the authority of those IDs.

---

## Historical Specifications

Prepared tasks can contain the resolved economic specification.

For example:

```json
{
  "tick_size": "0.10",
  "contract_value": "100",
  "contract_value_asset": "USD"
}
```

This is an illustrative specification.

### Why preserve decimal values as strings?

Financial specifications require exact decimal interpretation.

For example:

```text
0.000001
```

should not be unnecessarily converted into an approximate floating-point value.

### Why does contract denomination matter?

Consider:

```text
Contract value: 100 USD
```

and:

```text
Contract value: 100 BTC
```

These represent very different economic quantities.

The denomination must be explicit when required.

### Does the prepared batch prove historical correctness?

It carries the resolved authority accepted by preparation.

The quality of the historical evidence depends on the applicable catalogue and specification records.

For example, operational inference should not be described as independent historical verification.

### What if specifications conflict?

Forge can block preparation when the selected instrument crosses conflicting historical authority evidence.

A processing job should not silently use incompatible specifications for the same selected instrument.

---

## Stream Identities and Ranking

## What Is a Stream?

A stream identifies a logical sequence of market-data events.

For example:

```text
bybit.linear.perpetual.BTCUSDT.trades
```

and:

```text
bybit.linear.perpetual.BTCUSDT.depth
```

identify different logical content streams.

### Why does this matter?

A normalization job can contain several instruments.

The Engine needs to distinguish which stream each event belongs to.

For example:

```text
BTCUSDT Trades
ETHUSDT Trades
SOLUSDT Trades
```

These should not lose their individual identities merely because they are processed in one batch.

### Is `stream_id` the same as `instrument_id`?

No.

The instrument ID identifies the financial instrument.

The stream ID identifies the logical market-data stream.

For example, BTCUSDT trades and depth can share one instrument identity while having different stream IDs.

---

## Stream Ranks

The prepared task includes:

```json
"stream_rank": 5
```

### Why use stream ranks?

Several streams may contain events with identical timestamps.

A deterministic ordering contract can use stream ranks when resolving applicable ties.

### Example

Suppose a batch contains:

```text
BTCUSDT Trades
ETHUSDT Trades
SOLUSDT Trades
```

The prepared streams might receive:

```text
BTCUSDT → Rank 1
ETHUSDT → Rank 2
SOLUSDT → Rank 3
```

### Does a lower rank mean a more important instrument?

Not necessarily.

Stream ranks are processing-order metadata.

They can be derived from persisted market ranking positions, but they are not a universal economic priority.

### Do ranks change original timestamps?

No.

### Do ranks override depth reconstruction?

No.

Stateful source-event dependencies remain mandatory.

---

## Ranking Snapshot Resolution

Normalization preparation can use persisted market-ranking information.

For example:

```json
{
  "ranking_policy": "turnover",
  "ranking_snapshot": null
}
```

### Why include the policy?

The selected policy identifies the preferred source of stream-ordering evidence.

### What happens when a snapshot exists?

Forge can use its stored instrument positions when ordering the selected streams.

### Are the stored positions copied directly?

Not necessarily.

Forge can assign dense stream ranks to the selected workload.

For example:

```text
Stored ranking positions:

BTCUSDT → 1
ETHUSDT → 4
SOLUSDT → 8
```

A selected three-stream workload may receive:

```text
BTCUSDT → 1
ETHUSDT → 2
SOLUSDT → 3
```

The stored positions provide ordering evidence.

The generated ranks describe the selected processing streams.

### What if ranking positions are missing?

Forge uses deterministic stream identity ordering for the applicable fallback.

### Does preparation build a new market ranking?

No.

It uses retained ranking information.

It does not automatically refresh exchange statistics or publish new ranking snapshots.

---

## Family Archives

## What Is a Family Archive?

Some providers publish historical data grouped by instrument family.

For example, an OKX futures archive may contain records for several dated contracts.

Conceptually:

```text
BTC Futures Family
        |
        +-- Contract A
        +-- Contract B
        +-- Contract C
```

The archive path may identify the family rather than one concrete contract.

### Why is this different from an ordinary task?

A concrete instrument task identifies one instrument.

A family-scoped task may need to identify several instruments from the source records.

### What does Rust need?

It needs enough information to:

- Identify the source family.
- Read the native instrument identity.
- Resolve the concrete instrument.
- Apply the correct historical specification.
- Route the event to the appropriate stream.

### Why not assign one instrument ID to the family?

Because a family is not necessarily one financial instrument.

Different futures contracts can have different expirations and other instrument-specific properties.

---

## Family Scope

A family-scoped task can describe a native source family.

For example:

```json
{
  "scope": {
    "kind": "family",
    "identifier": "BTC-USD_UM"
  },
  "instrument_selection": {
    "mode": "all"
  }
}
```

This is a structural fragment.

### What does `scope.kind` mean?

It identifies the source selection as a family.

### What does `scope.identifier` mean?

It contains the provider-native family identifier.

### Is the family identifier a canonical instrument ID?

No.

It is the native source grouping.

Concrete instrument identities must still be resolved.

---

## Instrument Selection Modes

The current normalization family-selection contract supports explicit selection behavior.

### All instruments

For example:

```json
{
  "instrument_selection": {
    "mode": "all"
  }
}
```

This selects the supported concrete instruments represented by the applicable family source population.

### Explicit inclusion

The contract also supports an include form:

```json
{
  "instrument_selection": {
    "mode": "include",
    "symbols": [
      "BTC-USD-261225"
    ]
  }
}
```

This illustrates the supported exact-symbol selection structure.

The actual symbol must match the source-native identity.

### Are wildcards supported?

Not by the explicit include list.

The symbols must be exact.

### Can the include list be empty?

No.

The current parser requires a nonempty list of unique symbols.

### Does selection establish missing instrument metadata?

No.

Selecting a symbol does not create its historical specifications.

The concrete authority must already be available.

---

## Family Parameters

The relevant family-processing contract includes information such as:

| Parameter | Purpose |
|---|---|
| `scope.kind` | Family or concrete source-scope type. |
| `scope.identifier` | Native family identifier. |
| `scope.family_id` | Optional family authority identity. |
| `scope.members` | Concrete family-member bindings where supplied. |
| `instrument_selection.mode` | Selection strategy. |
| `instrument_selection.symbols` | Exact native symbols for supported inclusion. |
| `instrument_namespace` | Canonical identity authority. |
| `source_priority` | Relevant source-precedence information. |
| `archive_member` | Selected archive member where applicable. |

### Why is source priority separate from stream rank?

Source precedence and stream ordering describe different relationships.

For example:

- Source precedence can affect reconciliation between overlapping archives.
- Stream rank can affect ordering between logical streams.

One should not be treated as an automatic replacement for the other.

### Does every family task contain every field?

No.

The concrete serialized form depends on the supported family authority and task representation.

Use the current Forge-generated job rather than inventing fields.

---

## Family Record Routing

Rust routes records using their source-native instrument identities.

For example:

```text
Family Archive
      |
      v
Read Native Symbol
      |
      +-- Contract A
      |       |
      |       v
      |   Stream A
      |
      +-- Contract B
      |       |
      |       v
      |   Stream B
      |
      +-- Contract C
              |
              v
          Stream C
```

### What can happen to a record?

A record may be:

| Outcome | Meaning |
|---|---|
| Routed | Native identity resolves to a supported selected instrument. |
| Filtered outside scope | Record belongs outside the requested family selection. |
| Filtered unselected | Instrument exists but is not selected. |
| Rejected missing identity | Native instrument identity is absent. |
| Rejected malformed identity | Native identity cannot be interpreted. |
| Rejected missing authority | Required concrete instrument metadata is unavailable. |
| Rejected unsupported instrument | Source instrument is outside the supported processing contract. |

### Why distinguish filtering from rejection?

A record outside an explicit selection may be intentionally excluded.

A record whose identity is missing or malformed represents a different problem.

For example, an unknown symbol inside a supposedly selected family may indicate missing historical metadata.

The resulting diagnostics should preserve that distinction.

---

## Why Family Authority Must Be Complete Enough

Family routing requires authoritative concrete instrument metadata.

For example, a futures record may contain:

```text
Symbol: BTC-USD-261225
Quantity: 100 contracts
```

The Engine needs the applicable contract specifications to interpret that quantity.

### Can Rust infer the multiplier from the filename?

No.

The native symbol does not automatically establish contract value.

### Can current metadata always resolve expired futures?

No.

Expired contracts may no longer appear in the provider's current catalogue.

### What can MarketForge do?

The historical metadata system can retain catalogue, explicit historical, and archive-derived identity evidence.

Where applicable, initialization can perform bounded identity discovery.

However, incomplete authority can still block preparation.

### Important limitation

**Family routing cannot safely invent missing contract specifications.**

This is particularly important for OKX futures-chain archives.

See [Historical Metadata Inference](../configuration/markets.md#historical-metadata-inference).

---

## Wire types and scope

`schema_version` is uint32 4, `kind` is `normalization_batch` and `status` is `prepared`. Required strings are `run_id`, `run_directory`, `batch_path`, `ranking_policy` and `instrument_namespace`. `requested_interval` carries `start` and `end` strings. `ranking_snapshot` is optional JSON evidence. `instruments` and `jobs` are arrays, and `families` defaults to an empty array. A job has `operation`, `job_id`, `dataset_id`, `tasks`, `ordering`, and `output`. Job and dataset IDs are positive uint64 JSON integers, each unique within its own domain across a batch.

A task requires a positive uint64 integer `task_id`, unique within its job. It also requires nonempty `input_path`, `exchange`, `instrument_type`, `market_category`, nonblank `stream_id` and uint32 `stream_rank`. Optional/nullable fields are `base_asset`, `quote_asset`, `symbol`, `instrument`, `instrument_id`, `scope`, `source_options`, `instrument_selection`. `instrument_id` is signed 64-bit and resolves against the batch authority when supplied. `source_options.feed` is `incremental` or `snapshot`. `instrument_selection` is either `{"mode":"all"}` or `{"mode":"include","symbols":[...]}`. With a concrete symbol, omit the selector or include only that exact symbol. Family scope requires a selector and leaves concrete identity fields null. Scope carries `kind:"family"`, `identifier`, optional `family_id`, and `members` defaulting to an empty array. Each explicit member has `instrument_id` and optional `archive_member`.

One stream ID maps to one rank and one rank to one stream within a job. Relative paths resolve from the Engine project root. Output destinations cannot overlap or contain parent traversal, and the configuration validation and execution perform further checks. Loader and job resolution are distinct from archive decoding. [Production loader](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/config/batch.rs)
