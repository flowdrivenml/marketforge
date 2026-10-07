Python acts as the MarketForge control plane while Rust executes heavy historical-data processing.

## Quick Navigation

- [Responsibility Boundary](#responsibility-boundary)
- [ProcessingJob](#processingjob)
- [Job Storage](#job-storage)
- [ProcessingResult](#processingresult)
- [Development Strategy](#development-strategy)
- [Final Runtime Flow](#final-runtime-flow)

## Responsibility Boundary

```text
Python
    → CLI
    → PostgreSQL/catalog access
    → input selection
    → dataset planning
    → resource planning
    → stream ordering
    → job creation
    → dataset/lineage registration

Rust
    → raw processing
    → normalization
    → validation
    → temporary runs
    → merging
    → BookStore
    → transformations
    → Parquet
    → manifest
    → processing/integrity metrics
```

Core boundary:

```text
Python
    ↓
ProcessingJob
    ↓
────────────────────
Rust
    ↓
ProcessingResult
    ↓
────────────────────
Python
```

Rust does not require direct PostgreSQL access.

## ProcessingJob

Python eventually produces one complete, frozen execution specification:

```text
ProcessingJob
├── protocol version
├── job / dataset identity
├── operation
├── inputs
├── streams
├── stream ranks
├── raw-format information
├── instrument specifications
├── normalization configuration
├── integrity policy
├── resource configuration
├── output configuration
└── monitoring configuration
```

Operations may include:

```text
process
merge
```

Both use the same job infrastructure.

Rust receives resolved values rather than user-facing values such as:

```text
auto
8G
500G
```

Python resolves these before creating the job.

## Job Storage

Processing jobs are persistent small JSON specifications:

```text
data/
├── raw/
├── jobs/
├── .work/
└── datasets/
```

Example:

```text
data/jobs/42.json
```

Execution:

```bash
marketforge-process --job data/jobs/42.json
```

The directories have separate responsibilities:

```text
jobs/
    persistent processing specifications

.work/
    disposable temporary Arrow runs

datasets/
    permanent finished datasets
```

Job specifications are retained for reproducibility and debugging.

## ProcessingResult

Rust returns a small structured result containing information such as:

```text
protocol version
job ID
dataset ID
status
manifest path
events written
files written
actual timestamp range
integrity status
error information when applicable
```

Process convention:

```text
stdout
    → machine-readable final result

stderr
    → logs / diagnostics / progress

exit code 0
    → successful execution

non-zero exit code
    → failed execution
```

Python uses `ProcessingResult` to update PostgreSQL dataset state.

## Development Strategy

The Python planner should **not be implemented before the Rust processing protocol stabilizes**.

During Rust development, use manually maintained job fixtures:

```text
engine/
└── fixtures/
    └── jobs/
        ├── trades.json
        ├── depth.json
        ├── combined.json
        └── merge.json
```

Development flow:

```text
raw fixtures
    +
job JSON fixture
    ↓
marketforge-process
    ↓
Rust processing
    ↓
Parquet + manifest
```

This allows `ProcessingJob` to evolve freely while the Rust engine is being implemented.

If Rust discovers that another field is required:

```text
change ProcessingJob
    ↓
change fixture JSON
    ↓
continue development
```

Python does not need to be updated repeatedly while the protocol is still unstable.

Initial implementation order:

```text
1. Rust ProcessingJob structs
2. JSON deserialization / validation
3. development job fixtures
4. Rust processing engine
5. stabilize ProcessingJob
6. stabilize ProcessingResult
7. freeze protocol version
8. Python job models
9. Python planner
10. Python JobStore
11. Python Rust runner
12. CLI integration
```

The Rust `ProcessingJob` model is authoritative during early development.

## Final Runtime Flow

Once the protocol is stable:

```text
User / CLI
    ↓
Python Planner
    │
    ├── PostgreSQL metadata
    ├── selected inputs
    ├── instrument specs
    ├── raw formats
    ├── normalization rules
    ├── integrity policy
    └── resource planning
    ↓
ProcessingJob
    ↓
data/jobs/{job_id}.json
    ↓
marketforge-process
    ↓
Rust Engine
    ↓
ProcessingResult
    ↓
Python
    ↓
catalog.datasets / dataset_inputs
```

The protocol should be versioned independently from:

```text
database schema version
canonical schema version
manifest version
```

> Develop Rust against hand-written job fixtures first. Freeze the protocol only after the processing engine proves what information it actually requires; then implement Python job generation around that stable contract.