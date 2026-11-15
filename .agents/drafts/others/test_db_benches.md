## Preliminary Note: SQL Migrations, Database Environments and Benchmark Registration

### Purpose

Move database schema definitions out of embedded Python strings and into version-controlled SQL files.

Maintain separate production and testing databases. Both must use the same application schema migrations, while the testing database also contains benchmark records.

Benchmark execution should produce durable reports and register their results in PostgreSQL through a Python runner/importer.

### Database environments

Use two separately configured PostgreSQL databases:

| Environment | Purpose |
|---|---|
| Production | Normal application operation, archive inventory, datasets and pipeline execution. |
| Testing | Integration tests, development validation and benchmark history. |

Both databases may run on the same PostgreSQL server.

They must be distinct databases, not merely different schemas inside one database.

Use explicit connection settings, for example:

```text
MARKETFORGE_DATABASE_URL=postgresql://.../marketforge
MARKETFORGE_TEST_DATABASE_URL=postgresql://.../marketforge_test
```

Tests and benchmark commands must require the testing connection explicitly. A missing testing connection must not silently fall back to production.

Validate the actual connected database identity before destructive test setup or cleanup. Checking only the environment-variable name is insufficient.

### Shared application schemas

Production and testing must receive the same core migrations.

This includes the existing catalogue and configuration schemas, the proposed historical archive schema, and any pipeline execution schema introduced by the orchestration implementation.

Do not maintain independent production and testing copies of the application DDL. They would eventually diverge and make test results less representative.

Testing-only additions should be applied separately:

```text
Production database:
    Core application migrations

Testing database:
    Core application migrations
    Benchmarking migrations
    Explicit test fixtures where required
```

### SQL file organization

Place shared database infrastructure and SQL migration files under the agreed shared boundary:

```text
forge/shared/database/
├── connection.py
├── migrations.py
└── sql/
    ├── core/
    │   ├── 0001_baseline.sql
    │   ├── 0002_historical_archives.sql
    │   └── ...
    ├── benchmarking/
    │   ├── 0001_benchmark_registry.sql
    │   └── ...
    └── fixtures/
        └── ...
```

Filenames illustrate the intended organization. Determine actual migration numbering after inspecting the repository and existing database state.

Core migrations apply to both environments. Benchmarking migrations apply only to the testing environment. Fixtures must never be applied implicitly to production.

Python remains responsible for selecting the connection, loading SQL files, executing migrations and reporting outcomes.

### Migration behavior

Replace embedded schema DDL with SQL-file execution.

Track applied migrations in PostgreSQL, including:

- Migration stream, such as `core` or `benchmarking`.
- Migration version/name.
- SQL checksum.
- Application timestamp.

Apply migrations in an explicit order. Do not recursively execute every SQL file without distinguishing migrations, fixtures and environment-specific additions.

Use transactions where the SQL operation supports them. Prevent concurrent migration runners from applying the same migration simultaneously.

Applied migrations should be immutable. Add a new migration for subsequent changes.

`CREATE TABLE IF NOT EXISTS` is not a replacement for schema evolution. Existing tables require explicit migrations when columns, constraints or indexes change.

### Existing database transition

Inspect the actual existing schema before establishing a migration baseline.

Verify its compatibility with the proposed baseline before marking migrations as already applied. Do not assume the current schema-version number alone proves an exact match.

Preserve existing production records.

The production migration path must not depend on dropping and recreating the database.

Static seed values may also be moved into SQL files where appropriate. Exchange API synchronization remains application behavior.

### Testing database lifecycle

Treat the testing database as a managed environment containing both disposable test data and useful benchmark history.

Tests should clean up only the records or fixtures they own, using rollback or explicit cleanup where practical.

Routine test setup must not erase the entire benchmarking schema.

Provide a separate, explicitly requested reset operation if a complete testing-environment reset is needed.

Use separate output destinations for tests and benchmarks so they cannot overwrite production datasets.

For example:

```text
data/
├── raw/
├── processed/
├── testing/
└── benchmarks/
```

Existing datasets may be used as benchmark inputs when explicitly selected, but generated outputs must use isolated destinations.

### Benchmark execution responsibility

Keep the Rust benchmark binaries focused on executing benchmarks and producing reports.

A Python benchmark runner should:

1. Create a benchmark execution record.
2. Resolve the input datasets and execution configuration.
3. Invoke the appropriate benchmark binary.
4. Capture its exit outcome and logs.
5. Read and validate the generated report.
6. Register the report and individual trials in the testing database.
7. Record report and artifact locations.

An importer should also support registering an already generated benchmark report without rerunning the benchmark.

Inspect the actual report structures produced by the processing, merge and conversion benchmark binaries before implementing their adapters.

Do not require PostgreSQL dependencies inside every Rust benchmark binary merely to register its results.

### Benchmark database schema

Create a testing-only `benchmarking` schema.

The initial registry should contain at least:

| Table | Responsibility |
|---|---|
| `benchmarking.runs` | One benchmark invocation or imported report, including configuration, provenance and overall outcome. |
| `benchmarking.trials` | Individual measured executions, such as worker-count and repetition combinations. |

A benchmark run should record:

- A stable execution identifier.
- Benchmark kind: processing, merge or conversion.
- Status and failure details.
- Start/completion timestamps.
- Source job configuration and its checksum.
- Input dataset references and manifest/checksum information.
- Engine revision, build profile and relevant build features.
- Available hardware and runtime environment information.
- Report location, checksum and original JSON payload.
- Log and generated-output locations.

A benchmark trial should record:

- Parent benchmark run ID.
- Stable trial key and repetition index.
- Requested and actual resource settings where available.
- Execution phase, such as full execution or a separately measured phase.
- Success, failure or cancellation outcome.
- Reported timing measurements.
- Reported event, depth-level and output counts.
- Reported throughput measurements with explicit units.
- Output references and error details.
- Original operation-specific trial data.

Use typed columns for common fields needed for filtering and comparison. Preserve additional operation-specific measurements in JSONB initially.

The exact DDL should follow the inspected report contracts.

### Report interpretation

Preserve the original report. Normalize useful fields without discarding information that does not yet have a dedicated column.

Do not invent measurements.

Unavailable memory statistics, timings or worker counts should remain absent or null. Do not substitute zero for an unreported measurement.

Preserve units and measurement meaning. Events per second, depth levels per second and bytes per second are different measurements.

Keep materialization, publication and total timings distinct when the report provides them.

Record enough input and configuration identity to distinguish comparable benchmark trials from executions using different datasets or settings.

### Failure and partial-result handling

Create the benchmark execution record before invoking the binary.

A failed process must leave a failed execution record even if no JSON report was produced.

If a valid partial report exists, preserve and register its available results while retaining the overall partial or failed outcome.

Malformed reports should produce an import error associated with the execution. They must not be treated as successful benchmark results.

Generated benchmark datasets must not enter the normal completed dataset registry unless they pass the same publication and registration requirements as other datasets.

Benchmark measurements and dataset registration are separate responsibilities.

### Idempotent importing

Importing the same report more than once must not duplicate runs or trials.

Use stable execution/trial identities together with report checksums.

If an existing identity is presented with different report contents, handle it explicitly rather than silently overwriting previously recorded results.

A database registration failure should be recoverable by rerunning the importer, without rerunning the benchmark.

### Management interfaces

Expose database initialization/migration, benchmark execution and report importing through application services that the CLI and future TUI can reuse.

The interface should make the selected database environment visible.

Historical benchmark results should remain queryable across test sessions. Future comparison pages and charts can build on this registry without changing how benchmark binaries execute.

### Initial acceptance criteria

- Schema DDL is stored in version-controlled SQL files.
- Production and testing receive the same core migrations.
- Testing-only migrations and fixtures cannot be applied accidentally to production.
- Existing production data survives migration.
- Missing testing configuration never falls back to production.
- Ordinary test cleanup preserves benchmark history.
- Processing, merge and conversion reports can be imported through dedicated adapters.
- Failed benchmark executions remain visible even without a report.
- Reimporting an identical report does not create duplicates.
- Report registration can be retried without repeating benchmark execution.
- Original reports, configurations and relevant provenance remain available.
