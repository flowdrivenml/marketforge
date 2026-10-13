### Concept

Develop a modern **Textual-based Terminal UI (TUI)** to simplify MarketForge configuration, execution, and monitoring.

**Core principle:** Configure once → Plan → Validate → Execute → Reuse → Stream.

The TUI acts as a frontend to the existing Python orchestration and Rust processing engines, preserving full CLI functionality.

### Interface

Six main workspaces:

- **Explore:** Browse exchanges, instruments, and market metadata.
- **Pipelines:** Create, edit, save, and reuse configurations.
- **Jobs:** Execute and monitor downloads, processing, and merging.
- **Datasets:** Browse historical archives, normalized data, and merged Parquet outputs.
- **Streaming:** Manage real-time feeds, publishers, and algorithm bootstrapping.
- **Settings:** Processing profiles, storage, concurrency, and system configuration.

Default workspace: **Pipelines**.

Visual inspiration: Posting, Harlequin, Lazygit, btop.  
Framework: **Textual (Python)** with Tokyo Night styling.

### Pipeline Configuration

Users configure:

- Exchanges, markets, instruments, and date ranges.
- Tick trades and L2 order-book data.
- Normalization, validation, quantity conversion, and physical merging.
- Processing profiles and resource limits.
- Output destinations: Parquet, PostgreSQL, shared memory.
- Historical and real-time streaming requirements.

Support reusable configurations, instrument filtering, and saved presets.

### On-Demand Planning

**No global historical availability scraping.**

Availability is discovered only for user-selected instruments and periods.

Workflow:

1. Configure the pipeline.
2. Generate an acquisition plan.
3. Discover and validate historical availability.
4. Review missing data, reusable archives, and estimated download sizes.
5. Confirm and execute.

Cache discovered availability for future requests.

Distinguish unavailable data from failed or incomplete discovery.

### Architecture

```text
             CLI / TUI
                 │
          Application API
                 │
          Job Orchestrator
                 │
      ┌──────────┼──────────┐
      │          │          │
  Acquisition  Processing  Streaming
    Python       Rust     Python/Rust
      │          │          │
      └──────────┼──────────┘
                 │
       Parquet / PostgreSQL
           Shared Memory
```

Separate:

- **Pipeline configuration:** What the user requests.
- **Execution plan:** Resolved inputs, dependencies, and operations.
- **Job state:** Execution progress, checkpoints, and results.

### Key Capabilities

- One-click execution of complete pipelines.
- Persistent jobs, retries, and resumable processing.
- Automatic reuse of existing archives and datasets.
- Incremental dataset updates.
- Multi-exchange chronological merging.
- Historical-to-live streaming transitions.
- Stateful algorithm bootstrapping from sequential historical events.
- Dataset provenance and dependency tracking.

### Implementation Direction

1. Define reusable pipeline configuration schemas.
2. Introduce a unified pipeline orchestrator.
3. Build the Textual TUI and instrument selector.
4. Add planning, validation, and execution monitoring.
5. Integrate dataset management.
6. Add real-time streaming and bootstrapping.
7. Package Python + Rust as a standalone executable.

**Goal:** Transform MarketForge from a collection of CLI commands into a unified, high-performance market-data infrastructure application that remains simple to configure and operate.