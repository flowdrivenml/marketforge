**Decision:** Live processing monitoring and the interactive terminal interface (TUI) belong entirely to Rust, not Python.

**Python responsibilities:**
- Acquisition, metadata, and processing-job planning.
- Launching the Rust processing engine.
- Reading the final `ProcessingResult`.
- Updating dataset lifecycle in PostgreSQL.
- Future global scheduling across multiple Rust processes.

**Rust responsibilities:**
- Processing execution and worker management.
- Resource monitoring and enforcement.
- Processing and integrity metrics.
- Live error reporting.
- Interactive TUI using Ratatui + Crossterm.

### Live Monitoring

The Rust TUI must display processing progress, resource consumption, and integrity errors together.

```text
Rust Processing Engine
        ↓
Shared Processing + Integrity Metrics
        ↓
Rust Monitor
        ├── Progress and throughput
        ├── CPU, memory, workers, scratch
        ├── Integrity counters
        └── Recent errors
        ↓
Terminal Interface
```

Support monitoring modes:

```text
none
simple
json
tui
```

**Requirements:**
- Monitoring must not block processing workers.
- Error history must remain bounded.
- Fatal errors and degraded processing must be distinguishable.
- TUI rendering must remain optional for scripts and headless execution.
- Final machine-readable `ProcessingResult` remains on stdout.
- Progress events and diagnostics use stderr.

### Implementation Order

1. Shared processing metrics.
2. Integrity accounting and policy enforcement.
3. Rust resource monitoring.
4. Rust TUI and progress reporting.
5. Python integration for launching Rust and consuming final results.

**Boundary:** Python orchestrates processing jobs; Rust executes, monitors, and displays their live progress.