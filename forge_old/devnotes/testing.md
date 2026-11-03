## Testing Organization and TUI Verification

### Directory structure

Keep all Python tests under `forge/tests/`.

Separate unit and integration tests, and organize both by responsibility. Each implemented responsibility must have relevant integration coverage.

```text
forge/tests/
├── conftest.py
├── fixtures/
├── unit/
│   ├── startup/
│   ├── shared/
│   ├── historical/
│   │   ├── archives/
│   │   ├── engine/
│   │   └── pipelines/
│   ├── datasets/
│   ├── orchestration/
│   └── tui/
└── integration/
    ├── startup/
    ├── shared/
    ├── historical/
    │   ├── archives/
    │   ├── engine/
    │   └── pipelines/
    ├── datasets/
    ├── orchestration/
    └── tui/
```

Adapt names to the final source layout while preserving this responsibility-based organization.

Create meaningful tests alongside implementation. Empty directories and placeholder tests do not satisfy coverage requirements.

### Unit tests

Test individual rules and components in isolation:

- Configuration validation and error paths.
- Discovery-cache and acquisition-planning decisions.
- Request-limiter behavior.
- Engine job/result translation.
- Dataset compatibility and lineage rules.
- Pipeline dependency resolution.
- TUI presentation helpers and isolated widgets.

Use deterministic fixtures and controlled clocks where timing matters. Ordinary unit tests must not require PostgreSQL, exchange access or real engine execution.

### Integration tests

Test the boundaries between components, including:

- Discovery adapters, cache services and repositories.
- Download finalization and database registration.
- Engine invocation, report validation and dataset registration.
- Pipeline execution and downstream dependency release.
- Recovery after interruption or registration failure.
- Application startup and configuration loading.

Use the explicitly configured testing database and temporary output locations. Never fall back to the application database.

Keep integration fixtures and cleanup scoped so ordinary tests do not erase benchmark history or unrelated records.

### TUI integration tests

Place complete application interaction tests under:

```text
forge/tests/integration/tui/
```

Cover every implemented workspace:

| Workspace | Required behaviors |
|---|---|
| Markets | Filtering, sorting, selection, metric freshness and explicit refresh actions. |
| Acquisition | YAML loading, validation errors, Map/Download dispatch, cached results and file-count progress. |
| Pipelines | Configuration editing/reloading, validation, execution mode selection and final outcomes. |
| Datasets | Category filtering, details, lineage, artifact references and registry updates. |
| Logs | Successful/failed operation visibility, filtering and report inspection. |
| Settings | Profile loading, validation, persistence and effective defaults. |

Also verify navigation, keyboard focus, empty states, service failures and recovery from errors.

Test actual effects, not merely that a button exists or a screen renders.

### External processes and terminal behavior

Inject controllable editor, terminal-launch and execution adapters for routine automated TUI tests. CI must not open desktop terminal windows.

Verify:

- The correct saved YAML file is passed to the editor.
- Configuration is reloaded after editing.
- Progress and silent modes dispatch correctly.
- Launcher exit alone is not mistaken for operation completion.
- Failures restore the Forge interface.
- Registration-pending results remain distinct from successful completion.

Use separate terminal/PTY smoke tests where necessary to verify real terminal handoff and restoration. Headless screen tests alone cannot establish those behaviors.

### Complete workflow coverage

Include representative integration scenarios covering:

```text
Load YAML → Map → Download → Normalize → Merge → Convert
    → Register outputs → Inspect datasets and logs
```

Use small deterministic inputs and controlled services. Add appropriate real-engine contract tests without duplicating the Rust engine's entire test suite.

Verify failure paths as well as success, especially incomplete discovery, failed transfers, invalid reports and database registration failure.

### Execution and quality requirements

Keep external-network tests explicitly opt-in.

Run unit tests and the integration suites relevant to each change. Required integration coverage must not be silently replaced by mocks or skipped without explanation.

Tests and fixtures must meet the same typing, linting and formatting standards as application code.

Do not claim all functionality is verified because unit tests passed. Report which suites ran, which were skipped and any remaining verification limits.
