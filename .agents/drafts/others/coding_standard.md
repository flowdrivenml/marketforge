## Coding Standard: Rigorous Python Development for Forge

### Objective

Maintain Python code that is consistently formatted, explicitly typed and clean under the project's configured static checks.

Working behavior alone is insufficient for completion. New and rewritten code must also pass linting, formatting, strict type checking and the tests appropriate to the change.

Python annotations do not replace runtime validation or behavioral tests. Use these checks together.

### Fix diagnostics at their source

Treat editor diagnostics as actionable findings.

Do not assume a diagnostic is cosmetic merely because existing tests pass or the affected code has not yet failed during normal execution.

The supplied screenshots show both structural defects and formatting/import problems.

In the reviewed code:

- `AcquisitionPlanner.download_count` and `reuse_count` access `self.downloads`.
- `AcquisitionPlanner` does not define that attribute.
- `AcquisitionPlan` owns `downloads` and already implements the corresponding properties.

If those planner properties are accessed, they can raise `AttributeError`.

Remove the incorrect duplicate properties and use the returned plan's properties. Do not add dummy planner state, introduce dynamic attribute handling or suppress the diagnostic merely to obtain a clean editor.

Likewise, consolidate the duplicated imports and move module imports above constants such as `SOURCE_FACTORIES`.

### Standard toolchain

Use one primary tool for each responsibility:

| Responsibility | Tool |
|---|---|
| Static type checking | Pyright in strict mode |
| Linting and import organization | Ruff |
| Formatting | Ruff formatter |
| Behavioral testing | pytest |

Avoid introducing competing formatters, linters or type checkers without a concrete project requirement.

The editor, local checks and CI must use the same project configuration and compatible tool versions.

### Configuration location and scope

Keep authoritative Python tooling configuration in:

```text
forge/pyproject.toml
```

Configure checks to cover maintained Forge source, tests and scripts.

Adapt discovery paths to the actual package layout after restructuring. Do not retain stale paths that accidentally cause checks to inspect nothing or omit new modules.

Exclude generated artifacts, build outputs and dependency environments where appropriate.

Do not exclude maintained application modules simply because they produce diagnostics.

Avoid conflicting `pyrightconfig.json` or nested Ruff configuration files that silently override the intended project settings.

The supported Python version must agree across project metadata, type checking, editor interpretation and CI. Do not require a language-version upgrade merely to perform cleanup.

### Suggested configuration baseline

Merge the following policy into the existing configuration rather than replacing unrelated project settings:

```toml
[tool.ruff]
line-length = 100
extend-exclude = ["build", "dist"]

[tool.ruff.lint]
select = [
    "E4",     # Import and statement-layout errors
    "E7",     # Statement errors
    "E9",     # Syntax/runtime-related errors
    "F",      # Undefined names, unused imports and redefinitions
    "I",      # Import organization
    "UP",     # Modern syntax for the supported Python version
    "B",      # Common bug patterns
    "ASYNC",  # Relevant asynchronous-code checks
]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"

[tool.pyright]
typeCheckingMode = "strict"
include = ["."]
exclude = ["build", "dist"]
reportUnnecessaryTypeIgnoreComment = "error"
```

Verify the configuration against the versions selected for the project.

Use a deliberate rule set. Do not enable every available rule indiscriminately or add rules that conflict with the formatter.

### Type annotations and contracts

Annotate function and method parameters and return values, including meaningful internal helpers, callbacks, factories and test fixtures.

Declare and initialize meaningful instance state explicitly.

Prefer:

- Dataclasses for straightforward internal records.
- Pydantic models for external configuration and validated input.
- Enums or literal types for constrained states and operation kinds.
- `TypedDict` for structured mappings where appropriate.
- Protocols or callable types for service, factory and callback contracts.

Do not force every internal object into a Pydantic model.

Avoid bare `dict`, `list`, `tuple` and callback annotations without meaningful element or signature types.

Use `object` for genuinely unknown values that require inspection. Do not spread `Any` through internal interfaces to avoid resolving the actual data shape.

Use narrow, useful interfaces rather than elaborate abstractions created solely to satisfy typing.

### External data boundaries

Treat exchange responses, YAML, database rows and engine reports as external data.

Parse and validate them at the relevant adapter or repository boundary, then return typed application objects.

For example:

```text
Exchange JSON → source adapter → validated archive records
YAML → configuration loader → validated request/pipeline model
Database rows → repository → typed records
Engine JSON → result adapter → validated execution result
```

Application services should not repeatedly inspect arbitrary dictionaries because validation was deferred.

Validate relevant invariants as well as primitive types: time intervals, identifiers, non-negative sizes, supported states and required field combinations.

Do not use `typing.cast()` as a substitute for runtime validation. A cast changes the type checker's interpretation; it does not verify the value.

### Optional values and state ownership

Handle optional values before accessing their members.

An annotation saying a value may be absent must correspond to an explicit control-flow decision.

Do not introduce convenient fallback values that conceal missing metadata or invalid state.

Keep derived properties on the object that owns their underlying data. Do not duplicate state across planners, results and services merely to support the same calculation.

Avoid attaching undeclared attributes dynamically.

Do not introduce `__getattr__`, broad unions or `Any` to conceal an incorrect ownership model.

### Imports and module structure

Use this module order:

```text
Module docstring
Future imports, when used
Standard-library imports
Third-party imports
Project imports
Constants and type aliases
Classes and functions
```

Let Ruff organize imports.

Remove duplicate and unused imports rather than suppressing their diagnostics.

Use explicit imports. Avoid wildcard imports.

Local imports and `TYPE_CHECKING` blocks are allowed when justified, but should not become general workarounds for circular dependencies.

Do not move imports required by runtime model construction or annotation inspection behind `TYPE_CHECKING` merely to satisfy a lint rule.

Resolve architectural import cycles through module responsibilities where practical.

### Readability and formatting

Let the formatter handle layout.

Do not manually split every short signature or type annotation across many lines. Avoid unnecessary adjacent string fragments and excessive visual separators.

Use descriptive names and concise functions with clear responsibilities.

Introduce a type alias when it makes a complex contract easier to understand.

Lambdas are acceptable when their role and inferred types remain clear. Prefer named, annotated factory functions when a factory signature becomes ambiguous.

Keep comments focused on intent, contracts and non-obvious decisions. Avoid comments that merely repeat the next statement.

### Third-party typing limitations

Install appropriate compatible typing stubs when available.

If a dependency exposes incomplete types, isolate that interaction behind a narrow adapter or a justified local stub.

Do not allow an untyped dependency to erase the contracts of the rest of the application.

An unavoidable suppression must be:

- Narrowly scoped.
- Associated with a specific diagnostic.
- Explained locally.
- Removed when the underlying limitation is resolved.

Do not use blanket file-level suppression, global missing-import suppression or broad lint exclusions as cleanup strategies.

Do not weaken strict mode merely to make a check pass.

### Asynchronous code

Preserve explicit resource ownership and cleanup.

Shared HTTP clients, database connections, file handles and subprocesses must have clear lifecycle boundaries.

Do not leave coroutines unawaited or background tasks unowned. Retain task references and collect their failures.

Keep concurrency and buffering bounded.

Do not block the event loop with synchronous HTTP, large filesystem operations, database calls or subprocess waits. Use an appropriate asynchronous implementation or bounded offloading.

Cancellation must propagate correctly after necessary cleanup and state recording.

Do not introduce asynchronous wrappers around blocking work while leaving the blocking operation on the event-loop thread.

### Errors and observability

Catch specific exceptions where the application can make a meaningful recovery decision.

Do not use broad exception handling to return empty results or successful-looking defaults after failures.

Top-level exception handling may record an operation failure and restore terminal/resources, but it must preserve the failure outcome.

Preserve useful exception context when translating errors.

Backend services should return typed results or raise meaningful exceptions. Rendering, prompting and terminal presentation belong in the appropriate interface layer.

External input validation must not rely on assertions that disappear under optimized execution.

### Editor consistency

Ensure the editor uses the intended Python interpreter and installed project dependencies.

Ensure Pyright and Ruff discover the configuration in `forge/pyproject.toml`, including when the repository root is the editor workspace.

If diagnostics differ between the editor and CLI, investigate interpreter, import resolution, configuration discovery and tool versions.

Do not change application code to compensate for an incorrectly configured editor environment.

Retain useful diagnostics. Do not disable them globally to make the editor appear clean.

### Required checks before handoff

Run the configured static checks across the maintained Forge scope.

From the Python project directory, the expected commands are:

```bash
cd forge

ruff check .
ruff format --check .
pyright --project pyproject.toml --warnings
python -m pytest tests/unit
```

Run additional integration tests appropriate to changes involving PostgreSQL, engine contracts, filesystem publication, cancellation or acquisition behavior.

Network tests should be explicitly invoked rather than silently contacting exchanges during ordinary unit checks.

Use the project's chosen dependency environment. Document one reproducible way to install and invoke these tools.

After safe automatic fixes, review the diff and rerun checks. Do not apply broad unsafe fixes without inspecting their behavioral impact.

### Testing expectations

Test behavior and contracts.

For relevant changes, cover invalid external input, missing values, failure propagation, cancellation, resource cleanup and result registration.

Do not add tests whose only purpose is reproducing formatter output or asserting trivial implementation details.

For the illustrated ownership bug, verify the behavior exposed through the returned acquisition plan and relevant callers.

Existing passing tests do not justify ignoring a static error in an untested code path.

### CI and completion requirements

Run the same linting, formatting and type-check commands in CI.

Checks must inspect the intended source, tests and scripts. Do not obtain a green result by silently shrinking their scope.

Every new or rewritten module must meet the standard immediately. If untouched legacy code requires staged cleanup, identify that work explicitly rather than hiding it with broad exclusions.

Before declaring work complete:

- Ruff reports no violations under the configured policy.
- Ruff formatting checks pass.
- Pyright reports no errors or warnings under the project configuration.
- Relevant tests pass.
- Editor import/type resolution agrees with the configured project environment.
- No broad suppression or weakened checking was introduced to conceal defects.

If a required check cannot run, report that limitation and its reason. Do not claim it passed.

### Tool references

- [Pyright configuration](https://github.com/microsoft/pyright/blob/main/docs/configuration.md)
- [Pyright command-line options](https://github.com/microsoft/pyright/blob/main/docs/command-line.md)
- [Ruff configuration](https://docs.astral.sh/ruff/configuration/)
- [Ruff formatter](https://docs.astral.sh/ruff/formatter/)
