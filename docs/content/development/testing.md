# Testing

Choose the smallest test that crosses the boundary you changed. Commands on this page start at the checkout root unless noted.

| Change | Focused starting point | What it establishes |
| --- | --- | --- |
| Provider requests and archive discovery | `forge/tests/unit/historical/archives/test_source_regressions.py` | Scope, intervals and rejection of malformed listings with controlled HTTP responses |
| Forge CLI, job preparation and managed installation | `forge/tests/integration/cli/test_config.py`, `forge/tests/integration/cli/test_merge.py`, `forge/tests/unit/historical/{conversion,engine}/` | Command behavior, artifact retention and handoff structure under test-controlled inputs |
| Profile interpretation and historical normalization | Cargo target `historical_normalize`, filter `profiles::` or the relevant worker test | The parser, compiler or processing behavior selected by the test |
| Exact depth boundaries and source order | Cargo target `historical_normalize`, filter `depth_boundary::`, plus relevant CSV/depth tests | Boundary state and fingerprint semantics. Decode and replay tests exercise additional invariants |
| Merge or conversion validation and persistence | Cargo targets `historical_merge` and `historical_convert` | Engine behavior under generated or explicitly selected committed inputs |
| Registry lineage, migrations, and cross-language execution | `forge/tests/integration/forge_workflow/test_workflow.py` | The selected real Forge/Rust/PostgreSQL scenario with fixture-owned schemas |
| Python publication discovery and readers | `forge/tests/unit/datasets/` | Manifest handling, provenance conflicts, timestamp and decimal fidelity, and selected-file inspection |
| Documentation | Strict Zensical build, link checker and browser preview | Site generation, available targets and rendered diagrams |

Inspect the selected test and its fixture before relying on it. A mocked Engine invocation can establish a CLI preflight branch. A real loader test can establish input acceptance. A committed publication assertion can establish the publication behavior it observes. Exact value and state assertions are still needed to prove data semantics.

## Focused commands

Use the environments in [Local development](setup.md). Python tests use the pytest configuration in `forge/pyproject.toml`. The lab group provides Arrow and Polars for dataset tests. The selected tests below use fixtures or mocks and require no external service. Three optional conversion cases skip unless you supply a fresh converter.

```bash
uv run --project forge --locked --group dev --group lab pytest -c forge/pyproject.toml \
  forge/tests/unit/historical/archives/test_source_regressions.py \
  forge/tests/unit/datasets forge/tests/unit/historical/conversion \
  forge/tests/integration/cli/test_config.py forge/tests/integration/cli/test_merge.py
```

To exercise the real converter with Forge-generated jobs and synthetic merge input, build the debug binary and opt in explicitly. These tests use temporary directories and substitute fixture discovery, so they exercise the process handoff and committed output without a database.

```bash
cargo build --locked --manifest-path engine/Cargo.toml --features historical --bin marketforge-convert
MARKETFORGE_TEST_CONVERT_BINARY="$PWD/engine/target/debug/marketforge-convert" \
  uv run --project forge --locked --group dev --group lab pytest -c forge/pyproject.toml \
  forge/tests/unit/historical/conversion/test_conversion.py -k forge_prepared_job_runs_in_real_engine
```

Use pytest `-k` to filter names or give an exact `path::test_name`. Check the test count, skips, and exit status. For a changed Python source, run the configured Ruff linter and formatter and strict Pyright on the affected paths. For example:

```bash
uv run --project forge --locked --group dev ruff check forge/historical/archives/sources
uv run --project forge --locked --group dev ruff format --check forge/historical/archives/sources
uv run --project forge --locked --group dev pyright -p forge/pyproject.toml forge/historical/archives/sources
```

Pyright's configuration targets Python 3.12 and names the repository-root `.venv`. If your project environment lives elsewhere, use Pyright's `--pythonpath` to point to that environment's interpreter, such as `forge/.venv/bin/python`.

For a Rust change, enable the feature gate of the affected target. These examples compile the requested features and run only the named test selection. Test-generated files use the fixture's temporary or retained output directories.

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
cargo check --locked --manifest-path engine/Cargo.toml --features historical
cargo test --locked --manifest-path engine/Cargo.toml --features historical \
  --test historical_normalize depth_boundary::
cargo test --locked --manifest-path engine/Cargo.toml --features historical \
  --test historical_merge merge_depth_stream::
cargo test --locked --manifest-path engine/Cargo.toml --features historical \
  --test historical_convert portable::validation_cli_checks_inventory_and_planning_without_writing -- --exact
```

Cargo's name filter can select zero tests and still exit successfully. Confirm that the intended case ran. For a wider portable check, feature matrix, Clippy target selection, and per-case deadline tooling, start with the [bounded Rust verification guide](https://github.com/flowdrivenml/marketforge/blob/main/engine/scripts/README.md). Run feature configurations in sequence when they share a Cargo target directory. Engine CLI harnesses can launch the binary at Cargo's stable path. The per-case supervisor lists unless passed `--run` and fails an empty runnable selection.

There is no checked-in CI workflow defining an additional required matrix in this checkout. Repository guidance calls for the applicable formatting, lint/type, and focused verification checks. Run opt-in network or recorded workloads deliberately. A marker or feature gate alone does not imply that an ignored case will run.

## Testing database and artifacts

The guarded CLI and shared fixture resolver use `TEST_DATABASE_KEY` first and can fall back to the same key in `.secrets/postgres.env`. The connection checks for `marketforge_test` and the `testing` purpose marker. Some pytest integration fixtures explicitly skip without the environment variable. Follow the [testing registry setup](setup.md#set-up-the-testing-registry-when-needed) and inspect the selected fixture before running it. These operations can write real files and database records.

The Forge workflow fixture additionally requires `MARKETFORGE_FORGE_INTEGRATION=1`, a managed normalizer, historical runtime settings and profiles, and a testing role with schema creation privileges. It redirects catalogue, historical and ranking SQL to randomly named schemas, applies migrations there, and drops those schemas on fixture cleanup. Its ordinary cases use pytest temporary roots. The optional Databento BYOD case has external JSONL prerequisites and retains isolated output under `data/tests/historical/normalize/`. Other integration fixtures have their own cleanup scope. [Workflow fixture and tests](https://github.com/flowdrivenml/marketforge/blob/main/forge/tests/integration/forge_workflow/test_workflow.py) · [BYOD example](../examples/bring-your-own-data/databento/index.md)

To run the synthetic trade/depth workflow case after setting up the guarded registry and normalizer:

```bash
MARKETFORGE_FORGE_INTEGRATION=1 uv run --project forge --locked --group dev --group lab \
  pytest -c forge/pyproject.toml \
  forge/tests/integration/forge_workflow/test_workflow.py::test_trade_depth_registration_prepare_edit_execute_retry
```

Inspect skips and prerequisite failures separately from assertions. Use [recorded Engine smoke selections](https://github.com/flowdrivenml/marketforge/blob/main/engine/scripts/README.md#recorded-smoke-tests) only when the required archives or committed publications are available. Normalization uses feature gates including `historical-real-tests` and `historical-full-tests`. The recorded conversion target has a bounded prefix test and a separately ignored full publication case. Relocation variables in the verifier guide are for exact copies of the documented inputs.

## Write and maintain meaningful fixtures

A depth snapshot opens a book at a known state. Suppose it contains bid 100 with quantity 2 and ask 101 with quantity 3. An absolute update to the bid at 100 with quantity 1 should leave bid 100 at quantity 1 and the ask unchanged. If the applicable source operation interprets a zero as removal, a subsequent bid update to zero should remove that bid. This is a synthetic oracle. State which source rule applies, then assert the levels, boundary type, and event offsets or zero-row observation expected from the test. Checking only a total row count could miss a side swap or a failure to remove a level.

| Fixture family | Maintenance |
| --- | --- |
| Forge provider listings | Keep sanitized responses next to the tests in `forge/tests/unit/historical/archives/fixtures/`. Reproduce relevant URI and calendar evidence with a mock transport. Preserve positive and negative cases and omit private credentials or signed links. |
| Synthetic Rust records and publications | Use the nearby support builders or temporary fixtures in `engine/tests/historical/`. Express expected values independently and retain the reason for unusual timestamps, decimals, sequences or boundaries. |
| Python inspection | `docs/content/examples/notebooks/dataset_fixtures.py` constructs small Parquet and manifest examples. Temporary tests verify discovery and exact values without a registry. |
| Recorded historical workloads | The maintained normalization batch is `engine/tests/historical/fixtures/normalize.json`. The merge fixture is alongside it. Conversion also has committed job fixtures under `engine/tests/historical/convert/fixtures/jobs/`. The documents identify dependencies. Large raw and publication bytes are separate local inputs. |
| Generated output | Keep recordings and generated data outside Git. Use a fresh output root for each run and preserve persistent historical regression evidence. Derive new expectations from source records and the contract before updating snapshots, counts or hashes. |

Missing recorded input is a prerequisite failure or an explicit skip under the suite's configured rules. A fixture change should explain why its previous expectation changed and which consumer or invariant it exercises. [Historical verifier](https://github.com/flowdrivenml/marketforge/blob/main/engine/scripts/README.md) · [Notebook fixture builder](https://github.com/flowdrivenml/marketforge/blob/main/docs/content/examples/notebooks/dataset_fixtures.py)

## Conversion verification

The portable conversion suite generates small causal publications and exercises the production binary. The test below builds a complete output without database or provider access.

```bash
cargo test --locked --manifest-path engine/Cargo.toml --features historical \
  --test historical_convert
```

To inspect a portable output directly, export the 12-row dashboard fixture into a temporary directory and run it with the managed historical converter.

```bash
mf engine build convert
preview=$(mktemp -d)
MARKETFORGE_CONVERSION_PREVIEW_DIR="$preview" \
  cargo test --locked --manifest-path engine/Cargo.toml --features historical \
  --test historical_convert portable::export_conversion_dashboard_preview -- --ignored --exact
job=$(find "$preview" -name job.json -print -quit)
.bin/marketforge-convert --job "$job" --validate
.bin/marketforge-convert --job "$job" --run
jq '{statistics, integrity}' "$(dirname "$job")/converted/manifest.json"
```

Expect 12 physical rows, including one reconstruction record, with six identity, five converted and one unavailable valuation. Both `source_preservation` and `conversion_validation` pass. To inspect the monitor, use `--run --tui` in an interactive terminal with a fresh fixture destination.

The optional recorded check requires the exact pinned Bybit publication. A relocated copy can be selected with `MARKETFORGE_RECORDED_CONVERT_INPUT`. The prefix case materializes 4,096 events, including causal reconstruction and reference prehistory. The ignored `publishes_canonical_recorded_conversion` test is the full recorded replay and requires space for the converted publication.

```bash
cargo test --locked --manifest-path engine/Cargo.toml --features historical-real-tests \
  --test historical_recorded_convert validates_canonical_recorded_prefix_with_causal_prehistory \
  -- --exact --nocapture
```
