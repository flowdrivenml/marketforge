# Acquisition and markets documentation report

Completed 2026-11-14. Documentation only; all changes remain uncommitted on `codex-refactor`. No application code, runtime settings, databases, acquired data or retained test artifacts were modified by this assignment. No provider requests, acquisition jobs, metadata/statistics collection, migrations, ranking publication/deletion or application test suite was executed. No historical task, commit, staging, branch change, fetch or push was performed.

## Documentation created

Twelve new user pages:

- `docs/content/workflows/acquisition/index.md`: acquisition boundaries, prerequisites and command effects.
- `docs/content/workflows/acquisition/historical-data.md`: provider matrix and recipe → cache plan → discovery → download/reuse → inspection workflow.
- `docs/content/workflows/acquisition/external-data.md`: import lifecycle, bounded format recognition, identity/specifications, draft limitations and readiness.
- `docs/content/workflows/acquisition/managing-acquisitions.md`: stored files/coverage, run reports, cache, verification/recovery, cleanup and troubleshooting.
- `docs/content/workflows/markets/index.md`: metadata/statistics/archive/ranking distinctions and effects.
- `docs/content/workflows/markets/instrument-discovery.md`: support matrix, sync membership, filtering and qualified identities.
- `docs/content/workflows/markets/market-statistics.md`: collection, provider quantities/units, native versus estimated turnover, funding/open interest availability, source/fetch/window times and freshness.
- `docs/content/workflows/markets/rankings.md`: policy lifecycle, publication, inspection, conversion/volume evidence, standard updates and maintenance boundaries.
- `docs/content/configuration/acquisition.md`: complete recipes/import/settings examples, fields, defaults, constraints, scope and calendar differences.
- `docs/content/configuration/markets.md`: complete turnover/volume policies, conversion precedence, controls, defaults and standard-policy requirements.
- `docs/content/reference/cli/acquisition.md`: acquisition/import hierarchy, global options, cache planning, storage/run inspection and explicit maintenance syntax/effects/exit behavior.
- `docs/content/reference/cli/markets.md`: exact markets/ranking commands, available/hidden/no-op options, filters, JSON/display differences, publication/deletion and exits.

Four existing documentation files updated:

- `docs/zensical.toml`: nested navigation within the original six top-level sections.
- `docs/content/workflows/index.md`, `configuration/index.md`, `reference/index.md`: entry links and scope notices.

The homepage, README, global CSS, assets, Getting Started and Development content were not redesigned or edited. No engineering documents were moved or removed. Assignment checks/results and two rendering-review images were saved only under `.agents/drafts/docdev/` with new assignment-specific names; prior notes/reports/checkers were preserved.

## Functionality documented

Provider archive selection and discovery for Binance, Bybit, OKX, Bitget and Gate.io; differences between source scopes and current-market support; timezone-aware half-open intervals; whole-archive selection; structural validation versus availability; authoritative cache refresh and six-hour reuse; verified local reuse; failure/report/recovery semantics; external raw import and bounded layout inspection; nominal storage coverage and deliberate maintenance.

Current instrument metadata, supported spot/linear/inverse scopes and dated contracts, retained absent records, current sync prerequisites, current statistics and unit limitations, safe browsing, native denomination sorts, turnover conversion, grouped base-volume comparison, immutable published snapshots, exclusions/comparability/freshness, standard-policy update behavior, and guarded snapshot maintenance.

No speculative feature was presented as implemented. External-to-processed-data integration and profile validation remain explicitly unverified. No normalization, live-streaming, feature-engineering or strategy manual was added.

## Source verification

Primary sources were the local working tree, including uncommitted files:

- `.agents/AGENTS.md`, read for general safety; task-specific overrides respected.
- `forge/cli/main.py`, `market_filters.py`, and commands `acquisition.py`, `imports.py`, `markets.py`, `rankings.py`, `storage.py`, `storage_cleanup.py`, `maintenance.py`, `database.py`.
- `forge/historical/archives/config.py`, `templates.py`, `sources/exchanges.py`, `sources/base.py`, `planning.py`, `operation.py`, `scans.py`, and `storage/files.py`.
- `forge/historical/imports/models.py`, `formats.py`, `service.py`.
- `forge/markets/capabilities.py`, `models.py`, `providers/public.py`, `repository.py`, `service.py`, `filters.py`, `interpretation.py`.
- `forge/markets/ranking/policy.py`, `service.py`, `inputs.py`, `volume.py`, `workflow.py`, `deletion.py`.
- `forge/shared/config/models.py`, `library.py`, `paths.py`, `terminal/editor.py`, `http/configured.py`, `database/testing.py`, `database/status.py`; `forge/orchestration/artifacts.py`, `inspection.py`; storage `models.py`, `browsing.py`, `coverage.py`, `registry.py`, `service.py`.
- Existing `forge/SERVICES.md`, `forge/historical/archives/README.md`, `forge/markets/README.md`, `TURNOVER.md`, `ranking/README.md`, and `forge/storage/README.md`. Implementation/test evidence took precedence over historical verification claims in those records.

Relevant tests read or inspected for contract evidence (not executed): archive source/regression fixtures, import plan/format tests and copy/revision tests, CLI acquisition/markets/filter/ranking workflow tests, provider/futures/turnover-scope/filter tests, ranking conversion/volume/ordering tests, and archive reuse/publication-recovery tests. In particular:

- `forge/tests/unit/historical/archives/test_sources.py`, `test_source_regressions.py`.
- `forge/tests/unit/historical/imports/test_plans_formats.py` and `forge/tests/integration/historical/imports/test_copy.py`.
- `forge/tests/integration/historical/archives/test_acquire.py`.
- `forge/tests/integration/cli/test_acquisition.py`, `test_markets.py`, `test_market_filters.py`, `test_ranking_workflow.py`.
- `forge/tests/unit/markets/test_futures.py`, `test_turnover_scope.py`, `test_filters.py`, and `ranking/test_ranking.py`, `test_volume.py`.

Runtime configuration contents, secrets, database rows and datasets were not needed for these claims. Only configuration directory/file inventories were checked. No remote repository or live provider documentation was used as authority for local functionality.

## Examples and safe verification

`verify-acquisition-markets.py` validates examples without dispatching handlers:

- **72 shell examples** parsed through the actual current CLI parser, including required testing/cache flags and command hierarchy. No service handler invoked.
- **Six complete TOML examples** loaded with `tomllib` and current models: two AcquisitionConfig recipes, one AppSettings document, one ImportConfig, two RankingPolicy documents (turnover and grouped volume).
- Both acquisition recipes additionally passed the adapter's pure request-support validator without creating an HTTP client or sending a request.
- Import configuration validation was schema-only; illustrative recording paths were not read/frozen. Profile meaning, real contract specifications and coverage were not asserted.
- Ranking policy checks were schema-only, without observations, price graph resolution, database writes or publication. Time-sensitive rates and fabricated outputs were avoided.
- **43 command help paths** checked by parser help with exit 0, including import format, storage maintenance, ranking deletion, doctor and database setup/inspection. Two actual module help commands were also run safely: `acquisition plan --help` and `markets rank update --help`.

Full parser/help/schema results are in `acquisition-markets-verification.json`. Validation scripts use the existing application environment only to import the parser/models; documentation dependencies/environment were not changed. Python bytecode writing was disabled for application imports.

## Implementation issues and documentation gaps

1. **Format drafting prerequisite is absent.** The exposed import format helper expects physical templates in `data/.config/historical/formats/trades/` or `formats/depth/`. Both directories are absent in the current tree. The user guide/reference explicitly say this command cannot currently complete here. Raw import remains separate. No profiles or application files were created to work around this.
2. **Maintenance filters are not forwarded.** `storage verify/reconcile` accept status/origin, but dispatch calls `maintain` with only root, limit and archive ID. Docs warn not to rely on those filters and recommend an exact ID for targeted maintenance. No code correction made.
3. **Testing URL loaders differ.** Most relevant guarded commands read the environment or literal workspace secrets file. The older database status/migrate path uses the environment only. Prerequisites explicitly distinguish them.
4. **Current-market hidden options can mislead.** Shared parsing accepts limit/sort on sync/refresh/show; they do not bound network collection. `markets list` lacks an instrument-type selector; rank show has one but lacks exchange/market/instrument filters. References document actual availability rather than inventing symmetry.
5. **Collection result naming.** Metadata sync also returns its identifier in `refresh_id`, distinguished by `metadata`; no fictitious `sync_id` output field is documented.
6. **Standard updates are not generic policy updates.** Only turnover/volume names are accepted; existing files are required, and the new-policy template is always turnover. Update globally refreshes statistics and can publish one policy while another fails. These constraints are explicit.
7. **External registration is not processing approval.** Format/profile semantic validation and a complete external-import-to-processing workflow are not established. Unknown format bytes can be registered, with readiness labels; no arbitrary-format guarantee is made. Re-registering an existing content identity is not documented as a metadata/profile migration mechanism.
8. Provider availability, expired-contract completeness, exact real file sizes, current instruments/prices, database provisioning and example end-to-end success were not tested. Source-supported routes are labeled separately from observed data availability.

An earlier scaffold link checker also found an existing README link to absent `NOTICE`. This assignment's baseline already contained that README and it was not edited. The new site has no such broken link. README/licensing work is outside this assignment and was left alone.

## Verification results

- `(cd docs && .venv/bin/zensical build --strict)`: passed, no issues, including after final content corrections.
- Documentation-scoped link checker: **23 documentation Markdown files**, **23 generated HTML pages**, **1438 local generated link/asset references**, no missing destinations/fragments. Top-level six-section navigation preserved; examples and source/site relative links resolve.
- Browser: local Zensical preview on loopback port 8017, Playwright/local Chrome. All 12 new pages returned 200 with expected heading, six top navigation links, dark theme, tables/code and no desktop overflow. Three representative 390px mobile pages had no horizontal document overflow. No JavaScript errors or failed requests observed. Desktop acquisition-configuration and mobile statistics screenshots were opened and visually reviewed. No global design changes were needed.
- `git diff --check` for documentation: passed. Explicit all-authored-file checks confirmed no trailing whitespace and final newlines; this includes untracked new pages that Git diff alone does not cover.
- Branch/status and staged diff inspected before edits. Staged diff remained unchanged. Baseline hashes show no modifications to unrelated application/configuration files by this assignment. Two Engine test files changed concurrently: `engine/tests/historical/normalize/source_ordering.rs` and `engine/tests/historical/support/depth_assertions.rs`; they were neither edited nor reverted here.
- No application tests, HTTP provider requests, PostgreSQL connections or operational examples were executed. Builds/parser/schema checks are not claims of successful acquisition/statistics/ranking runs.

Check artifacts: `acquisition-markets-baseline.json`, `verify-acquisition-markets.py`, `acquisition-markets-verification.json`, `check-acquisition-markets-links.py`, `acquisition-markets-link-results.json`, `acquisition-markets-browser-results.json`, `acquisition-configuration-review.png`, `market-statistics-mobile-review.png`.

### Reproduce documentation checks

From the repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. .venv/bin/python .agents/drafts/docdev/verify-acquisition-markets.py
(cd docs && .venv/bin/zensical build --strict)
python3 .agents/drafts/docdev/check-acquisition-markets-links.py
(cd docs && .venv/bin/zensical serve)
```

The browser smoke used `serve --dev-addr 127.0.0.1:8017`; that task-owned preview was stopped after verification. Generated output stays in ignored `docs/site/`.

## Remaining work

Separately resolve the format-helper/catalogue prerequisite and maintenance-filter issue before presenting affected paths as complete. Establish a credential-free verified environment/bootstrap guide, then record bounded, authorized live provider and database walkthroughs. Expand data-format/processing contracts in their own assignment. Review older engineering records and the unrelated README link separately; no wholesale migration or speculative platform expansion was attempted here.
