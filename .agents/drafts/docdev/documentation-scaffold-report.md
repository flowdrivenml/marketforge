# MarketForge documentation scaffold report

Completed 2026-11-14. Changes remain uncommitted on the existing `codex-refactor` branch. No staging, commits, branch changes, fetches, pushes, deployment, application jobs, or historical task execution were performed.

## Files changed by this assignment

- `README.md`: redesigned introduction, five section links, requested section order, conservative installation draft, roadmap, documentation/contribution entry points, and licensing notice. The pre-existing working README included a large pasted implementation assignment; it was replaced as part of the authorized README rewrite, not executed.
- `.gitignore`: appended only documentation environment, output, and cache exclusions. Preserved its pre-existing normalization-example exception.
- `docs/README.md`: dependency installation, preview, build, source boundaries, and draft-content conventions.
- `docs/requirements.txt`: pinned `zensical==0.1.1`, a version confirmed available and successfully installed.
- `docs/zensical.toml`: source/output locations, explicit navigation, local branding, system fonts, dark-first palette and light-mode toggle.
- `docs/content/index.md`.
- `docs/content/getting-started/index.md`, `installation.md`, and `quickstart.md`.
- `docs/content/workflows/index.md`.
- `docs/content/configuration/index.md`.
- `docs/content/reference/index.md`.
- `docs/content/development/index.md`, `architecture.md`, and `extending.md`.
- `docs/content/assets/stylesheets/extra.css`.
- Assignment-only artifacts under `.agents/drafts/docdev/`: this report, `baseline.json` (initial status, pre-existing file hashes and staged diff), `check_links.py`, and three browser screenshots (`overview-dark.png`, `overview-light.png`, `overview-mobile.png`).

Created the separate ignored `docs/.venv/`, `docs/site/`, and Zensical cache. Existing branding files were reused without modification. No `CONTRIBUTING.md` was needed: the Development page is the contribution entry point.

## Structural and visual decisions

The site has exactly six top-level navigation entries: Overview, Getting Started, Workflows, Configuration, Reference, Development. Ten short content pages provide orientation without a tutorial catalog. Architecture and extension outlines sit under Development. Existing technical records remain outside `docs/content/` so their migration does not imply an audit.

The README uses repository-relative documentation links and ordinary section anchors; Getting Started lands at Installation, immediately followed by Quick Start. Two static badges link to the actual package manifests. No published site URL is invented.

The supplied banner and small logo are used locally. A restrained Tokyo Night-inspired dark palette, readable secondary text, system font stacks, subtle borders, and a light-mode alternative support terminal-oriented presentation. No new artwork, remote fonts, or promotional panels were introduced. Empty screenshot/diagram directories were omitted until assets exist. Assignment screenshots are verification records, not product demonstrations.

## Factual sources inspected locally

- `.agents/AGENTS.md`: general safety, project boundaries and conventions. User instructions superseded numbered-task, commit, and progress-reporting instructions for this assignment.
- `forge/pyproject.toml`: Python 3.12+, Hatchling package layout, editable-install mapping, `marketforge`/`mf` scripts, development tools, and MIT metadata.
- `forge/cli/main.py`: actual command groups, global options and help parsing before application initialization.
- `forge/cli/commands/normalization.py`: `new`, `validate`, `prepare`, testing restriction and nonexecuting preparation.
- `forge/startup/context.py`, `forge/shared/paths.py`, and configuration module inventory: workspace resolution and application configuration location.
- `forge/historical/normalization/batch.py`: schema-3 producer, preparation/report boundaries, and integration diagnostics.
- `forge/historical/normalization/README.md`: module-local context, checked against current code rather than accepted as a current Rust contract.
- `forge/historical/imports/formats.py`: bounded format inspection and draft generation, unresolved semantics and Engine-validation limitations.
- `engine/Cargo.toml`: Rust edition 2024, feature gates, historical/live/terminal targets and shared components.
- `engine/src/bin/marketforge-normalize.rs`: batch/job CLI options and historical runtime default path.
- `engine/src/historical/config/batch.rs`: schema-4 prepared-batch loader requirement.
- `engine/src/shared/historical/config.rs`: current runtime loader structure.
- Local file inventories of `forge/`, `engine/docs/`, `engine/examples/`, and `docs/`: component boundaries, existing technical material and live TOML example location. Large datasets and private configuration contents were not inspected.
- `docs/content/assets/branding/logo.png` and `logo_small.png`: inspected file types; banner viewed and both rendered successfully. Existing extensions do not match actual encoded types (WebP and JPEG); preserved unchanged because browsers load them successfully.
- Git tracked-file licensing search: no license file found. The Python manifest's MIT declaration alone was not turned into a project-wide license claim.

Zensical configuration was checked against official documentation, not the outdated MarketForge remote repository:

- [Basics](https://zensical.org/docs/setup/basics/)
- [Navigation](https://zensical.org/docs/setup/navigation/)

The installed Zensical 0.1.1 build is the practical syntax validation.

## Intentional gaps and unresolved questions

- In Action: visible placeholder for a real terminal capture with inspected output and provenance.
- Application installation: supported by manifests but not executed. Native dependency/platform setup and complete database initialization still need a verified walkthrough.
- First workflow: help commands are established by the parser, but were not executed. No invented output or sample processing command sequence is included.
- Concrete integration mismatch: Forge currently emits normalization batches with `schema_version = 3`; Rust prepared batches require `schema_version = 4`. Forge also retains older diagnostics about Rust accepting only individual v3 jobs; those diagnostics are not treated as current Engine truth. A compatible producer/consumer walkthrough needs separate engineering verification, outside this assignment.
- Current source coverage, end-to-end imports, live operation, and complete research workflows were not assessed. Module presence is distinguished from readiness.
- Broad algorithmic workflows are direction, without dates or trading claims. New documentation is not used as evidence that an existing component is unimplemented.
- Command, configuration, data-format, output, contributor-test, and extension references intentionally remain compact drafts.
- Licensing needs maintainer clarification; no license was chosen or created.

## Verification actually performed

- Inspected branch, initial working-tree status and staged changes before editing. Staged diff was empty and remains unchanged.
- Installed Zensical 0.1.1 into the newly created documentation-only environment. Forge dependencies and existing environments were not changed.
- Ran strict builds successfully, including after final documentation/CSS changes: `Build started / No issues found / Build finished`.
- `check_links.py` passed: 12 Markdown files (README, local development guide, ten content pages), README section fragments, 11 generated HTML pages (including generated auxiliary page), and 376 local generated link/asset references. This checks local destinations and HTML fragments; external badge services were not availability-tested.
- Started Zensical preview on loopback port 8017 and loaded it using Playwright with local Chrome. Confirmed all six top-level labels, successful image decoding, actual system font use, initial slate theme, working light-mode toggle, and navigation to Installation. No JavaScript errors, failed requests or remote asset requests were observed in those page loads.
- Viewed desktop dark/light and 390px mobile screenshots. Mobile document width did not exceed viewport width. The first browser automation attempt timed out because the theme control is a labeled radio rather than a button; the corrected label-based click passed. No application change was needed.
- Reviewed the README/ignore diff and all new authored pages/configuration. Documentation-scoped `git diff --check` passed. New Markdown/CSS/TOML files were also checked for trailing whitespace.
- Verified environment and generated-site exclusions using `git check-ignore`; cache exclusion added explicitly.
- No application installation, Rust build, application test suite, acquisition, normalization, benchmarks, migrations, live streams or runtime-data writes were performed. A successful documentation build is not an application workflow test.

### Concurrent working-tree activity

Initial status already contained extensive Engine, old Forge, historical documentation, branding, README, and `.agents/` changes. This assignment deliberately preserved those outside the authorized README rewrite and appended documentation ignores.

Hash comparison detected concurrent changes in these pre-existing Engine files during the assignment: `engine/src/historical/normalize/executor.rs`, `progress/reporter.rs`, `scheduler.rs`, `session.rs`, `progress/diagnostics.rs`, `reconciled.rs`, and `engine/tests/historical/normalize/execution/process_executor.rs`. This assignment did not edit or revert them. Remaining baseline file hashes were unchanged at the recorded comparison. The checker reports concurrent differences as a notice rather than falsely attributing them to documentation or reverting them.

No instruction file, historical task queue, normalization draft, report, issue record, or shared progress file was edited by this assignment.

## Exact local commands

From the repository root, create the documentation environment only if absent:

```bash
python3 -m venv docs/.venv
docs/.venv/bin/python -m pip install -r docs/requirements.txt
(cd docs && .venv/bin/zensical serve)
(cd docs && .venv/bin/zensical build --strict)
python3 .agents/drafts/docdev/check_links.py
```

The smoke preview used this port override and was stopped after verification:

```bash
(cd docs && .venv/bin/zensical serve --dev-addr 127.0.0.1:8017)
```

## Existing documentation left in place

All existing `docs/historical/`, `docs/tofixlater/`, `engine/docs/`, `engine/scripts/README.md`, and Forge module-local documentation remain in place, including their pre-existing modifications and deletions. No wholesale engineering-record migration was attempted. Some old links and statements in those records may be stale; validating them was outside this focused scaffold assignment.
