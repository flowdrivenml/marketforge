## Task R01 — Unified Historical Data Registration

**Suggested task file:** `.agents/tasks/data-onboarding/01-unified-registration.md`

**Scope:** Register locally supplied historical market data through the same managed registry used by exchange acquisition, including instruments, historical specifications, raw inputs, and reusable normalization profiles.

**Separate follow-up task:** Align Forge’s normalization preparation and orchestration with the current authoritative Rust input contract. Do not implement that broader integration in this task.

### Objective

Implement one configuration-driven onboarding workflow for historical data obtained outside MarketForge’s exchange downloaders.

The user must be able to supply one TOML file describing the source, instruments, specifications, input files, and trade/depth profiles. Forge should validate the configuration, show the intended changes, register verified managed copies, and install or reuse compatible profiles in the existing Rust profile catalogue.

Locally supplied data must not remain in a separate external-only archive subsystem. Acquisition origin is provenance, not a reason to maintain a second registry or normalization path.

The intended relationship is:

```text
Exchange acquisition ───────┐
                           ├── Shared raw-data registry
Local-file registration ────┘             │
                                         ├── Instrument/specification references
                                         ├── Source/feed identity
                                         ├── Profile references and fingerprints
                                         └── Verified local input copies
                                                    │
                                                    ▼
                                      Ordinary normalization preparation
                                      implemented/aligned in a later task
```

Registration does not create a normalized dataset. It prepares registered inputs and interpretation metadata for subsequent processing.

## Assignment Boundaries and Concurrent Work

Other agents are working in this repository. Inspect the current branch, working-tree changes, staged changes, and applicable `AGENTS.md` files before editing.

Implement only this assignment. Historical instructions to execute Tasks 01–12, continue through an existing queue, or update shared progress records do not authorize additional work here.

Preserve unrelated changes, including changes within files this task must touch. Do not reset, stash, revert, reformat unrelated files, change branches, or modify another agent’s reports. Do not stage, commit, or push as part of this assignment unless separately authorized.

Use an assignment-specific report:

```text
.agents/reports/data-onboarding-01-registration.md
```

Do not overwrite `.agents/reports/task-01.md`, `final-summary.md`, existing task files, or shared progress documents.

Database verification must use an explicitly verified testing environment. Do not migrate or reset a shared database underneath another running agent. Use isolated test records and, for migration tests, an isolated disposable testing database through established tooling.

## Inspect Current Contracts Before Designing the Changes

Read the current implementations of acquisition, local imports, instrument persistence, storage management, configuration loading, CLI dispatch, and database migrations. Reuse their existing publication, ownership, verification, and recovery mechanisms.

Relevant starting points include:

```text
forge/historical/archives/
forge/historical/imports/
forge/historical/normalization/
forge/markets/
forge/storage/
forge/shared/database/
forge/shared/config/
forge/shared/paths.py
forge/shared/ownership.py
forge/cli/
```

Inspect the current Rust profile loader and its actual configuration tree. The existing documentation audit records an import-format helper that depended on absent physical templates; do not extend that obsolete dependency instead of using the self-contained profile system. Recheck the current code before deciding whether the issue still exists. :chatgpt-content-reference{index="0"}

Also inspect, read-only:

```text
engine/tests/historical/fixtures/normalize.json
engine/src/historical/config/
data/.config/historical/general.toml
```

The fixture and current production Rust loader govern the future normalization handoff. Do not select a protocol version from an old task document: a later supplied report explicitly distinguishes prepared-schema-version 4 from older version-3 descriptions. Determine the actual current contract locally. :chatgpt-content-reference{index="1"}

Do not modify the authoritative fixture, downgrade Rust to match the old Python producer, or create another maintained normalization workload. This task must retain the metadata needed by the current contract, but the full job-serialization correction belongs to the follow-up assignment.

## Unify the Raw-Data Registration Model

### One registry for different acquisition methods

Refactor the existing remote/external split into a shared model. Conceptually, distinguish:

| Entity | Responsibility |
|---|---|
| Source | Provider or recorder, product/feed, and provenance |
| Archive/source object | Stable identity of a raw input object, independent of its current location |
| Captured revision/local copy | Particular bytes, checksum, managed location, availability, and acquisition history |
| Registration | Submitted recipe, frozen input selection, metadata/profile bindings, and operation outcome |

Use names consistent with the current repository. Generic `historical.sources` and `historical.archives` are reasonable targets, but avoid unnecessary renaming where an existing structure can be evolved cleanly.

Do not create an additional database, an external-instrument catalogue, or another permanent external-only archive path through the application.

An archive need not be compressed. A CSV, JSONL, Parquet, or binary file can be a registered raw object. Its extension does not establish that it is already canonical.

### Preserve provenance and acquisition behavior

Keep provider, market venue, product/feed, and acquisition method separate.

For example, a vendor supplying an exchange feed is not itself the trading venue. A manually supplied file must not be assigned `exchange = "external"`.

Remote locators must be optional for locally supplied objects. Do not invent download URLs or completed remote-discovery scans for local registrations.

Preserve existing downloader behavior, discovery-cache membership, source revision handling, and storage ownership. Route both acquisition methods through common persistence/publication services without rewriting working exchange adapters.

Do not deduplicate unrelated source objects solely because their checksums match. Physical-byte reuse, where supported, must not collapse distinct provenance or interpretation bindings.

### Migration requirements

Implement changes through new SQL migrations. Do not rewrite applied migrations or require deleting and recreating the database.

Preserve existing local-archive IDs where possible, raw-input lineage, content hashes, storage locations, and historical references. Remote and external tables may contain overlapping numeric IDs; use explicit migration mappings rather than assuming their IDs are globally unique.

Do not move existing raw files merely to remove the external classification.

Update affected browsing, selection, verification, reconciliation, cleanup, and backup consumers. A unified CLI over two permanently separate authoritative registration systems is not sufficient.

Temporary compatibility views or aliases are acceptable when documented, but they must not become a second write path. Preserve useful old import commands through shared-service delegation or provide an explicit migration path.

## One Registration TOML

### Configuration location and ownership

Provide a versioned registration recipe under:

```text
data/.config/historical/registration/<name>.toml
```

Support explicit recipe paths outside that directory.

Use the configured project/data-root infrastructure. Document path resolution clearly: repository-oriented source roots should resolve through the explicit project root, and file selections should resolve beneath the declared source root. Behavior must not depend on the current notebook or shell directory.

The recipe is a user-owned document. Generated plans, frozen configurations, receipts, and reports belong in operation-owned artifact locations, not beside every raw file as mandatory sidecars.

Use ordinary TOML tables and arrays of tables. Avoid large inline dictionaries.

### Required capabilities

The recipe must express these relationships:

| Section | Required capability |
|---|---|
| Registration metadata | Schema version and human-readable name |
| Source | Provider/recorder, product/feed, source root, and provenance |
| Instruments | One or more named instrument declarations or references |
| Specifications | Exact values, provenance, and historical applicability |
| Profiles | Complete embedded profile definitions or references to installed profiles |
| Inputs | Named file groups bound to instruments/scopes, content types, and profiles |
| Coverage | Optional declared intervals, kept separate from observed bounds |

A common instrument and specification must be reusable by both trade and depth input groups. Do not require users to duplicate the instrument declaration for every file.

Use recipe-local keys to connect sections. Users should not need to invent PostgreSQL IDs or Rust canonical instrument IDs.

Support explicit files and bounded patterns. Resolve patterns deterministically, reject unintended duplicate selections, and freeze the actual file inventory before registration. Later additions to the source folder must not silently join an existing registration.

For the initial successful workflow, support concrete instruments and separate trade/depth input groups. Do not force a genuinely multi-instrument archive into one invented instrument identity. Unsupported multi-instrument mappings must remain explicit blockers with their declarations retained.

Reject unknown registration fields, invalid references, unsupported versions, and contradictory declarations with field-level diagnostics.

## Instrument Identity and Historical Specifications

### Reuse the instrument catalogue

Resolve or register instruments through the shared catalogue and its repositories.

Creating a catalogue entry for a new venue does not automatically make that venue supported by an exchange downloader or Rust decoder. Keep those capabilities distinct.

Preserve stable instrument identity. Do not allocate IDs from row order, filenames, unstable hashes, or repeated run-local enumeration.

Do not assume a PostgreSQL instrument ID is interchangeable with an ID from Rust’s persisted instrument namespace. Inspect the existing identity contract and retain the references needed for an explicit mapping.

### Support the complete specification record

The registration model must support the fields the user already maintains:

| Field | Required treatment |
|---|---|
| `instrument_id` | Resolved catalogue identity, not a manually invented recipe value |
| `quantity_type` | Explicit quantity interpretation |
| `contract_value` | Exact decimal where applicable |
| `contract_value_asset` | Asset/currency associated with the contract value |
| `tick_size` | Exact declared trading increment where applicable |
| `qty_step` | Exact declared quantity increment where applicable |
| `min_qty` | Optional declared minimum |
| `max_qty` | Optional declared maximum |
| `min_notional` | Optional declared minimum notional with its denomination understood |
| `fetched_at` | Actual metadata observation/retrieval provenance |

Preserve decimal values exactly. Use decimal strings in configuration and appropriate exact representations internally. Do not pass financial values through binary floating point.

Missing, unknown, and inapplicable values must not become zero or fabricated defaults.

Validate relevant relationships, such as positive applicable increments, valid contract-value combinations, and consistent minimum/maximum constraints. Required fields must follow the actual supported instrument and engine contracts rather than requiring every field for every asset class.

Do not automatically reject historical trade records using order-submission limits unless an existing, explicit processing rule requires that behavior.

### Historical applicability and conflicts

Keep metadata retrieval time separate from effective validity. Registering a file today must not imply that today’s specifications apply to its historical records.

Retain specification provenance, declared effective interval when known, and an explicit uncertainty state when applicability is not established. Do not infer historical validity from `fetched_at`.

Reuse matching specification versions. Do not overwrite earlier values or mutate specifications already referenced by a registration.

Distinguish a legitimate historical change from contradictory claims about the same period. Ambiguous selection must produce a blocker, not “latest row wins.”

Where a selected interval crosses a specification change, preserve that fact for the future normalization planner. Do not apply one arbitrary specification across the entire interval.

Do not silently substitute current API metadata for explicitly supplied historical specifications. Ordinary local registration must not trigger market synchronization or network requests.

### Unsupported instrument classes

Register supported identities accurately. Where an instrument classification is not supported by the catalogue or engine, retain a precise diagnostic and the supplied declaration without coercing it into another class.

In particular, do not label equity data as a crypto spot instrument merely to satisfy an existing enum. Do not invent equity tick sizes, quantity increments, assets, or historical contract metadata.

## Trade and Depth Profile Registration

### Reuse the self-contained Rust profile contract

Embedded profile definitions must follow the current production profile schema, including applicable:

```text
schema_version
kind
id
format_code
exchange
instrument_type
market_category
source
fields
normalization
rules and source variants, where supported
```

Preserve the structure illustrated by the existing trade and depth profiles. Registration may wrap a complete profile definition, but the extracted file must be a valid standalone profile without registration-only fields.

Do not recreate separate physical-schema files or introduce a Python-only normalization language.

Profile definitions describe reusable source layouts and semantics, not individual instrument specifications. Multiple instruments and registrations must be able to reuse the same approved profile.

### Automatic placement

Install profiles into the authoritative Rust-discovered trade/depth catalogue beneath:

```text
data/.config/historical/
```

The intended organization is:

```text
profiles/
├── trades/
│   └── <profile-id>.toml
└── depth/
    └── <profile-id>.toml
```

Verify the actual loader paths before implementation. If the existing authoritative catalogue uses a different trade/depth subdirectory layout, integrate with that layout and document it rather than creating a second catalogue.

Use existing configured path resolution. Tests must publish into isolated profile roots, never modify the user’s active profiles.

Preserve the production ID and filename rules. Do not invent version suffixes, additional ID components, or provider-as-exchange substitutions that the Rust loader does not support.

### Conflict and binding rules

An installed profile with an equivalent engine-defined interpretation may be reused. An existing ID with different semantics must produce a conflict; never silently overwrite it.

Retain the original definition and its engine interpretation fingerprint. A raw-file checksum is not a substitute for the engine’s semantic fingerprint.

Where multiple source formats exist for the same venue/instrument class/content type, use the engine’s supported variant or selection mechanism. If it cannot represent an unambiguous binding, report that limitation instead of choosing the first matching file.

Do not weaken profile matching to make registration succeed.

### Engine validation

Python owns recipe structure, references, and publication. Rust remains authoritative for profile parsing, compilation, and executable normalization semantics.

Use an existing nonexecuting validation interface if available. If none exists, a narrowly scoped Rust validation entry point delegating to the production loader/compiler is permitted. It must not normalize archives, create datasets, or contact PostgreSQL.

Distinguish:

```text
TOML syntax validated
Profile structure validated
Profile semantics accepted by the engine
Bounded source evidence inspected
Full normalization not executed
```

Successful parsing or a matching header does not prove source semantics.

Trade-side interpretation, timestamp units, quantity encoding, native-ID/reset semantics, snapshot/update behavior, deletion rules, and sequence handling must remain explicit. Do not infer them from field names alone.

Unsupported profile operations must fail or remain blocked; they must not be silently ignored.

## CLI and Readiness

### Public commands

Implement a focused command family using existing CLI conventions:

```bash
mf acquisition register new NAME --from PATH
mf acquisition register edit NAME
mf acquisition register inspect NAME
mf acquisition register validate NAME
mf acquisition register plan NAME --testing
mf acquisition register run NAME --testing
```

Named recipes and explicit recipe paths must both work.

`new` creates a draft without overwriting an existing document. It may inventory bounded local evidence but must leave unknown market semantics unresolved.

`inspect` displays declarations and bounded file evidence.

`validate` performs non-mutating configuration and available engine-profile validation.

`plan` resolves the exact proposed registrations, reuse decisions, conflicts, destinations, and blockers. It may read the selected registry but must not copy raw files, install active profiles, or write registration rows.

`run` applies the validated registration through shared services. It does not execute normalization.

Preserve global machine-readable output conventions, existing terminal styling, meaningful exit statuses, and help without database initialization. Keep command handlers thin.

### Separate storage completion from processing readiness

Represent at least these independent dimensions:

```text
Input-copy verification
Instrument identity resolution
Historical-specification resolution
Profile validation and publication
Engine capability validation
Normalization handoff compatibility
```

A verified raw input may be registered while normalization remains blocked. Such an outcome must explicitly say that storage registration completed but processing readiness is blocked.

Malformed configuration, conflicting authoritative identities, unsafe files, and publication failures must not become successful-looking registrations.

Once missing metadata or profiles are supplied, support an explicit, versioned binding update that reuses unchanged managed bytes. Do not require copying the same file again merely to complete its interpretation metadata.

Preserve earlier registration evidence and any bindings already used by downstream work.

## Publication, Idempotency, and Recovery

Reuse existing managed-copy mechanisms. The initial registration mode should copy selected inputs into managed raw storage; do not add moving, deletion, hard-link adoption, or arbitrary in-place ownership in this task.

Original files remain read-only. Verify source stability around hashing and copying, and verify the final copy against the recorded checksum.

Bound file enumeration, sample reads, buffering, and concurrent work. Reject unsafe paths and unsupported filesystem objects using established path-safety rules.

Freeze the submitted recipe, input selection, resolved metadata, and intended publication actions. Revalidate relevant files and conflicts before applying a previously prepared plan.

Use short database transactions and recoverable filesystem publication. Do not hold a database transaction open while copying large files.

Stage profile files outside active discovery, validate the intended catalogue additions, and publish without overwriting existing definitions. Use applicable workspace/data-root ownership and protect shared profile destinations.

PostgreSQL and filesystem changes are not one atomic transaction. Retain sufficient durable intent and receipts to recover failures between copying, profile publication, and final database registration.

An incomplete registration must not be selectable as a fully ready input collection.

Repeated application must reuse unchanged verified copies, instruments, specification versions, and profiles. Changed source bytes create a new captured revision. Conflicting metadata creates an explicit conflict or new binding revision, not an in-place rewrite of historical evidence.

Recovery must finalize or diagnose existing artifacts without blindly recopying files, reinstalling profiles, or duplicating registry rows.

## Handoff to the Later Normalization Task

Expose a typed, origin-independent selection interface over registered inputs.

It must provide, where applicable, verified local-copy identity and path, checksum, source/product/feed identity, concrete instrument or scope, historical specification references and values, profile binding/fingerprint, declared coverage, observed evidence, and readiness blockers.

Preserve requested intervals separately from observed event bounds. Do not treat filenames, minimum/maximum timestamps, or declared coverage as proof of chronological order, continuous coverage, or reconstruction continuity.

Use the same interface for exchange-acquired and locally registered inputs.

Do not create a special external-normalization serializer. Do not fabricate current-contract fields or emit an executable batch merely by changing its version number.

The later task will align batch generation, engine invocation, output registration, and recovery with the actual fixture and Rust implementation. Record any missing contract metadata in this task’s report.

Registration must not claim end-to-end normalization readiness while that handoff remains unverified.

## Testing and the Local Databento Samples

### User-provided input location

The user intends to place externally obtained samples beneath:

```text
<repository-root>/research/databento_samples/
```

Treat this as an optional local test corpus, not a mandatory application location or proof of a particular schema.

Discover what is actually present. Do not assume filenames, instruments, dates, export settings, or formats from earlier notebook examples.

The originals must remain unchanged. Do not download replacements, call the vendor API, consume credits, inspect API secrets, or upload sample contents.

If both JSONL and binary exports represent the same observations, select the intended representation explicitly. Do not accidentally register both as separate logical coverage merely because both files exist.

Use bounded inspection for file structure and representative records. Preserve exact timestamp and price representations. Do not assume every book record is an independent snapshot or every side field represents aggressor side.

A local sample may expose an unsupported instrument class or format. That is a valid readiness-blocker test, not permission to invent specifications or broaden this assignment into a new decoder implementation.

### Automated verification

Keep ordinary tests independent of the private sample folder. Use small synthetic fixtures and isolated profile/storage roots.

Cover these behaviors:

| Area | Required verification |
|---|---|
| Configuration | Shared instrument declarations, multiple input groups, embedded/referenced profiles, invalid references, unknown fields, path resolution |
| Instruments/specifications | Exact decimals, nullable fields, historical applicability, conflicts, stable identities, no implicit network synchronization |
| Profiles | Standalone extraction, engine validation, reuse, conflicting definitions, unsupported semantics, ambiguous selection |
| Raw registration | Common registry path for acquired and supplied files, frozen selection, safe copying, checksum verification, revision preservation |
| Recovery | Interrupted copy, failed profile publication, database failure after file publication, retry without duplicate copies or identities |
| Migration | Fresh schema and upgrade from remote/external records, colliding legacy IDs, preserved local IDs and lineage |
| Compatibility | Existing acquisition, browsing, verification, cleanup protections, and backup behavior |
| CLI | Help, dispatch, JSON output, read-only planning, truthful readiness and exit behavior |

Add an opt-in local-sample check accepting an explicit source root. When explicitly requested, missing samples must produce a clear unmet-prerequisite result, not a passing test.

Retain useful integration artifacts under unique task-specific paths, for example:

```text
data/tests/historical/registration/<run-id>/
```

Do not overwrite earlier results or use production dataset destinations. Keep raw vendor files and generated artifacts out of version control.

Do not run the full normalization workload or performance benchmarks. This task may perform focused profile-validation tests and bounded registration tests; it must not imply that those establish complete trade/depth normalization correctness.

## Zensical Documentation

Update the existing documentation site alongside implementation.

The supplied documentation report identifies established acquisition workflows, configuration references, CLI references, and navigation files. Extend those rather than creating a parallel documentation tree. :chatgpt-content-reference{index="2"}

Review and update the applicable pages:

```text
docs/content/workflows/acquisition/external-data.md
docs/content/workflows/acquisition/managing-acquisitions.md
docs/content/configuration/acquisition.md
docs/content/reference/cli/acquisition.md
docs/content/workflows/markets/instrument-discovery.md
docs/content/configuration/markets.md
docs/zensical.toml
```

A dedicated registration-configuration page may be added if it keeps the reference readable. Preserve existing top-level navigation, branding, and unrelated content.

Explain the complete local registration workflow, the single-TOML schema, specification provenance, embedded profile extraction, profile destinations, reuse/conflicts, managed-copy ownership, readiness states, and recovery.

Provide complete schema-valid examples for an already supported source format, including trade and depth input groups sharing one instrument. Label synthetic fixtures clearly. Do not present guessed specifications as a real Databento or equity configuration.

Document the optional `research/databento_samples/` walkthrough only to the extent actually verified. Clearly separate registration success, profile validation, and normalization integration deferred to the next task.

Update old external-import instructions so users are not directed to a competing obsolete workflow. Preserve useful links or provide a clear migration note.

Validate documented commands against the actual parser and TOML examples against the implemented models. Build the site with its configured strict build and check affected links. Do not redesign the site or change documentation dependencies unnecessarily.

## Completion and Report

Run the project’s configured formatting, linting, type checking, and relevant focused tests. Report pre-existing failures separately; do not weaken checks or hide maintained code to obtain a clean result.

The task is complete when one registration recipe can resolve or register instruments and specifications, reuse or publish engine-compatible trade/depth profiles, and register verified local inputs through the same managed model as exchange acquisition.

Existing acquisitions and lineage must survive migration. Repeated registration must be idempotent, interrupted publication must be recoverable, originals must remain unchanged, and unsupported processing requirements must remain visible.

The implementation report must identify the actual configuration schema and commands, migrations and compatibility decisions, profile validation mechanism, publication/recovery behavior, tests executed, documentation checks, and any local samples inspected.

Include a specific handoff section for the later normalization-orchestration task. State what registered metadata it can consume and what remains unresolved against `engine/tests/historical/fixtures/normalize.json`.

**Stop after this assignment. Do not continue into normalization orchestration, other agents’ tasks, centralized dataset search, or unrelated engine work.**