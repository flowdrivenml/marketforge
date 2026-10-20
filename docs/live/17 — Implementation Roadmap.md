## 17 — Implementation Roadmap

**Project:** MarketForge Live  
**Planning baseline:** documentation and Rust-engine snapshots supplied on 2026-10-20  
**Purpose:** sequential, contract-first implementation with complete component work packages and bounded AI-assisted execution  
**Implementation status:** planned; no production Live implementation or test success is asserted by this document

## 1. Execution policy and scope

This document defines how to build MarketForge Live from the supplied Rust repository. Documents 00–16 remain architecture references. They are not reopened for another general editing pass. Confirmed execution resolutions are recorded here; new implementation-critical ambiguities are resolved before the affected code is written, without requiring an unrelated documentation rewrite.

The objective is **to implement each component against its complete approved v1 contract, including observability and operational behavior, and then compose components without redesigning their interfaces**. It is not a promise that code will never change. Correctness defects, measured performance problems, new exchange behavior, or explicitly approved requirement changes can require changes. Avoidable revisits caused by forgetting metrics, shutdown, configuration, replay, or TUI requirements are not an acceptable default workflow.

The implementation remains in the existing `marketforge-engine` package, whose Rust library is addressed as `marketforge_engine`. The roadmap uses `<engine>` for its actual directory: normally `engine/` in the full repository, or the root of the supplied engine-only snapshot. All new paths below are relative to that crate unless explicitly described as repository-relative.

### 1.1 Initial product

MarketForge Live provides Rust-only continuous trade and L2 acquisition; generic exchange protocols and normalization; connection and subscription supervision; source-demand ownership; integrity and recovery; canonical routing; Unix market-data publication; RAM/Parquet retention; optional PostgreSQL recording; derived merges and eligible currency conversion; native metrics and health; configuration reconciliation; a versioned Unix control API; and an independent Ratatui client.

The intended exchange matrix remains Binance, Bybit, OKX, Bitget, and Gate.io across Spot, Linear, and Inverse families, subject to verified capabilities. A directory or draft profile is not evidence that a family/channel works. Every intended capability receives an explicit verification status.

Bootstrap remains a separate application. **Its Live-side requirements are included, not postponed:** demand ownership, retention protection, committed coverage queries, cursor replay, L2 initialization boundaries, recording reads, and replay-to-live handoff. Bootstrap orchestration, historical/live dataset assembly, and its dedicated TUI are not implemented inside the Live daemon.

The current documents also include an optional Prometheus exporter. Include that adapter as a disabled-by-default consumer of native measurements; do not require or operate Prometheus/Grafana, and do not put HTTP management commands on the metrics endpoint. This reflects the supplied current documents, rather than earlier conversation preferences.

### 1.2 Evidence, decisions, and unknowns

Three categories must stay distinct:

| Category | Treatment |
|---|---|
| Observed repository fact | Cite the actual path and symbol in the source inventory. Do not infer that a suggested architecture path already exists. |
| Selected implementation policy | Defined by this roadmap and the approved contract records; implemented and tested deliberately. |
| Unresolved detail | Assigned a decision owner, prerequisite package, and blocking consumer. Do not invent an answer in a coding task. |

A minor heading or illustrative naming error in Documents 00–16 does not halt the project. A conflict about event identity, money, durability, ownership, cancellation, or wire compatibility blocks the affected package until a recorded decision exists.

## 2. Observed repository baseline

The supplied engine snapshot contains substantial historical processing code, but only a placeholder Live entry point. `src/live/mod.rs::run()` prints `marketforge-live`; `src/bin/marketforge-live.rs` calls it. Document 17 is empty in the supplied documentation snapshot. These are observations from source inspection, not runtime verification.

### 2.1 Reuse map grounded in the supplied code

| Existing path / symbol | Observed behavior or boundary | Implementation consequence |
|---|---|---|
| `src/canonical/event.rs::CanonicalEvent` | Contains `Trade` and per-level `L2LevelUpdate` variants | Do not pretend this enum already represents a grouped Live depth event. Preserve it for historical compatibility; establish a shared grouping/container boundary. |
| `src/canonical/l2.rs::L2LevelUpdate` | Absolute level quantities; zero removes a level; unavailable quantity is not deletion | Reuse the financial representation without float intermediates or alternate deletion semantics. |
| `src/formats/depth/outcome.rs` | Defines `DepthProcessingOutcome`, `DepthEventBoundary`, `DepthSourceOperation`, and source metadata | Reuse the logical message grouping. Make the required model accessible outside historical-only execution through an approved extraction/re-export. |
| `src/book/batch.rs::BookStore::apply_batch` | Validates the batch before mutation and preserves change order | Reuse for canonical mutation; do not build a separate Live book. |
| `src/book/snapshot.rs::BookStore::apply_snapshot` | Returns differences from the prior state | A new independently reconstructable generation needs complete initialization, not merely a diff from an old generation. |
| `src/book/sequence.rs::SequenceTracker` | Generic consecutive/ranged/unsequenced policies | Reuse where compatible; exchange-specific bridge/checksum/reset semantics remain strategy responsibilities. |
| `src/historical/normalize/parquet/depth_events.rs` | Separates a logical event ordinal from canonical level-row offsets/counts | Preserve this distinction when adding one Live cursor per logical depth event. Historical row offsets are not Live event offsets. |
| `src/historical/merge/core/ordering.rs::MergeKey` | Orders timestamp, stream rank, task rank, and event ordinal | Extract compatible ordering primitives; define the Live tie-break mapping explicitly rather than importing the historical executor. |
| `src/historical/convert/shared/decimal.rs::ConversionDecimal` | Uses Decimal256-style fixed scale/precision and checked arithmetic | Preserve conversion precision/rounding; do not substitute a smaller numeric representation for convenience. |
| `src/instrument/catalog.rs` and `src/job/instrument.rs::InstrumentSpec` | Catalog identity/currency information and economic instrument specifications are separate representations | Build an immutable metadata adapter that supplies all required economics. The currency catalog alone is not proof that contract-value information is available. |
| `src/tui/processing.rs`, `merging.rs`, `conversion.rs` | Existing terminal applications contain styling and historical workflow dependencies | Reuse/extract presentation primitives and theme; do not import historical execution into the Live TUI. |
| `src/lib.rs` | `formats` and `historical` are feature-gated; `live` has its own gate; `process` is a compatibility re-export | Plan shared extraction and feature wiring before Live consumers are built. Check actual imports rather than assuming the illustrative module tree is implemented. |
| `Cargo.toml` | Edition 2024; empty `live` feature; historical optional dependencies; `monitor-tui`; explicitly registered nested test targets | Establish the feature/test matrix early. The roadmap does not imply Tokio, a PostgreSQL driver, or TOML support is already configured. |

Source inspection establishes candidates and risks, not blanket reuse suitability. Contract compatibility and actual builds determine the final extraction boundary.

### 2.2 Inputs not established by these snapshots

The two supplied snapshots do not establish the actual database schema/migrations, Cargo lockfile, locally installed Rust/OpenCode versions, complete instrument data, or the 45 Live TOML file contents. They must be located in the real checkout or supplied before the dependent implementation is accepted. No migration, exchange endpoint, quota, quantity rule, or authentication behavior is fabricated to fill those gaps.

The engine has many existing tests, including archive-dependent and benchmark-style targets. Classify them before declaring a standard test command. Missing external datasets must produce an explicit unavailable/skipped result, not a false regression pass.

## 3. Implementation resolutions carried by this roadmap

The following resolutions preserve the decisions already established in the correction pass. They govern implementation even where an old illustrative example remains in Documents 00–16.

| ID | Execution resolution | Required verification |
|---|---|---|
| R01 | `StreamLifecycle::Active` means the logical runtime is running, not that acquisition is ready | A running source can reconnect, resynchronize, or repair coverage without acquiring an invented lifecycle variant. |
| R02 | Persistent deactivation removes only persistent acquisition intent | Recording/subscriber/derived/retention demand survives; restoration consults effective demand. |
| R03 | Configuration acceptance is validated and durably committed before published reconciliation effects | Persistence failure cannot mutate runtime state; accepted revision and applied/adopted state remain distinct. |
| R04 | A cursor is `(StreamId, LogEpoch, offset)` | It survives RAM/Parquet/recording representation changes; no runtime-generation substitution. |
| R05 | Connection, subscription, book, derived, and log-generation domains are distinct | Delayed events cannot cross the appropriate fences; no wrapping/reused identities. |
| R06 | Allocate a connection-attempt generation before asynchronous establishment work | Stale DNS/TCP/TLS/handshake completions are rejected, including failed attempts. |
| R07 | One accepted logical L2 event, containing its ordered levels, receives one Live cursor | A multi-level message is never silently split into independently accepted level events. |
| R08 | A new book generation includes complete authoritative initialization for its declared scope | Reconstruction from the new generation does not depend on an invalid older book. |
| R09 | Ordinary merger release uses `timestamp < watermark` | Inclusive release requires explicit equality-closure evidence; late-event handling remains necessary. |
| R10 | Physical connectivity, native subscription, integrity, readiness, recovery, historical coverage, and health are independently reported | TUI and control snapshots preserve the dimensions; health does not mutate integrity. |
| R11 | Control and market-data sockets are separate | Length-prefixed versioned JSON management; independently versioned binary market data. |
| R12 | Bootstrap orchestration is separate; Live support contracts and integration fixtures are mandatory | A simulated Bootstrap consumer exercises protection, replay, coverage, recording reads, and handoff. |
| R13 | Static configuration uses `data/.config/live/`; operational state uses `data/.live/` | Resolved paths respect their distinct owners; CLI roots are explicit. |
| R14 | Historical canonical financial semantics remain authoritative | Exact values, timestamps, quantities, grouping, ordering and conversion are compared against existing fixtures. |
| R15 | Diagnostic logs/history remain local; PostgreSQL stores optional market data | No operational-log database dependency is introduced. |
| R16 | Optional metrics export does not create a second metric authority | Disabled exporter binds nothing; exporter failure does not affect native monitoring or data processing. |

These resolutions do not freeze every Rust field layout. For example, grouped depth requires a shared container around existing values, but the current `CanonicalEvent` enum must not be destructively rewritten merely to make a conceptual `LiveEvent` example compile. The physical representation is decided in WP-05/WP-06 using the inspected historical types.

### 3.1 Handling a new conflict

Record the conflicting source passages, the affected owner and consumers, the observable behavior that differs, the proposed decision, and the contract tests that will prove it. The user and GPT approve the decision. The local worker does not adjudicate it. Update this roadmap's implementation decision record and the relevant task packet; modifying Documents 00–16 is not a prerequisite for continuing unrelated work.

A hard semantic conflict cannot be waived by “the documentation has mistakes.” It is resolved locally and explicitly before the affected code is committed.

## 4. The complete-component method

### 4.1 Plan from consumers backward; implement from dependencies forward

Before implementing a component, examine its known consumers: production callers, control/TUI clients, recovery paths, storage/replay, tests, and shutdown. Freeze the smallest interface that satisfies those concrete needs. Then implement the component once against that interface.

This is not speculative generalization. Do not add generic plugin systems or universal context objects for hypothetical future features. Include the approved v1 needs and the explicitly required Bootstrap support boundary.

Each component must leave its package with the following applicable surfaces complete:

| Surface | Required when the component is first implemented |
|---|---|
| Domain behavior | Success path, invalid input, boundary cases, and authoritative ownership |
| Dependencies | Typed constructor inputs and ports; no global service lookup |
| Configuration | Validated effective settings, revision context, supported update boundary |
| Observability | Real counters/timings where required, typed incidents, immutable snapshots, and health inputs |
| Safety | Cancellation, deadlines, stale-generation rejection, bounded resources, overflow policy |
| Operation | Start, stop, restart/replacement where supported, idempotency, failure isolation |
| Persistence/replay | Actual acceptance and commit guarantees, cursor/generation preservation where applicable |
| Verification | Unit tests, port-contract tests, failure tests, consumer witness and measured hot-path cost where applicable |

A package is not accepted with “add metrics later,” “wire shutdown later,” or “make it configurable later” when those are already approved requirements.

### 4.2 Construction-time injection, not parameters added through every function

As a selected implementation pattern, each long-lived owner receives a small typed dependency bundle at construction. A trade processor, for example, may receive its compiled plan, output-admission handle, recovery-request handle, scoped metrics handle, diagnostic emitter, clock, cancellation token and resource budget. The exact Rust signatures are frozen in the contract packages; this paragraph is not a competing API definition.

The owner stores the handles it needs. Its internal pure normalization functions continue to accept only the values needed for normalization. Do not thread a TUI, global runtime object, database client, or metrics registry through every helper.

Metrics are registered before the owner starts; hot-path work uses pre-bound handles rather than allocating labels or looking up registry entries for each event. Runtime updates use an owner-specific command/update boundary. The caller does not acquire mutable access to internal state.

The TUI consumes control-protocol snapshots. Adding a view later should change client presentation code, not processing-function signatures. Adding the real recorder should bind the existing event-sink port in the composition root, not add a database parameter to the normalizer.

### 4.3 Early consumer witnesses

A shared contract is not frozen after reading only its producer. Before acceptance, compile and exercise representative consumers against it. For the event contract, this includes a router destination, retention reader, recorder encoder and derived processor. For snapshots and commands, use a headless client and TUI presentation fixtures. For connection protocols, use contrasting fixture families rather than only one happy-path exchange.

Witnesses are small contract consumers, not temporary alternative business implementations. They validate ownership, field sufficiency, error behavior and serialization. Later real adapters run the same contract suite.

### 4.4 Completion states

| State | Meaning |
|---|---|
| Scaffolded | Files and declarations exist. No behavior is implied. |
| ContractFrozen | Approved interfaces and consumer witnesses pass; unresolved blocking decisions are closed. |
| ComponentAccepted | All assigned behavior works against the frozen ports, including failure paths and observability. |
| IntegrationVerified | The component has been exercised with its real adjacent components. |
| CapabilityVerified | Relevant real protocol/storage/environment behavior has supporting evidence. |
| ReleaseAccepted | System-level acceptance and documented capability scope are approved. |

A component may be accepted against a deterministic transport before a complete daemon exists. It is not thereby production-certified. An empty file, a zero-test run, or a fabricated-success adapter never qualifies as ComponentAccepted.

The runtime composition root, test harness, wiring and task registry intentionally grow throughout the project. Editing those integration points is not a redesign of a completed component.

### 4.5 Reopening an accepted component

Reopening is permitted for a demonstrated defect, a missing approved requirement, a measured performance issue, or an approved contract change. Record the reason and affected consumers first. Add a failing regression case where appropriate, apply the smallest correction, run the affected contract/compatibility matrix, and re-accept explicitly.

Do not preserve a defective abstraction solely to avoid touching old code. Conversely, do not “clean up” completed modules while implementing an unrelated feature.

## 5. Roles and AI-assisted execution

### 5.1 Responsibilities

| Participant | Primary responsibility |
|---|---|
| User | Approves scope and architectural choices, controls the checkout/environment, runs or authorizes commands, accepts integrated results |
| GPT | Reads relevant source, designs/finalizes contracts, decomposes packages, implements or reviews difficult logic, reviews diffs and evidence |
| Qwen through OpenCode | Executes bounded, explicitly specified jobs; extracts source facts; applies mechanical edits; implements well-defined leaf behavior and tests |
| Deterministic tools | Scaffold generation, exact replacements, formatting, compilation, tests, context extraction, scope checks and evidence capture |

Qwen is not assigned “implement MarketForge Live,” “finish the next phase,” or an unresolved cross-module design. GPT is also not exempt from scope, test and evidence requirements.

### 5.2 Work package versus local-model task

A work package covers a coherent component and may require multiple sessions. A local-model task is one small action within it: for example, implement a checked cursor comparison against an exact signature, add specified overflow tests, extract imports and callers from three files, or render an approved snapshot in one TUI widget.

A practical initial local-task budget is one behavior, a few related files, and a small context packet. Prefer roughly 2,000–8,000 input tokens where sufficient; this is a workflow starting point, not a model capability guarantee. Increase scope only after observing reliable results. Start a fresh session before conversation/tool-output growth approaches the model's configured limit. Do not use the entire approximately 65k context as a target.

For the previous local context-overflow problem, the operational remedy is small fresh packets and durable task state—not reliance on automatic compaction retaining every contract.

### 5.3 Delegation policy

| Job | Default execution choice |
|---|---|
| Exact repeated replacements or module declarations | Script, not an LLM |
| Find symbols, callers, feature gates or test names | Bounded read-only Qwen task, with quoted paths and evidence |
| Simple enum/serialization conversion under fixed schemas | Qwen; GPT reviews wire/default semantics |
| Parameterized test cases with approved expected results | Qwen; independent oracle review |
| Scoped counters, snapshot mapping, TUI rendering | Qwen after schemas and expected behavior are fixed |
| Generation fencing, ownership races, atomic cutover | GPT-led implementation/review; local worker only on explicit substeps |
| Cursor restart safety, commit ambiguity, replay/handoff | GPT-led; failure/contract tests mandatory |
| Financial normalization and conversion semantics | Reuse first; GPT reviews any change against historical fixtures |
| Database migration or shared-core extraction | Explicit approval and compatibility review before edits |
| Final acceptance | User plus reviewed evidence; never a self-reported model completion message alone |

An agent's proposed tests are not independent proof of its own implementation. Expected values must come from the approved contract or independently reviewed fixture. Never generate an oracle by calling the function being tested.

### 5.4 Standard execution loop

A task starts with a confirmed repository root, baseline revision/dirty-state inventory, task ID, read set and exact write allowlist. GPT supplies the frozen signatures, required cases, forbidden changes, commands and stop conditions. Qwen executes only that packet. The return contains the actual diff, command lines, exit codes, test counts/skips and unresolved issues. GPT reviews both the code and evidence; the user accepts the task. Only then is the next task dispatched.

Two failed repair attempts on the same underlying issue trigger escalation rather than a longer speculative edit loop. Missing interfaces, a need to edit an unrelated file, conflicting fixtures, or an insufficient context budget also trigger a stop.

The attached `planning/task-template.md` defines the packet format. `planning/first-task.md` is a ready starting packet for a read-only baseline investigation. It does not authorize implementing the daemon.

### 5.5 OpenCode controls

Use the installed OpenCode version's permissions and per-project agents to make the worker's access deliberate. Begin with edits and shell actions requiring approval, no subagent spawning, and no external-directory access. A task-specific reviewed allowlist can reduce repeated approvals later. Do not enable unrestricted shell execution merely because file edits are restricted: shell commands can modify files too.

The supplied `opencode/mf-worker.example.md` is a proposed worker configuration, not a replacement for the user's global configuration or model settings. Verify it with the installed version before adoption. The supplied rules example is short; merge relevant rules into the project's existing instruction hierarchy instead of overwriting an existing `AGENTS.md`.

Permissions and prompts are not an operating-system sandbox. Use a separate worktree/checkout for worker edits, avoid production credentials, and review the full changed-file inventory before integration. Parallel workers must not write the same files or independently change shared contracts.

### 5.6 Durable state instead of chat memory

Maintain a repository-relative `.planning/live/` directory containing the package status ledger, approved contract decisions, task packets, bounded evidence reports and source manifests. Store the current task and next allowed task explicitly. A new chat begins from those artifacts, not a narrative guess about what the previous model completed.

Only the user or integration owner marks a package Accepted. Workers report `ReadyForReview` or `Blocked`. OpenSpec may wrap this process later, but structural OpenSpec validation is not semantic or code validation and is not required for this workflow.

## 6. Scaffolding and selected module layout

### 6.1 One-command creation

The accompanying `scripts/scaffold_live.py` creates the selected Live and TUI module skeleton and populates `mod.rs` declarations. It detects the `marketforge-engine` crate at the supplied root or its `engine/` child. It preserves existing leaf implementations and the current `live::run()` body, appends only missing module declarations/target entries, and backs up modified existing files.

From the full repository root, after placing the implementation kit alongside the checkout:

```bash
python3 /path/to/marketforge-live-implementation-kit/scripts/scaffold_live.py --root . --apply
```

For a dry run, omit `--apply`. Use `--verbose` to list every planned file. Python 3.11 or newer is required by this helper. No external Python packages, Cargo dependency resolution, Git operations, database access, or exchange requests are performed.

The helper refuses ambiguous crate roots, symlink targets, conflicting file/directory modules, nonstandard conflicting declarations, and incompatible existing target definitions. Do not run another writer concurrently. It is non-destructive scaffolding with backups, not a transaction across the filesystem; an interrupted run must be inspected before rerunning.

### 6.2 Ownership layout

The first column below is the selected implementation layout. `foundation`, `ports`, and `routing` are explicit roadmap choices that separate shared operational primitives, contracts, and event admission from production adapters; they are not asserted to exist in the uploaded code.

| Directory | Responsibility |
|---|---|
| `src/live/model/` | Identities, cursors, generations, immutable event grouping, lifecycle, policy/value models |
| `src/live/ports/` | Infrastructure/subsystem boundaries, typed commands/results and compatibility contracts |
| `src/live/foundation/` | Clocks, cancellation, budgets, diagnostics, bounded shutdown and filesystem primitives |
| `src/live/config/` | Configuration schema, compile/validate/diff, revisions, persistence and manager |
| `src/live/exchange/` | Profiles, capabilities, classification, selectors and specialized strategies |
| `src/live/transport/ws/` | WebSocket adapter, pool/admission, reader/writer, heartbeat and supervision |
| `src/live/transport/rest/` | REST adapter, shared scheduling, quota/cooldown/retry and pagination |
| `src/live/registry/` | Source identity, independent demand, logical lifecycle and dependency ownership |
| `src/live/registry/subscriptions/` | Native subscriptions, acknowledgements, restoration and replacement |
| `src/live/ingest/trades/` | Trade identity, deduplication, continuity, reconciliation and ordered acceptance |
| `src/live/ingest/depth/` | Book synchronization, sequence/checksum rules, initialization, coverage and views |
| `src/live/recovery/` | Incident coordination, resource scheduling, cancellation, escalation and coverage |
| `src/live/routing/` | Accepted-event admission, destination policies, acknowledgements and cursor integration |
| `src/live/retention/` | Bounded RAM, protection, spill/manifest/commit, recovery, reads and handoff |
| `src/live/recording/` | PostgreSQL batching/transactions, idempotency, coverage, reads and cleanup |
| `src/live/publish/` | Binary Unix data protocol, sessions, fan-out, backpressure and replay/handoff |
| `src/live/derived/` | Derived registry, merge processing, currency references and conversion |
| `src/live/metrics/` | Pre-bound instrumentation, snapshots, health, diagnostic history and optional export |
| `src/live/control/` | JSON Unix control protocol, authorization, requests, operations, queries and client |
| `src/live/runtime/` | Composition, supervision, startup, reconciliation, shutdown and daemon CLI |
| `src/tui/live/` | Five views, dialogs, widgets, asynchronous client and presentation state |
| `tests/live/` | Contract/component/integration tests and deterministic support implementations |

Shared extraction destinations outside `live/` are created only in the approved reuse packages. Do not populate a second canonical model or a speculative storage framework just because an illustrative tree named one.

### 6.3 Scaffold limitations

The skeleton intentionally does not invent public function signatures, add network/database dependencies, modify canonical models, or implement fake-success services. Empty source files are visibly marked as scaffold-only. The new TUI binary explicitly reports that it is unimplemented and exits unsuccessfully. The old daemon placeholder is preserved until the runtime package replaces it.

The helper registers a feature-gated `live_suite` integration target and a `marketforge-live-tui` binary target requiring `live` and `monitor-tui`. It does not repair pre-existing feature/import problems. Creating these targets does not prove the repository builds, that tests execute, or that any feature is complete.

Public Rust exposure is finalized through deliberate facade/re-export decisions in the contract packages. Scaffolding module declarations are not the external API freeze.

## 7. Contract freeze and downstream-readiness gate

Complete the following decisions before their first production consumer. The purpose is to remove known later reasons to reopen earlier modules. A giant speculative API is not required: write the actual Rust interfaces, small executable witnesses and explicit behavior tables needed by approved consumers.

| Contract | Required decision before acceptance | Witnesses / first blocking consumers |
|---|---|---|
| C01 Identity and metadata | Stable source/derived identity; canonical key normalization; persistent identity mapping; economic metadata provisioning and versioning | Registry, compiler, recording and restart fixtures |
| C02 Generations | Allocation/fencing points; counter exhaustion; process-instance scope versus restart-persistent identities; candidate/current generations | Connection, subscription replacement, recovery and snapshot consumers |
| C03 Event grouping and provenance | Trade versus grouped depth; immutable payload ownership; no-op depth outcomes; initialization boundary; optional native sequence fields; REST/replay/derived provenance without fabricated WebSocket origins | Router, retention codec, recorder encoder, derived consumer |
| C04 Delivery and admission | Batch ownership; accepted versus queued versus committed; per-destination policies; partial fan-out failure; cursor allocation boundary; bounded admission and cancellation | In-memory destination and failure-injecting destination with the same contract |
| C05 Retention and handoff | Inclusive/exclusive cursor ranges; protection ownership/expiry; accepted/committed/durable positions; atomic replay/follow cut; L2 view cursor boundary; restart-safe epoch persistence | Publisher and simulated Bootstrap consumer |
| C06 Recording | Actual schema mapping; event/level identity; transaction boundary; ambiguous commit; contiguous coverage; retry reconciliation; protected reads and cleanup | Disposable PostgreSQL database and historical-compatible reader |
| C07 Configuration | Effective models; compile/diff and revision check ordering; atomic desired-state persistence; no-op changes; accepted/applied/adopted meaning; reconciliation completion objectives | Headless client and failure-injected config store |
| C08 Management | One versioned command/result vocabulary; authorization; persistent mutation versus transient demand; operation correlation, idempotency, recovery commands, snapshot/query optionality | Fake server/client round trip and TUI fixtures |
| C09 Observability | Typed metric registry; owner-specific handles; units, aggregation/reset boundaries and cardinality budgets; state observation schema; health capability rules; redaction | Every owner's constructor plus control/TUI/export consumers |
| C10 Exchange strategies | Protocol classification; topic/ack identity; heartbeat strategies; native sequence/checksum/snapshot/recovery contracts; verified capability status | Contrasting recorded/synthetic protocol fixtures |
| C11 Resource and shutdown | Global versus per-owner budgets; queue count/byte limits; cancellation propagation; task joining; deadline and unfinished-obligation reporting | Deterministic failure and shutdown harness |
| C12 Feature and storage compatibility | Shared extraction boundaries; dependency/TLS/SQL driver choices; optional-feature matrix; config/wire/Parquet schema versions | Live-only, historical-only and combined builds; old fixture readers |

### 7.1 Important representation decisions

**Grouped depth:** preserve `Trade`, `L2LevelUpdate`, `DepthEventBoundary`, `DepthSourceOperation`, and compatible source metadata. Prefer a shared depth group and a Live payload/envelope that reference those existing values. Do not add a second financial model or force old historical event counts to become Live cursor counts. Decide whether a valid zero-change native message produces an accepted logical event; always preserve its required sequence progress independently. Record and test that decision before routing/storage consumers are implemented.

**Provenance:** WebSocket generation fields are applicable to WebSocket origins. A REST repair result, reconstructed retained event, or derived event must not obtain invented transport provenance simply to satisfy a non-optional illustrative struct. Freeze a typed representation of actual origin/provenance and retain original identity on replay.

**Restart safety:** an in-memory attempt counter is not enough if its values appear in records that can outlive the process. Decide the identifying scope (including runtime identity where appropriate) and distinguish it from durable stream/log identity. If safe offset continuation cannot be proved, allocate a new non-reused log namespace before publishing, while reporting any coverage loss truthfully.

**Delivery:** one accepted event can have different destination outcomes. Do not promise a distributed transaction across a socket, PostgreSQL and Parquet. A bounded channel enqueue is not durability. Required-destination failure must be visible and actionable, not hidden behind a no-op sink.

**L2 views:** a new subscriber's initial book image must be tied to a defined accepted-event boundary and generation. A convenient call to `BookStore::bids()` is not a complete snapshot-to-live protocol. Freeze the view/cutover contract before the depth processor is accepted.

### 7.2 Contract record

Each frozen contract record contains: owner, Rust location, producers, consumers, exact signatures/types, ownership and allocation rules, errors, ordering, cancellation, generation/revision context, resource bounds, observation requirements, compatibility/version rules and named executable tests. Include a hash/version in task packets.

Shared identifiers and wire formats are frozen centrally. Individual adapter workers may not introduce alternative aliases such as a second cursor epoch, broaden enum meanings, or use arbitrary JSON in the canonical hot path.

## 8. Sequential implementation map

The default execution order is the order below. Each package is divided into bounded tasks using the packet template; a package row is not a suitable single local-model prompt. Adjacent packages can share an integration checkpoint, but skipped prerequisites must be explicit.

Every package inherits the completion surfaces in Section 4: metrics, snapshots, configuration, cancellation, limits, errors and tests are part of the package, not separate future retrofit phases.

| Order | Package | Primary result |
|---|---|---|
| 00 | Baseline and evidence | Real crate/build/fixture inventory and missing external inputs |
| 01 | Scaffolding and task tooling | Complete selected module tree and repeatable bounded workflow |
| 02 | Shared domain and depth extraction | Live-accessible shared grouping and canonical compatibility |
| 03 | Shared storage, arithmetic and presentation boundaries | Reusable primitives without historical executor dependencies |
| 04 | Feature/dependency foundation | Validated build matrix and explicit dependency ownership |
| 05 | Identity, state and configuration domain | Stable domain types and state semantics |
| 06 | Event, delivery and replay contracts | Consumer-proven event and persistence boundaries |
| 07 | Management, observation and infrastructure contracts | Frozen service ports, wire DTOs and metric/snapshot needs |
| 08 | Foundations and native observability | Real clocks/budgets/diagnostics/metrics/health infrastructure |
| 09 | Configuration system | Compilation, revisions, persistence, diffs and update contracts |
| 10 | Canonical event router | Ordered bounded destination admission and observable failures |
| 11 | Source registry and dependency ownership | Deduplicated sources, independent claims and lifecycle |
| 12 | Exchange profiles and decoding plans | Validated generic protocol/format compilation |
| 13 | WebSocket connection manager | Complete supervised transport lifecycle and heartbeats |
| 14 | REST scheduling and shared quota handling | Recovery-capable request infrastructure |
| 15 | Subscription manager | Ack-safe activation, restoration and staged replacement |
| 16 | Trade processing | Exact normalization, deduplication, continuity and reconciliation |
| 17 | L2 processing | Shared BookStore integration, complete initialization and synchronization |
| 18 | Recovery coordination | Demand-aware recovery, escalation and coverage outcomes |
| 19 | Acquisition integration checkpoint | Real owners composed under deterministic fake exchange IO |
| 20 | Live Retention | RAM/Parquet protection, crash recovery, replay and Bootstrap support |
| 21 | PostgreSQL Recording | Idempotent recording, coverage, recovery, reads and cleanup |
| 22 | Unix market-data publisher | Full binary sessions, fan-out, replay and L2 handoff |
| 23 | Derived streams | Merge ordering, reference acquisition and currency conversion |
| 24 | Production runtime composition | Daemon startup, supervision, reconciliation and shutdown |
| 25 | Control server and shared client | External management with actual runtime owners |
| 26 | Ratatui Live client | Five operational views and management workflows |
| 27 | Optional metrics exporter | Disabled-by-default read-only native-metrics export |
| 28 | Exchange-family certification | Intended 15-family/two-channel capability matrix with evidence |
| 29 | Hardening and release | Failure, performance, stability, deployment and release evidence |

The production runtime is finalized late, but integration is continuous. Use a small test composition root from WP-08 onward and compose each new real component as soon as its direct peers exist. This is not a plan to implement dozens of isolated modules and discover incompatibilities only at WP-24.

## 9. Foundation work packages

### WP-00 — Establish the actual baseline

**Sources:** actual `Cargo.toml`, `src/lib.rs`, feature-gated module roots, existing tests; Documents 01, 15 and 16.

**Build:** no production behavior. Record the actual crate root, versions, lockfile status, branch/dirty-state fingerprint, selected feature results, current test targets and fixture availability. Locate database migrations, instrument metadata and Live TOML files. Classify baseline failures separately from new changes. The initial read-only worker packet is supplied as `planning/first-task.md`.

**Acceptance:** evidence contains actual commands and exit codes; missing inputs and unrelated failures are explicit; the first implementation task has a bounded baseline. Inspect import compatibility, including the existing `process` re-export and historical TUI dependencies, before assuming all-target or live-only builds work. Do not silently repair these in an unrelated package.

**Delegation:** Qwen extracts bounded evidence. GPT reviews any proposed baseline repair. User approves dependency resolution when a lockfile/cache is missing.

### WP-01 — Create the module skeleton and working records

**Prerequisite:** crate identity from WP-00. Full semantic contracts need not be finalized just to create inert files.

Run the supplied scaffolder, preserve existing files, and inspect its plan/backups. Establish `.planning/live/status.md`, the contract index, task packets and evidence directory. Adopt the context-packet helper and a short agent instruction set without overwriting unrelated project instructions.

**Acceptance:** rerunning the scaffold produces no changes; all referenced child module files exist; existing canonical/historical bodies are unchanged; no generated placeholder is marked implemented. Confirm test-target registration through local Cargo metadata, not filenames alone.

**Delegation:** deterministic script. Qwen may inspect the resulting file inventory; it does not invent implementations for every empty file.

### WP-02 — Make shared depth/domain models available safely

**Sources:** `src/canonical/*`, `src/formats/depth/outcome.rs`, `src/book/*`, `src/normalize/*`, `src/validate/*`; Documents 02, 06, 06.1 and 16.

Select the smallest shared home for depth grouping that does not require historical execution. Preserve old imports through re-exports where possible. Reuse exact price/quantity/timestamp and level semantics. Add compatibility fixtures showing one source depth outcome with several levels, initialization versus ordinary changes, zero deletion, unavailable quantities, native metadata and an empty/no-change outcome.

Inspect source identity and economic metadata adapters. Do not replace the existing `Exchange` or instrument identity types with competing public types merely because a document uses another illustrative name. Use explicit mapping/newtypes only where semantic distinctions require them.

**Acceptance:** historical behavior is unchanged; shared APIs are reachable from the selected Live-only feature boundary; financial values and grouped event metadata round-trip through the shared representation. The complete-initialization emission path is established before the Live depth processor is built.

**Delegation:** GPT chooses boundaries and reviews semantics. Qwen can perform explicit moves/re-exports and add reviewed fixtures under an exact allowlist.

### WP-03 — Extract compatible storage, arithmetic and UI primitives

**Sources:** `src/historical/normalize/parquet/*`, merge ordering/materialization/publication primitives, `src/historical/convert/shared/decimal.rs`, conversion planning/reference primitives, and existing TUI files; Document 16.

Extract only the primitives Live actually requires. Leave historical job scheduling, archive processing and executors under historical ownership. Retain compatibility re-exports/adapters rather than mass-renaming old callers. Separate pure merge-key ordering from its historical depth-reader dependency. Preserve the existing wider conversion arithmetic and rounding. Reuse Arrow/Parquet schemas and atomic-publication primitives where compatible, adding Live metadata through a deliberate versioned adapter.

Extract reusable Tokyo Night presentation constants/terminal utilities only when compatible; do not copy entire historical applications into Live. Resolve the feature gates that currently couple existing TUI modules to historical execution.

**Acceptance:** old canonical/storage/conversion fixtures remain readable and numerically equivalent; old import surfaces retained where practical; Live does not call historical executors. Shared components do not import Live runtime internals.

**Delegation:** GPT leads dependency/compatibility design. Mechanical extraction is suitable for narrowly scoped Qwen tasks. This is a deliberately early shared-core change, not a later surprise refactor.

### WP-04 — Finalize dependency and feature wiring

**Sources:** real manifest/lockfile; Documents 01 and 16.

Select one owner for WebSocket supervision and one HTTP transport; choose compatible Tokio/TLS, TOML, logging, test and PostgreSQL dependencies using the actual lockfile/toolchain and primary documentation. Record versions, features, licenses and reasons. `backon`, `governor`, SQLx and other listed candidates are not automatically selected and cannot replace MarketForge quota/commit semantics.

Preserve existing `historical`, `live` and `monitor-tui` entry points. Add only justified internal shared features and optional backend/export features. A useful target is a Live build without historical executors or TUI, a Live+TUI build without historical execution, and independently disabled recording/export startup. Exact feature names are recorded here after manifest review; later jobs cannot invent synonyms.

Fix identified feature/import baseline issues in explicit bounded tasks. Register tests with accurate feature requirements and keep archive/data-dependent targets distinct from deterministic gates.

**Acceptance:** the agreed build/target matrix is executable; dependencies are reproducibly locked; optional features cannot silently enable unwanted services. Do not claim an all-features build proves isolation.

### WP-05 — Implement domain identity, lifecycle and policy types

**Sources:** Document 02; R01–R06 and C01/C02/C07.

Implement source/derived keys and identity mapping, demand tokens/owners, lifecycle dimensions, generation fencing and checked exhaustion, cursor comparison, typed failures, operation identity, revision types and resolved policy values. Preserve semantic identity independently of connection assignment and recording settings. Establish restart-safe identity/epoch storage interfaces and test implementations before code emits persistent cursor identities.

Keep source, connection, subscription, book, recovery, coverage and health states separate. Define authoritative desired/applied/adopted revision scope rather than interchanging names. Incompatible cursor namespaces must not receive a fictitious total order.

**Acceptance:** identity/dedup/property tests, stale-generation and exhaustion tests, serialization fixtures where needed, and model-only consumer compilation. Unresolved identity representations must be settled here, not inside a recorder or TUI task.

**Delegation:** GPT designs; Qwen implements exact value types, conversions and reviewed test cases.

### WP-06 — Freeze and implement event/delivery/replay contracts

**Sources:** Documents 02, 08–11 and the shared depth types; C03–C06.

Implement the immutable Live payload/envelope and grouped-depth container, origin/provenance, event batches and destination results. Decide ownership/copying, exact decimal encoding, acceptance/commit boundaries, range endpoint semantics, protection handles, book-view cutover, and restart-safe cursor allocation. Keep wire/storage DTOs separate from arbitrary Rust memory layout.

Before freezing, implement the small consumer witnesses: a sink with failure injection, a recorder encoding probe, a retention/replay round-trip, a grouped-depth reconstruction and a derived-provenance consumer. Confirm the actual database schema or mark the database-specific mapping gate blocked; this does not authorize inventing a replacement schema.

**Acceptance:** multi-level groups retain one cursor; rejected input and valid no-change outcomes obey the approved rules; replay preserves original identity; failures distinguish accepted/committed/durable; a full initialization reconstructs without an older generation. No later storage package may silently change this event unit.

**Delegation:** GPT owns semantic decisions. Qwen can build fixed DTO mappings and fixture tests after approval.

### WP-07 — Freeze service ports, management and observation surfaces

**Sources:** Documents 01–03, 05/05.1, 12–14; C07–C12.

Implement narrow transport, registry, processing/recovery, configuration, retention/recording, publication and observation contracts. Resolve the exact Control API vocabulary centrally, including recovery and transient-demand operations that are only conceptual in older examples. Freeze request/operation correlation, validation errors, authorization, completion objectives and version behavior.

Define the full v1 snapshot/metric needs of all five TUI views now. Include required lifecycle/demand/readiness/coverage distinctions, unavailable measurements, per-component revisions/generations and resource budgets. Define factory/constructor dependency bundles and shutdown/update command boundaries.

**Acceptance:** a fake management server/client and TUI fixture consumer compile against the schema; every planned owner has a complete constructor contract; real and fake infrastructure share interfaces; no God object or untyped service locator is necessary. The critical producer and consumer contracts are now frozen.

**Delegation:** GPT designs and reviews. Qwen handles bounded schema/codec boilerplate with approved exact fields and tests.

### WP-08 — Implement foundations and real native observability

**Sources:** Documents 01, 07, 12 and 15.

Implement monotonic/wall-clock separation and deterministic time, cancellation, bounded task/deadline primitives, resource accounting, local structured diagnostics/redaction, scoped metric handles, counters/gauges/bounded histograms, aggregation snapshots and health evaluation. Cover known source, connection, subscription, recovery, storage, publication, derived and runtime observations from the frozen model. Health consumes authoritative state; it does not drive canonical validity.

Provide a genuinely disabled optional-measurement mode and explicit missing/stale values. Mandatory integrity decisions and required resource accounting remain active. Fake clocks/storage/transports are test adapters, not production fallback behavior. Establish the reusable fault-injection harness and a small composition test runner.

**Acceptance:** metrics/schema tests, reset/generation/cardinality bounds, stale observation behavior, health capability policies, local-log redaction and overhead baselines. Each subsequent owner must bind real instrumentation and snapshots in its first implementation.

**Delegation:** GPT reviews concurrency, memory and health semantics; Qwen can implement scoped handles and mapping tests. Export HTTP/UI remains separate from collection.

### WP-09 — Implement the Configuration System

**Sources:** Document 03 and Document 13 configuration sections.

Build TOML/schema loading, explicit inheritance, semantic/capability/resource validation, immutable effective plans, source/derived DAG validation, diffs, optimistic revisions, atomic persistence and restart recovery. Configuration consumes the frozen protocol/metadata compiler ports; it does not establish connections. Provide all approved runtime update categories and unsupported-update errors from the start.

Persist accepted contents and revision identity together with the required synchronization/recovery behavior. Publish only committed desired state. Implement history/rollback-as-new-revision and query/diff/status operations. Runtime execution of plans remains the reconciler's responsibility.

**Acceptance:** invalid proposals have no effects; failed persistence has no runtime mutation; concurrent revisions conflict correctly; independent-demand semantics survive persistent changes; only affected resources appear in plans. Test all approved hot settings and the correct next-batch/admission/measurement boundaries.

**Delegation:** Qwen can implement schema parsing and explicit validators. GPT owns persistence, concurrent acceptance and reconciliation contract review.

## 10. Core acquisition work packages

### WP-10 — Implement the canonical event router

**Prerequisites:** event/delivery contracts and observability.

Build per-stream ordered admission, registered destinations, required/best-effort/explicit durability policies, bounded count/byte accounting, immutable batch sharing, result correlation and cursor integration. A destination is an adapter behind the frozen port; the router does not know PostgreSQL transaction or socket details.

Test slow/failed destinations, cancelled admission, partial fan-out, no subscribers, disabled optional sinks and deadline shutdown. Preserve accepted identity even when a downstream guarantee fails. Do not call a dropped required event “success.”

**Acceptance:** complete ordered trace and destination outcomes match the contract; queue sizes remain bounded; failure of one best-effort subscriber does not block unrelated sinks. Control snapshots and router metrics already exist.

**Delegation:** GPT leads admission semantics; Qwen handles fixed accounting helpers and reviewed tests.

### WP-11 — Implement source registry and dependency ownership

**Sources:** Document 05 and Document 02 demand contracts.

Implement semantic source deduplication, stable logical registration, explicit individual demand claims, persistent/transient distinction, lifecycle commands, idle grace/resource release, protected removal and dependency graph ownership. Recovery/subscription/runtime execution is requested through ports, not embedded in the registry. Implement concurrency/idempotency and diagnostics together.

Use atomic coordination for removal versus new demand and protected dependencies. Persistent deactivation must not stop a source still required elsewhere. Restart reconstruction restores persistent requirements, not obsolete transient handles or native resources.

**Acceptance:** concurrent equivalent acquisitions produce one source; releasing one claim preserves others; stale completions cannot reverse newer intent; failed removal preserves claims; topology cycles are rejected. Test with command-port implementations before the real connection manager is bound.

**Delegation:** GPT owns races and lifecycle semantics. Qwen can implement exact indexes, snapshot projections and deterministic cases.

### WP-12 — Implement exchange-profile and format compilation

**Sources:** Documents 03, 04 and 06; existing format/normalization primitives.

Build profile discovery/versioning, capability validation, topic/ack templates, message classification, compiled selectors and typed native-record extraction. Resolve instrument economics before activation; no per-event database/catalog lookup or TOML interpretation. Preserve strings/precise numeric tokens where exact decimal semantics require them.

Inventory all intended profile behaviors before closing generic interfaces. At minimum, exercise contrasting fixtures for range/REST-bridged depth, server snapshot/reset behavior, and checksum-sensitive behavior. These are design coverage categories, not an assertion that current exchange details have been verified. Check applicable primary exchange documentation when implementing each real profile.

Draft or validate the actual 45 configuration files without inventing missing limits/units. Unknown capability semantics remain explicitly blocked for activation. Typed strategy dispatch handles irreducible differences; common engines do not accumulate scattered exchange-name branches.

**Acceptance:** valid profiles compile; unsupported combinations fail before network IO; malformed required values never become zeros; heartbeat/ack/data frames remain distinct; normalization fixtures match shared historical semantics.

**Delegation:** bounded Qwen extraction/fixture/config jobs; GPT reviews protocol and economic interpretation. Real capability certification remains WP-28.

### WP-13 — Implement WebSocket Connection Manager

**Sources:** Document 05.1 in full, with the accepted connection contract.

Implement the real transport adapter and supervised controller together: descriptor/pool compatibility, resource/rate admission, generation allocation before each attempt, DNS/TCP/TLS/WebSocket establishment, bounded reader/writer tasks, frame routing, configured heartbeat strategies, half-open detection, jitter/backoff/cooldowns, outage probing/escalation, idle policy, controlled replacement and deadline shutdown.

The manager is exchange-protocol-aware but market-data-agnostic. It does not normalize canonical data, retry REST requests, own native subscription semantics, or import a TUI. A heartbeat response is not proof of market-data readiness. Never hold a global lock over network IO.

**Acceptance:** deterministic fake IO and local mock-server tests cover every required lifecycle/heartbeat mode, old-attempt completions, buffer limits, simultaneous failure reports and shutdown. Real IO adapters implement the same interface; no transport supervisor is rewritten at integration.

**Delegation:** GPT owns supervision/fencing. Qwen can implement a specified heartbeat strategy or adapter conversion with exact cases.

### WP-14 — Implement REST client and shared request scheduling

**Sources:** Documents 03, 04, 07 and relevant recovery requirements.

Implement request execution, cancellation/deadlines, shared API-pool budgets, multiple quota constraints, endpoint weight functions, usage-header reconciliation, cooldown/Retry-After handling and typed pagination/recovery capabilities. All requests for the same quota—including snapshots, recovery and metadata when applicable—share the authoritative limiter.

Choose retry ownership explicitly. A transport retry and a recovery operation's scheduling policy must not multiply each other into uncontrolled request storms. Do not assume a generic token bucket models an exchange's fixed/sliding/multiple-window rules.

**Acceptance:** deterministic clock tests for admission/cooldowns and cancelled reservations; local mock HTTP tests for ambiguous/transient/terminal outcomes; no protocol/auth/config errors retried indefinitely; accurate telemetry. No live exchange credential is required for normal tests.

**Delegation:** GPT reviews quotas/retries; Qwen implements approved parsing and fixture-based endpoint adapters.

### WP-15 — Implement Subscription Manager

**Sources:** Document 05 subscription, restoration and replacement sections.

Build topic identity, request-ID correlation, capacity reservation, connection assignment, native acknowledgement/rejection semantics, operation deadlines, unsubscribe, idempotent restoration and staged replacement. Unacknowledged writes do not establish an active subscription unless the selected profile's documented activation rule explicitly says so.

Replacement must distinguish preparation/candidate traffic from current authoritative output. A candidate can initialize without contaminating the active processor. Switch authority at a defined boundary, then fence and release old resources; rollback/abort cannot resurrect stale state. This contract must work for trades and L2 before the manager is accepted.

**Acceptance:** demand-aware reconnect restoration, ack reordering/timeouts, obsolete connection/subscription messages, shared connection capacity, replacement races and finite cleanup. No new wire command is invented locally for an internal capability.

**Delegation:** GPT leads replacement and races. Qwen works on exact encoders/correlators/tests.

### WP-16 — Implement trade processing and continuity

**Sources:** Documents 06 and 06.2; historical trade/normalization/identity primitives.

Implement compiled extraction and exact normalization, individual versus aggregate semantics, native identity rules, bounded deduplication, continuity incidents and interruption boundaries, current live buffering during repair, REST/live overlap reconciliation, ordering and accepted-event cursor assignment. Preserve opaque/native IDs and do not infer gaps from numeric IDs without a supported profile rule.

Expose recovery request/result boundaries, readiness, coverage, scoped metrics and diagnostics from the first implementation. Enforce bounded dedup/recovery windows and truthful exhaustion outcomes. A repaired current feed does not imply complete history.

**Acceptance:** known duplicates, legitimate identical-looking trades, ambiguous IDs, reconnect overlap, stale repair result, partial/unrecoverable coverage, malformed numeric values and overflow tests; historical equivalence on compatible fixtures. Publication receives accepted trades through the existing router port.

**Delegation:** GPT owns continuity/reconciliation. Qwen adds approved fixture cases and pure transformations.

### WP-17 — Implement L2 synchronization and processing

**Sources:** Documents 06 and 06.1; shared BookStore/depth grouping.

Implement REST-plus-buffer and WebSocket-snapshot initialization, protocol-specific sequence bridges/resets/checksums, bounded delta buffering, authoritative book generations, atomic mutation, complete initialization, current integrity, declared depth coverage/truncation, consistent read-only views and snapshot-to-live cut boundaries.

Keep canonical book mutations in the shared implementation. Sequence/checksum failure invalidates the relevant book and prevents trusted publication until valid resynchronization. Native normalization must preserve absolute canonical quantities even when the source uses relative changes. A new generation cannot start with a diff that assumes the prior invalid book.

**Acceptance:** all required strategy families have deterministic fixtures; duplicate/out-of-order/ranged updates, checksum mismatch, timeout, buffer exhaustion, stale snapshot and candidate replacement are covered. Group boundaries and one-cursor-per-logical-event survive output serialization/reconstruction.

**Delegation:** GPT leads synchronization and atomicity; Qwen handles explicit strategy cases and test matrices.

### WP-18 — Implement Recovery Coordinator

**Sources:** Document 07 plus trade/L2 recovery ports.

Implement typed incidents, shared-failure dependency coordination, prioritization, bounded concurrency/budgets, retry/escalation, obsolete-generation cancellation, demand-aware continuation, partial outcomes and persistent coverage reporting. Delegate connection restoration, native subscriptions and book mutation to their respective owners.

Persistent `Inactive` does not cancel recovery needed by independent owners. Conversely, absent live demand does not authorize cancelling independent historical/recording/retention obligations. Database/disk outages affect their declared guarantees rather than silently stopping unrelated acquisition.

**Acceptance:** concurrent recovery causes, shared pools, restart, resource exhaustion, cancellation and supersession tests. Every result states which guarantee was restored; it cannot equate current readiness with recovered missed history.

**Delegation:** GPT leads orchestration and failure semantics; Qwen implements bounded reporters, selectors and reviewed fault cases.

### WP-19 — Verify the complete acquisition slice

Compose the real compiler, registry, protocol adapters, connection/subscription controllers, processors, recovery coordinator, router and native observability under deterministic fake WebSocket/REST IO and explicit test destinations. Include at least trade and L2 sources plus shared demand, not just a stream of synthetic canonical values injected after normalization.

Exercise initial startup, an independent recording-style demand, persistent deactivation, a disconnect, obsolete completions, continuity repair, L2 resynchronization and shutdown. Assert exact accepted-event traces, generation/cursor invariants, snapshots, operation completion and resource release.

**Acceptance:** the same production owners run through the real processing path. This checkpoint adds composition and evidence; it is not permission to replace earlier components with a prototype. Any discovered defect follows the explicit reopening procedure.

## 11. Storage, distribution and derived work packages

### WP-20 — Implement Live Retention completely

**Sources:** Document 09; shared Parquet primitives; C04/C05.

Implement bounded per-stream RAM batches with service-wide resource accounting, retention policies, protection leases/reservations, asynchronous spill admission, versioned immutable segments, manifests, synchronization/atomic publication, committed-range recovery, quarantine of incomplete/corrupt artifacts, safe deletion/reclamation and cursor readers across RAM and disk.

Include concurrent capture/read behavior, epoch/generation boundaries, bounded replay/follow handoff, coverage queries, protected L2 initialization and explicit gap/expiry/capacity outcomes. Record separately what is accepted in RAM, committed to a segment and crash-durable under the selected storage guarantee. Memory-first spill is not a lossless power-failure journal.

Validate Bootstrap-facing behavior with simulated consumers now: acquiring independent demand/protection, capturing a cut, replaying while capture continues, changing protection, and following without an unexplained cursor gap. Preserve interfaces needed for historical overlap reconciliation without implementing the Bootstrap orchestrator.

**Acceptance:** real temporary-file Parquet round trips, injected failures before/after file and manifest publication, restart/reclamation races, protected-reader and disk-full tests, grouped-depth reconstruction, bounded memory and replay latency measurements. Define the chosen cursor-checkpoint/epoch recovery mechanism here if not already implemented behind its frozen port; do not alter the cursor identity.

**Delegation:** GPT owns crash/replay/protection semantics. Qwen can implement specific schema/manifest encoders and narrowly specified fault tests.

### WP-21 — Implement PostgreSQL Recording completely

**Sources:** actual database schema/migrations, Document 11 and the frozen event contract.

Map the existing live schema to trade/parent-depth/ordered-level storage. Introduce reviewed migrations only where required; never assume a documentation table name or key is already deployed. Implement optional writer lifecycle, batching/timeouts, prepared serialization, transactions, idempotent retry, ambiguous-commit reconciliation, contiguous commit/coverage tracking, bounded queues, outages and retention-assisted catch-up.

Include historical-compatible reads, native identity overlap, L2 generation initialization, protected readers/cleanup, archive coverage notification boundaries and Bootstrap integration tests. The recorder does not orchestrate archive retrieval or own source identity; recording demand is registered through the existing registry port.

**Acceptance:** disposable PostgreSQL tests for exact numeric/timestamp round trips, parent/level transaction atomicity, duplicate retry, lost commit acknowledgement, out-of-order batches, no fabricated contiguous coverage, recovery after restart/outage, cleanup while protected and explicit missing history. Disabled recording creates no database connection and does not alter unrelated publication.

**Delegation:** GPT leads schema/transactions/coverage. Qwen may implement reviewed column mappings, SQL fixtures and individual deterministic cases. No production database writes during implementation tests.

### WP-22 — Implement Unix market-data publication completely

**Sources:** Document 08; C03–C05.

Implement the versioned binary codec and framing, exact financial encoding, negotiation, bounded frame limits, socket permissions/lifecycle, sessions, subscriber admission and source demand, per-subscriber queues, fan-out, progress, slow-client policy, failure isolation and finite shutdown.

Include cursor-based replay/reconnect and continuous replay-to-live handoff using the actual retention interfaces. Implement L2 snapshot-to-live delivery through the frozen consistent-book-view/cut contract. Handle generations and unavailable ranges explicitly. A socket write does not prove consumer processing and must not be reported as such.

**Acceptance:** local Unix-socket tests with fragmented/coalesced/invalid frames, slow readers, disconnects, duplicate retries, incompatible versions, multi-level groups, protected replay, L2 initialization and handoff races. Readiness/identity and publication counters already appear in snapshots.

**Delegation:** GPT leads protocol/version/security and handoff. Qwen can implement fixed codecs and approved framing test vectors.

### WP-23 — Implement derived streams completely

**Sources:** Document 10; shared merge and conversion primitives.

Implement derived identity and dependency ownership, DAG validation, source/derived reuse, lifecycle and generation replacement, independent output cursors and full provenance. Implement both arrival and adaptive event-time merges with bounded buffers, deterministic complete-key ordering, source progress/idle/failure handling, strict watermark release, late-event policy and source reintroduction.

Implement currency/instrument graph resolution and eligible conversion, shared reference demand, bounded reference state, event-time and latest-observed policies, freshness/availability, approved precision/rounding and source-value preservation. Keep Spot conversion eligibility and other documented scope restrictions explicit. Do not infer that every source market is numerically convertible.

Use the existing router/retention/recording/publication ports for derived output. Full derived instrumentation and capability-aware health inputs are part of this package; no source processor signature changes should be needed.

**Acceptance:** compatible historical/live ordering/conversion fixtures; equal-timestamp late arrival; idle/source failure and reintroduction; reference gap/staleness; inverse path and rounding; DAG/replacement/demand release; independent output identity and storage/replay. A derived failure must not corrupt a healthy source.

**Delegation:** GPT leads ordering and financial semantics. Qwen works on fixed graph operations, metadata projections and reviewed tests.

## 12. Operational integration work packages

### WP-24 — Finalize the production runtime and reconciler

**Sources:** Documents 01, 03, 07 and 13.

Turn the incrementally maintained composition root into the real daemon: CLI/root resolution, immutable construction, selected optional services, local logging, configuration recovery, identity/epoch initialization, startup reconciliation, supervised task ownership, readiness/convergence reporting, service signals and finite shutdown. Replace the existing print-only `run()` entry point with explicit success/failure and cleanup behavior.

Implement dependency-safe updates: persistent intent versus demand, recording toggles at declared cursor boundaries, source/derived changes, approved hot settings and per-resource revision tracking. Reconciliation must be idempotent and stale results cannot reverse a newer accepted revision. Unavailable exchanges do not invalidate persisted configuration.

**Acceptance:** daemon runs headlessly, reconstructs persistent requirements, does not restore transient demand, survives independent worker failures, and reports incomplete obligations on shutdown. Full simulated pipeline uses actual sinks/adapters where locally available and explicit fake exchange IO. With recording/export disabled, the daemon starts without those services.

**Delegation:** GPT leads composition/supervision. Qwen can implement approved CLI projections or configuration plumbing without changing owner APIs.

### WP-25 — Implement Control server and shared Control client

**Sources:** Document 13; schemas/ports frozen in WP-07.

Implement length-prefixed JSON transport, permissions and local authorization, bounded requests/responses/concurrency, version/correlation handling, dispatch to authoritative owners, optimistic mutations, idempotency, asynchronous operation tracking, pagination/query limits and optional bounded operational notifications.

Expose source/derived management, configuration validation/diffs/history/status, recording, retention/Bootstrap support, recovery, metrics and health according to the approved command matrix. Internal capabilities are not automatically external commands. The shared client implements reconnect, timeouts and authoritative refresh without direct runtime access.

**Acceptance:** fake and actual server/client tests cover versions, framing, malformed data, authorization, stale revisions, disconnect after accepted mutation, retries without duplicate side effects, observation freshness, operation completion and slow clients. Management failure must not block established data-plane work. Optional unsupported commands return explicit errors.

**Delegation:** Qwen is useful for bounded dispatch/client mappings after schemas are frozen. GPT reviews mutation, authorization and operation semantics.

### WP-26 — Implement the independent Ratatui Live client

**Sources:** Document 14 and extracted presentation primitives.

Build Dashboard, Streams, Derived Streams, Recording and Settings, with scoped details, health/diagnostic history, filters, forms, semantic diff, revision conflict handling, confirmation dialogs and operation tracking. Include retention inspection and Bootstrap support status without implementing a Bootstrap TUI.

The client is asynchronous, bounded and resilient to disconnects. It refreshes from authoritative snapshots, preserves selection/focus, displays unavailable/stale values truthfully, and restores terminal state on normal and error exit. No UI task owns exchange connections, evaluates book validity, writes configuration files or calls mutable daemon internals.

**Acceptance:** deterministic presentation/client fixtures, resize/focus/dialog tests, stale/conflicting state, failed/retried operations, offline/reconnect behavior and terminal cleanup. Closing the TUI leaves the daemon running. Metrics/state contracts do not require new processing parameters.

**Delegation:** well-specified views, widgets and mappings are suitable local Qwen jobs. GPT reviews command semantics and integration. Work can start against frozen fixtures earlier in an isolated worktree, without changing the main sequential acceptance order.

### WP-27 — Implement optional Prometheus exposition

**Sources:** current Documents 00, 01, 03, 12, 14–16.

Implement only an adapter over existing native metrics: approved stable names/units, bounded label cardinality, cached exposition where selected, loopback default, explicit configuration, request/response bounds and isolated errors. Do not duplicate measurement collection or health logic. Exporter activation/address changes follow the approved configuration policy.

**Acceptance:** disabled means no HTTP listener/export task; enabled exposes the native snapshot consistently; unavailable measurements are not fabricated; bad/slow scrape behavior and exporter failure do not affect market data or local monitoring. Settings UI uses the existing control configuration contract.

**Delegation:** Qwen can implement fixed exposition mappings and tests. GPT reviews cardinality, exposure and failure isolation. No Prometheus/Grafana server installation is part of this package.

### WP-28 — Verify the complete intended exchange matrix

**Sources:** actual profile files, primary exchange documentation, captured fixtures and integration evidence.

The generic design must already accommodate known protocol families; this package completes and certifies all intended profiles rather than discovering their fundamental interface needs for the first time. Work through Binance, Bybit, OKX, Bitget and Gate.io, with each Spot/Linear/Inverse family and each intended trade/L2 channel recorded separately.

For each row record the exact API/profile version, endpoints, symbol resolution, quantity units, native identity, ack/heartbeat, quotas, snapshot/sequence/checksum/reset rules, REST recovery capabilities and authentication prerequisites. Verify recorded-message fixtures first; run bounded, explicitly authorized exchange smoke tests separately.

Bitget inverse depth and any other previously unresolved economics/protocol details remain disabled until verified. Do not synthesize values to make the matrix green. An unavailable capability has a documented disposition; reducing the promised v1 scope requires explicit user approval rather than silently relabeling the intended full matrix complete.

**Acceptance:** no intended capability lacks an implementation/evidence/disposition row; live-only changes do not break other family fixtures; two competing retry or supervision authorities have not emerged. Changes to a frozen interface follow the reopening procedure.

**Delegation:** Qwen can extract bounded protocol facts and fill approved configuration templates; GPT reviews economic/synchronization interpretation. Current external API facts are verified at execution time, not assumed from old drafts.

### WP-29 — Hardening, reproducibility and release

**Sources:** Document 15 and every subsystem's acceptance criteria.

Run the integrated failure matrix: network reset/outage, old generation completions, malformed data, acknowledgement delays, memory pressure, spill/manifest crash points, disk full, database outage/ambiguous commit, protected replay, slow subscriber, concurrent revisions, derived source/reference failure, terminal disconnect and shutdown deadlines.

Establish measured targets for throughput, latency distributions, allocation rate, peak memory, spill/recording backlog, recovery time, query overhead and long-running resource stability on a named machine/workload. Compare instrumentation-enabled/optional-disabled behavior for identical canonical results. Correctness failures are not excused by high throughput.

Prepare service configuration, socket/data permissions, secret handling, startup/shutdown/restart runbooks, validated sample configurations, capability/version inventory and recovery diagnostics. Performance/environment targets are recorded before acceptance; do not invent benchmark claims in advance.

**Acceptance:** relevant historical regressions, deterministic Live tests, actual storage/protocol integration evidence, supported feature builds, soak/fault tests and documented deployment procedures pass. Every skipped/unavailable check and unsupported capability remains visible. Release scope is explicitly approved.

## 13. Dependency and integration rules

### 13.1 Compile-time dependency direction

Shared canonical, numeric, book, instrument, validation and extracted storage/arithmetic primitives do not depend on `live/`. Domain/contract modules do not import concrete networking, SQL, publisher or TUI implementations. Concrete owners depend on their declared ports and narrow shared primitives. The runtime composition root binds them.

A producer does not import its consumer's implementation to publish a metric, request recovery or enqueue an event. Control clients and the TUI consume serialized contracts/read-only client interfaces, not mutable runtime locks.

Traits are justified at replaceable or testable boundaries. Pure financial operations and hot-path state machines need not become dynamically dispatched plugins. Select dispatch/ownership during contract freeze and validate cost where relevant.

### 13.2 Incremental integration checkpoints

| Checkpoint | Real components exercised | Deliberate substitutions |
|---|---|---|
| After WP-08 | Domain, ports, clocks, metrics/health and diagnostic primitives | Deterministic test observations and clocks |
| After WP-11 | Configuration, router, registry and owner observations | Protocol/runtime command ports and event destinations |
| After WP-15 | Protocol compiler, actual connection/subscription controllers and registry | Exchange IO and source processors where not yet implemented |
| WP-19 | Full acquisition/integrity/recovery processing path | Exchange IO and explicit collector sinks |
| After WP-22 | Acquisition plus real retention/recording/publication | Fake exchange IO; disposable database for DB tests |
| After WP-23 | Source and derived pipelines through real output sinks | Deterministic exchange/reference inputs |
| After WP-25 | Headless daemon, actual control server/client and runtime mutation | Fake exchange IO for deterministic suites |
| WP-29 | Complete supported application | Only substitutions declared by each test type |

Fake implementations have the same ports and fail in specified ways. They never stand in for unimplemented required behavior in a production release.

### 13.3 Parallelism policy

Start sequentially with one active writer. The fastest safe overlap is read-only extraction or preparation of the next bounded packet while the current implementation is reviewed. After contracts are frozen, independent fixture cases or TUI presentation against fixed snapshots can run in isolated worktrees.

Do not parallelize edits to the manifest, shared models, wire schemas, module exports, registry ownership logic or the same test fixture. The integration owner serializes those changes. Limit active work to what can be reviewed promptly; generating more unreviewed code is not progress.

## 14. Testing and evidence gates

### 14.1 Per-task checks

Run formatting/checks for the touched scope, exact named tests for the behavior, and relevant contract tests. Record the test target, filter, actual executed count and skipped cases. A broad green command with zero matching tests is a failed verification attempt.

Every significant negative contract needs a case: stale generation, cancellation, invalid input, full queue/budget, timeout and cleanup where applicable. Concurrency and persistence packages require deterministic race/fault injection, not only happy-path unit tests.

Do not modify the expected outcome, weaken validation, add broad allowances, ignore tests or remove assertions solely to make the implementation pass.

### 14.2 Build matrix

The initial observed feature names are `historical`, `live`, and `monitor-tui`. After WP-04, the recorded manifest is authoritative for any additional features. At minimum verify:

| Build | Required meaning |
|---|---|
| Base library without optional application features | Shared models/primitives compile under their declared gates |
| Historical-only library/application targets | Existing processing remains supported |
| Live-only library/daemon | No dependency on historical executors or terminal rendering |
| Live plus `monitor-tui` | Independent management client works without historical execution |
| Historical plus `monitor-tui` | Existing historical interfaces remain supported |
| Approved full feature set | Feature combinations are compatible; not a substitute for isolated checks |
| Recording/export disabled at runtime | No unwanted network/database/listener initialization |

Command patterns, executed from the actual crate root after lockfile and target registration are established:

```bash
cargo check --locked --lib --no-default-features --features live
cargo test --locked --no-default-features --features live --test live_suite
cargo test --locked --no-default-features --features historical --lib
cargo check --locked --no-default-features --features live,monitor-tui --bin marketforge-live-tui
```

These are planned gate commands, not reports that the uploaded baseline passes them. New target names and extra features must match the accepted manifest. For existing nested tests use the explicitly registered names, not guessed filesystem paths. Archive/real-exchange/database/benchmark cases run in their designated environment; offline deterministic cases form the ordinary task gate.

Cargo's documented target/feature rules explain why `required-features` and explicit test targets matter. The official reference was checked while preparing the kit; local version and target discovery are still verified in WP-00/WP-04.

### 14.3 Evidence report

An evidence report includes baseline identity, changed files, package/task/contract version, exact commands and exit codes, executed test names/counts, fixture versions/hashes, relevant output, failures/skips, measured resource/performance results where applicable and a reviewer disposition.

Do not dump unlimited build logs into the local model's context. Save the full local log, return a bounded excerpt and error summary, and retain the exit code before piping output to a clipboard helper. `cpout` is a convenience for transporting evidence, not a validator.

### 14.4 Compatibility and external evidence

Shared-core changes run corresponding historical regression suites. Storage/wire changes run golden serialization and old-format interpretation cases. Exchange-specific behavior uses primary documentation and versioned fixtures; online smoke tests are separate and authorized. Database operations use disposable test infrastructure, not a production recorder database.

The supplied snapshots support source inspection, not a claimed successful build. This document and the scaffolding must not be cited as implementation test evidence.

## 15. Requirement-to-package coverage

Each implementation task references exact relevant headings/symbols rather than loading whole documents. The following table establishes responsibility for all supplied architecture documents; the acceptance checklist of each document must be expanded into named task tests before its owner is accepted.

| Specification | Primary packages | Cross-cutting gates |
|---|---|---|
| 00 — Master Plan | WP-05–WP-29 | Scope, application independence, release capability inventory |
| 01 — Architecture and Module Boundaries | WP-01–WP-08, WP-24 | Dependency direction, ownership, narrow ports, bounded execution |
| 02 — Domain Models and Contracts | WP-02, WP-05–WP-07 | Identity, cursor/generations, delivery, versioning and compatibility |
| 03 — Configuration System | WP-09, WP-12, WP-24–WP-25 | Persistence-before-effects, effective plans, update boundaries |
| 04 — Exchange Protocols and Connectivity | WP-12–WP-18, WP-28 | Every intended family/channel has capability evidence |
| 05 — Stream Registry and Subscriptions | WP-11, WP-15 | Independent demand, replacement, removal races, stable identity |
| 05.1 — Connection Manager | WP-13 | Full lifecycle/heartbeat/backpressure/fencing/shutdown tests |
| 06 — Normalization and Integrity | WP-02, WP-12, WP-16–WP-17 | Shared exact semantics, compiled extraction and malformed data |
| 06.1 — L2 BookStore and Synchronization | WP-02, WP-17 | Independent reconstruction, sequences/checksums, consistent views |
| 06.2 — Trade Processing and Continuity | WP-16, WP-18 | Identity/dedup, repair overlap, honest coverage and ordering |
| 07 — Recovery Coordination | WP-08, WP-18, WP-24, WP-29 | Resource/failure isolation and demand-aware cancellation |
| 08 — Event Distribution and Unix Sockets | WP-06, WP-10, WP-22 | Binary framing, slow-client isolation and replay/L2 handoff |
| 09 — Live Retention and Bootstrap Buffering | WP-06, WP-20 | Protection, crash recovery, Bootstrap contract fixtures |
| 10 — Combined Streams and Currency Conversion | WP-03, WP-23 | Strict watermark boundary, provenance, references and precision |
| 11 — PostgreSQL Recording | WP-06, WP-21 | Schema compatibility, idempotency, contiguous commits and reads |
| 12 — Metrics and Stream Health | WP-07–WP-08, every owner, WP-27 | No late instrumentation retrofit; bounded authoritative observations |
| 13 — Runtime Configuration and Control API | WP-07, WP-09, WP-24–WP-25 | Exact external commands and truthful completion semantics |
| 14 — Ratatui Management Interface | WP-03, WP-07, WP-26 | Five views, asynchronous independent client, terminal cleanup |
| 15 — Testing and Validation | Every package, WP-29 | Deterministic faults, properties, historical regressions and soak |
| 16 — External Libraries and Code Reuse | WP-00, WP-02–WP-04 | Proven reuse, licenses, explicit dependency/version choices |

A package does not inherit permission to skip a requirement merely because this roadmap summarizes it. The task-level coverage ledger records implemented, tested, explicitly deferred, blocked or not-applicable status for each applicable v1 requirement. A deliberate scope reduction requires approval.

## 16. Methods that improve implementation speed

### 16.1 Generate structure; reserve models for decisions

Use the scaffolder for the complete selected tree, deterministic exact-match scripts for repeated replacements, and formatters for formatting. Use GPT for design and difficult logic; use Qwen for bounded execution. Do not spend context tokens on hundreds of `mkdir`, `touch` or `mod` statements that a manifest-driven script can create reproducibly.

### 16.2 Small evidence-backed context packets

Use the supplied `scripts/pack_live_context.py` to assemble an explicit list of files or line ranges, with path names and SHA-256 identity, into one bounded text packet. It refuses oversized output rather than silently truncating contracts. Byte limits are not tokenizer counts; retain headroom for output and tool use.

A packet normally contains the task, its frozen interfaces, the exact implementation file, one relevant fixture/test and only the required specification extracts. Do not send the whole repository and all architecture documents to each local task.

### 16.3 Reuse one fixture set across layers

A small approved trade/depth trace should exercise decoder, normalizer, processor, cursor allocation, routing, serialization, retention, recording and replay. Preserve an independently reviewed expected event trace. This catches contract mismatches earlier and reduces repeated fixture authoring.

Use contrasting edge cases, not only one venue's happy path. Include missing optional data, exact decimals, a multi-level L2 event, a new initialization generation, a duplicate, an interruption and a stale result.

### 16.4 Separate fast feedback from acceptance

Run the smallest relevant test while implementing. Run the package's full contract/negative/compatibility gate before acceptance. Run wider integration at the defined checkpoints. Reuse build caches and avoid unnecessary clean builds, but do not confuse cached success with evidence for changed inputs.

Measure contention when a local model and Rust builds share CPU/RAM/GPU resources. Choose job parallelism from the machine's behavior rather than increasing every tool's concurrency. No throughput improvement is assumed without measurement.

### 16.5 Use real diffs and single-writer integration

Prefer a reviewed patch or a bounded agent edit over manual multi-paragraph replacement. Verify changed-file scope automatically where possible. Keep the integration checkout under one owner; merge isolated workers' results only after contract tests and review. This addresses the earlier failure mode where a model continued a different task or edited a different component.

### 16.6 Track useful speed

Track elapsed time to an accepted task, review time, repair attempts, unexpected-file changes and regression rate. Raw generated lines and a model's “complete” report are not useful productivity metrics. Shrink task packets when repair/review time dominates; broaden only repeatable low-risk jobs.

## 17. First execution session

The first implementation session has a deliberately narrow objective: establish the baseline and create the inert module skeleton. Run or delegate `planning/first-task.md`, review its evidence, then run the scaffold. Do not begin a transport or recorder before the shared extraction and contract packages.

The first architectural coding work is WP-02/WP-03: making the actual existing grouping, storage/arithmetic and presentation primitives usable under the correct feature boundaries. That is the necessary source-grounded foundation for avoiding later rewrites. WP-05–WP-08 then establish the event/management/observation contracts and real instrumentation before owners are implemented.

The default next-action ledger starts as:

| Item | Initial state |
|---|---|
| Documents 00–16 | Reference inputs; no further general editing pass |
| Document 17 | Implementation plan supplied for execution |
| WP-00 baseline | Ready to execute locally |
| WP-01 scaffold | Ready after crate identity confirmation |
| WP-02–WP-04 reuse/feature tasks | Planned; exact edits depend on baseline evidence |
| WP-05–WP-07 contract freeze | Planned; code interfaces are not yet claimed frozen |
| WP-08–WP-29 | Planned; execute in dependency order |
| Production Live acceptance | Not established |

This makes progress explicit without declaring the system ready merely because planning is finished.

## 18. Source and verification notes

Repository facts in this document come from `repomix-output(20261020-140528).xml`, especially `Cargo.toml`, `src/lib.rs`, `src/live/mod.rs`, canonical/book/format files, historical Parquet/merge/conversion primitives and existing TUI sources. Architectural requirements come from `repomix-output(20261020-140515).xml`, Documents 00–16, and the user's explicit execution constraints in this conversation.

The module layout additions, work-package numbering, workflow budgets, packet format and scaffolding helper are selected implementation-methodology proposals in this document. They are not described as pre-existing repository functionality.

For tool mechanics, the official OpenCode Permissions, Agents, Rules and Commands documentation and the official Cargo Targets / `cargo test` references were consulted on 2026-10-20. Local tool versions still control exact supported configuration. No new exchange endpoint or quota facts were established by that consultation.

The repository was inspected from the supplied snapshots. Its Rust code was not compiled or tested in the document-preparation environment, and no database or live-exchange certification was performed. Any validation of the accompanying helper scripts is reported separately and is not evidence of MarketForge runtime correctness.

**Final implementation rule:** complete a component's approved behavior and operational surfaces against tested contracts, then integrate it through those contracts. Build metrics and control observability into every owner now; build the TUI and concrete adapters as consumers later. Reopen accepted components only for an explicit necessary change, never because an already-known requirement was left for another phase.
