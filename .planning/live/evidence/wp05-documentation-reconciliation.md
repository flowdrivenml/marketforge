# WP-05 — Authoritative documentation reconciliation

**Completed documentation-only reconciliation, 2026-10-21. WP-05 remains
Partial — Not Accepted, not ReadyForReview.** Implementation checkpoint `63f2526`
is evidence of verified contracts, not authority to invent lifecycle, freshness
or infrastructure policy. No work package implementation was started here.

## Files and sections changed

| File | Sections / reconciliation |
|---|---|
| `docs/live/02 — Domain Models and Contracts.md` | §3.3 full catalog identity/classification, exact economics and conditional normalization; §3.5 actual shared InstrumentIdentity/canonicalization; §§3.6–3.8 distinct IDs, Option A mapping, generation/evidence limits and owners |
| `docs/live/03 — Configuration System.md` | §§7.2–7.3 catalog resolution and persistent identity validation; §§10.2/10.7 pure compilation and cold-path metadata/port boundary; §11.3 collisions; §12.4 retained-history limits; §14.4 source identity versus derived generation; §§16.1/16.3 journal authority and restart reconstruction |
| `docs/live/07 — Recovery Coordination and System Resilience.md` | §§11.2–11.5 persistent state/restart ownership and runtime-instance versus cursor identity; §§12.4–12.5 missing historical bindings, replay/epoch limits and catalog authority rather than Python orchestration |
| `docs/live/13 — Runtime Configuration and Control API.md` | §2.2 operational versus persistent ownership; §4.1 durable acceptance and downstream roles; §4.7 logical identity versus generation; §5.3 removal without reuse permission; §13.4 and §§27/27.6–27.7 catalog-backed conceptual Python provisioning versus verified model contracts |
| `docs/live/17 — Implementation Roadmap.md` | C01; WP-05/07/09/11/12/23/24 scopes clarify existing obligations and open adapter/freshness/lifecycle issues. Package order and existing acceptance paragraphs are preserved. |
| `.planning/live/decisions.md` | DEC-035–038 record approved decisions; DEC-014/OPEN-001 amended only for the approved authority/classification resolution; OPEN-006–011 record six unresolved questions with affected consumers/consequences |
| `.planning/live/AUTONOMOUS_PLAN.md` | Documentation-only task/stopping boundary; WP-05 current evidence/ownership table; stale WP-09 worker/service-pending wording replaced with reuse and actual remaining integration obligations |
| `.planning/live/STATUS.md` | Records this documentation checkpoint, preserves Partial, and marks older authority/adapter-owner wording as superseded |

## Accepted decisions versus evidence and unresolved policy

| Authority | Documented outcome |
|---|---|
| DEC-035 / user Option A | Committed configuration revisions/existing journal own persistent source/derived declarations. Registry reconstructs validated committed IDs. No separate identity database/journal; epoch allocation remains independent. |
| DEC-036 / user catalog decision | Existing PostgreSQL catalog tables remain authoritative without schema changes. Typed read-only cold-path resolution, immutable exact economics/provenance; Python/historical code is a compatibility reference, not Live architectural authority. `instrument_metrics` is not contract economics. |
| DEC-037 / user classification decision | Full `(exchange,symbol,instrument_type,market_category)` lookup and the five approved pairs. Existing Rust financial types; Base/Quote/Contracts; no family inference. Metadata validity differs from native normalization capability; multipliers only when selected semantics require them. |
| DEC-038 / user identity safety | No silent semantic reassignment; reject demonstrated conflicts. Non-reuse across different semantics is the user's preference, not an implemented permanent allocation/tombstone mechanism. |
| Verified WP-05 implementation | `SourceStreamKey::canonicalized`, immutable metadata resolver fixtures, bounded current/retained mapping evidence, pure candidate checks, derived generation boundaries. These do not prove compiler enforcement, SQL implementation or runtime reconstruction. |
| OPEN-006–009 | Non-reuse after pruning; identical-definition reintroduction; UUID/other allocation; retained-event/replay/recorded-row historical binding. Missing history grants no reuse permission. A fresh epoch does not prove a historical semantic binding. |
| OPEN-010 | Activation freshness/reference time/age/refresh/failure/version consistency policy. Preserved timestamps and an explicit age-check function do not decide policy. |
| OPEN-011 | Concrete read-only PostgreSQL adapter package remains unassigned. WP-12 catalog/protocol integration and WP-07 service ports do not settle who implements SQL hydration. |

## Contradictions resolved and downstream implications

- DEC-014/OPEN-001 previously left the authority/family mechanism unspecified.
  The full catalog tuple now resolves that architectural choice; absent/conflicting
  inputs still reject/report unresolved. No accepted rule permits profile/symbol
  guesses to fill missing economics.
- The conceptual SourceStreamKey used `InstrumentId`; Document 02 now names the
  existing shared `InstrumentIdentity`. Operational registry ownership no longer
  reads as a second durable mapping authority. Source/derived declarations remain
  distinct while shared cursor identities cannot collide.
- Recovery's “Python-managed catalog” wording no longer makes Python orchestration
  authoritative. Local metadata snapshots preserve available provenance/versions;
  they do not establish an accepted freshness policy or additional catalog owner.
- The earlier WP-05 implementation report's WP-12 concrete-adapter assignment was
  not an approved architectural decision. Current decisions/roadmap/plan explicitly
  supersede that ownership inference with OPEN-011; the historical implementation
  report/receipts remain intact. Older WP-09 source-choice/family blocker wording
  is historical; actual metadata port/adoption and other implementation gaps remain.
- WP-05 owns domain contracts; WP-07 service ports; WP-09 compilation, identity
  validation and durable acceptance; WP-11 operational registration/transient
  allocation/demand/reconstruction; WP-12 profiles/native normalization and
  catalog/protocol integration; WP-23 derived semantic compilation/execution;
  WP-24 composition/reconciliation. No requirement was moved to claim completion.
- Sequence is unchanged. WP-09 must consume WP-23 semantic results for complete
  derived acceptance, and approved metadata ports/adapter for real catalog use.
  Early independent contract witnesses do not satisfy missing production
  integration. No speculative implementation or package reorder resolves these
  remaining availability dependencies.

The six open questions are explicitly retained, including consequences for cursor
meaning, historical retained/recorded data, replay and restart. No unresolved
issue was converted into an accepted answer to make documentation agree.

## Validation and scope

Inspected the requested WP-05 source modules, configuration compiler/ports/journal,
AGENTS, current plan/decisions/status, WP-05/WP-09 implementation evidence and
relevant architecture sections. Actual WP-09 still uses the shared historical
`InstrumentCatalog`; the new model-level resolver/candidate-identity contract is
not claimed to be integrated there. Journal history is bounded, which supports
retained-suffix evidence rather than permanent non-reuse.

Documentation checks: scoped diff/whitespace, new local links, decision/open-issue
references/uniqueness, unchanged unrelated DEC-001–034 text (except authorized
DEC-014 amendment), unchanged roadmap acceptance paragraphs, source-symbol and
ownership/classification consistency review. The scoped read-only audit exits 0:
9 authorized documentation paths; 33 unrelated existing decision records unchanged;
30 existing roadmap acceptance paragraphs unchanged; 5 new local links and 96
decision/open-issue references valid. Tracked non-documentation diff and runner
content fingerprints match their pre-task baseline. `git diff --check` exits 0.
No Cargo tests/builds or database/exchange requests were run: this task
changes documentation only and claims no new implementation verification.

No Rust, tests, Python, schema, configuration or dependency files were modified.
The pre-existing `engine/src/live/config/validation.rs` blank line and untracked
`scripts/run-autonomous-codex.sh` are preserved/excluded. One local documentation
checkpoint; no push. **Stop after reconciliation; no next package authorized.**
