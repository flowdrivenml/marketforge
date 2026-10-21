# WP-05 — Identity and authoritative metadata contracts

**Status: Partial — Not Accepted.** 2026-10-21. Independently implementable
contracts below are verified. Removed-identity reuse/history scope remains an
unresolved architectural decision; no permanent non-reuse claim is made.
This matrix supersedes the earlier WP-05 reconciliation progress report.

Authority: current WP-05 assignment and its explicit Option A/catalog/classification
instructions; Document 02 §§3–5/8, Document 03 §§7/14.3–14.4, Document 07
restart recovery/§12.4, Document 17 WP-05/C01/C02; DEC-012–018. Accepted decisions
are unchanged. Existing PostgreSQL table definitions/Python catalog models and
Rust `instrument`, `job::InstrumentSpec` and `normalize_quantity` were inspected
as compatibility references, without database access or historical modifications.

## Requirement → code → test

Code paths are under `engine/src/live/model/`; tests are under
`engine/tests/live/`. Test stems below uniquely identify executable tests.

| Requirement / outcome | Implementation | Verification |
|---|---|---|
| Canonical keys, equivalent acquisitions, identity independent of operations — implemented | `identity::SourceStreamKey::canonicalized`; existing equality/hash/types unchanged | `wp05_identity_canonical_dedup_includes_all_acquisition_semantics`: canonical venue spelling, opaque symbols, duplicate normalized keys, depth/interval combinations; `disabled_and_operational_changes_keep_identity_without_runtime_claim_effects` |
| Explicit derivative family and spot consistency — implemented | `SourceStreamKey::validate` now checks authoritative explicit `ContractKind` for both perpetual/future; absent family remains unresolved; spot cannot carry a derivative family | `wp05_metadata_classifies_all_approved_catalog_tuples_and_fences_identity`, `derived_cycles_and_unresolved_families_never_become_proven_mappings`; existing WP-09 spot mismatch regression remains green |
| Restart mapping from committed configuration — implemented within evidenced scope | `persistent_identity::{PersistentIdentityEvidence,PersistentIdentityMap,IdentityHistoryScope}`; no additional persistence | `real_configuration_journal_restart_and_epoch_namespace_compatibility`: real `FileRevisionJournal` reopen, same IDs/definitions, transparent ID/configuration serde compatibility; `real_bounded_journal_exposes_unprovable_earlier_mapping` verifies retained suffix and disabled declaration |
| Duplicate IDs/source keys/source-derived collisions and reassignment — implemented over supplied authoritative history | Bounded reconstruction, canonical key/ID indexes, revision/parent validation; reuses existing pure bounded `config::validation::dependency_order` | `duplicate_ids_and_cross_kind_collisions_are_rejected`, `reassignment_is_detected_across_revisions_and_removal_gaps`, `validated_dependencies_revision_chain_and_bounds` |
| Pre-acceptance contract — implemented without WP-09 enforcement | `validate_candidate_identity`, immutable `IdentityCandidateRequirements` | `candidate_validation_is_pure_and_reuses_the_reconstruction_contract`: rejection before persistence, unchanged base, provisional boundary numbers only, removed/reintroduced declarations, checked revision exhaustion. No revision is reserved or accepted. |
| Derived identity references validated structure/dependencies; semantic generation boundary — implemented at domain level | Existing typed `DerivedDefinition` and DAG validator, `DerivedSemanticBoundary::validate_generation` | `derived_changes_require_fenced_checked_generation_boundaries`: changed transformation requires NewDerivedGeneration, exact current-owner fence, checked next/exhaustion; operational changes need no new semantic generation; cyclic/unknown dependencies reject. Economic approval/execution remains downstream. |
| Removal/reintroduction without invented tombstones — explicitly unresolved | Evidence reports reintroductions and RetainedSuffix; `require_complete_identity_proof` returns ReintroductionPolicy/IncompleteHistory | `reintroduction_and_truncated_history_do_not_invent_tombstones`, real bounded journal witness; consequences below |
| Complete catalog lookup and immutable validated metadata — implemented as model contract | `metadata::{CatalogLookup,MetadataResolver,InMemoryMetadataResolver,ValidatedInstrumentMetadata}` | `wp05_metadata_complete_lookup_prevents_symbol_only_or_family_fallback`: all five tuples sharing a symbol resolve distinct rows; exchange/symbol mismatch rejects; multiple exact rows return Ambiguous. Immutable validated fields have no deserialization bypass. |
| Spot/perpetual/future classification — implemented | Complete `(exchange,symbol,instrument_type,market_category)` mapping; shared `InstrumentIdentity`, `InstrumentKind`, `ContractKind`, `MarketKind` | `classifies_all_approved_catalog_tuples_and_fences_identity`, `rejects_missing_and_conflicting_classifications_without_inference`: missing family, conflicting pairs, unknown/options, explicit identity mismatch; no symbol/exchange inference |
| Exact economics, specifications, limits/assets — implemented | Read-only `CatalogSpecifications` projection uses existing strict `ExactDecimal`; validated result embeds existing `InstrumentSpec`/`Decimal`/`QuantityEncoding` and shared `Currency` | `missing_spec_tick_economics_and_provenance_are_actionable`, `exact_decimals_limits_and_serialization_preserve_all_fields`, `optional_absence_and_zero_limits_remain_explicit`: missing/invalid tick/specs, invalid limits, exact 28-place decimals, no float/exponent/overprecision acceptance, optional fields/zero minima preserved |
| Metadata validity versus normalization economics — implemented separately | `ValidatedInstrumentMetadata::validate` versus `check_normalization` | `normalization_requires_multipliers_only_for_contract_encodings`: Base/Quote need no multiplier; Contracts requires positive value/asset and supported asset interpretation; unsupported conversions reject. Tests consume shared production `normalize_quantity` with the returned `InstrumentSpec`. Native field/channel capability still requires WP-12 proof. |
| Provenance/freshness/version information — implemented where supplied | `CatalogProvenance`, optional raw reference/version, explicit caller-supplied freshness bound | `freshness_and_concurrent_model_only_consumers`, exact-field test: catalog IDs, fetched/updated/raw timestamps, optional version preserved; exact stale/future boundaries and concurrent read-only trait consumers |
| Generation exhaustion/fencing and cursor namespace — existing guarantees reverified | Existing five independent generation types, `StreamCursor::compare_position`, `FileEpochStore` unchanged | 2 `model::generation` tests; 18 `foundation::filesystem` fault/restart tests; new real-journal/epoch witness. A derived generation is never a LogEpoch or DerivedStreamId. |

All new snapshot/identity reconstruction paths have caller-supplied count/byte and
DAG limits, checked counters, and deterministic typed failures. They perform no
runtime owner mutation, demand release, connection management or database I/O.
Canonicalization changes only the venue spelling already defined by `Exchange`;
it does not guess symbol aliases, channel defaults or derivative economics.

## Unresolved architectural decision — removed identity reuse/history scope

The user explicitly confirmed during this assignment that no complete identity
reuse policy exists after permanent removal. Option A remains authoritative:
committed configuration revisions own persistent declarations. No separate
identity database/journal, permanent tombstone, retention extension or reuse
permission was introduced. Accepted decision records were not edited.

Current state and retained history prove their own mappings. Demonstrated ID/key
reassignment, acquisition aliasing and category collisions reject. A known
removed/reintroduced ID is reported for lifecycle resolution even when its key
matches. A suffix cannot prove an identifier was never used before its missing
parent; `require_complete_identity_proof` fails explicitly. This does not prevent
reading the current committed declarations, including disabled declarations.
The real journal test with history=1 proves the loss of earlier evidence on
ordinary commit/reopen; this is not a hypothetical persistence failure.

Consequences still requiring an accepted lifecycle/identity-scope decision:

- Retained events, recorder rows, replay clients and independently owned demand
  can outlive a configuration declaration. Removal does not delete those owners
  or their identity evidence.
- Rebinding an old StreamId to a different key would reinterpret old retained
  events/replay as a different acquisition. A fresh LogEpoch prevents numerical
  cursor namespace reuse under its existing guarantees; it does **not** prove a
  stable StreamId-to-key binding or authorize semantic ID reuse.
- Restart can reconstruct current persistent identities and graph, not obsolete
  runtime handles, readiness, generations or transient demand. When historical
  bindings are unavailable, it cannot claim permanent non-reuse/exact historical
  identity restoration. Consumers must not silently authorize reuse or resolve
  old replay identities through an unproved new binding.
- Ordinary FileEpochStore restart/fault guarantees remain verified. Unsupported
  external journal rollback/sidecar deletion limitations in its existing tests
  are preserved; no stronger storage guarantee is claimed.

These unresolved removal/reintroduction and historical-scope guarantees keep
WP-05 **Partial**, rather than treating unknown history as permission or inventing
permanent tombstones. All independently verifiable work above is implemented.

## Downstream dependencies and completion boundaries

| Owner | Remaining obligation; not implemented/claimed by WP-05 |
|---|---|
| WP-07 | Adopt/bind the minimal model-level `MetadataResolver` contract into approved compiler/infrastructure ports, including controlled read-only PostgreSQL access and adapter/lifecycle choices. No frozen WP-07 interface was silently changed. |
| WP-09 | Enforce candidate identity requirements against committed authoritative history during durable CAS; consume validated full-tuple metadata instead of relying on the historical JSON catalog. Preserve unsupported/unknown outcomes and desired/applied/adopted separation. |
| WP-11 | Operational canonical registration/dedup, transient ID allocation, independent demand and reconstruction from persistent declarations. Disabling/removing a declaration is not a command to release another owner's claim. OPEN-002 remains its allocation decision. |
| WP-12 | Supply real catalog/protocol compilation integration through approved ports; implement/verify the downstream read-only catalog adapter, atomic row/provenance hydration and native normalization/channel capability. No production PostgreSQL adapter, SQL correctness or real-exchange capability is claimed here. |
| WP-23 | Validate/execute derived economics/processing and use checked semantic generation boundaries; no processor/runtime was added. |
| WP-24 | Restore current committed declarations through actual owners, preserve independent claims and reconcile generations/applied/adopted status. No runtime integration was claimed. |

DEC-014/OPEN-001 remains unresolved for **missing** authoritative family inputs;
the user-specified full catalog tuple now has a verified resolution contract.
No missing multiplier/settlement information is invented. Optional provenance
version fields do not claim a catalog-wide transactional snapshot/version unless
the future adapter actually supplies it.

## Verification and checkpoint scope

- Focused integration: `cargo test --locked --offline --no-default-features
  --features live --test live_suite wp05_`: exit 0, **19 passed**.
- Generation/cursor: same flags with `--lib model::generation`: exit 0,
  **2 passed**; `--lib foundation::filesystem`: exit 0, **18 passed**.
- `./scripts/verify-live.sh full`: exit **0**, all five gates pass;
  **137 library / 542 integration passed (2 opt-in ignored) / 1 doctest**.
  Final receipts: `engine/target/live-verification/20261021T125212Z-2/`.
- From `engine/`, each exits **0**:
  `cargo check --locked --offline --no-default-features --features live,historical --lib`;
  `cargo check --locked --offline --no-default-features --features live,monitor-tui --lib`;
  `cargo check --locked --offline --no-default-features --lib`.
  Model-only consumer compilation is exercised by the read-only generic resolver
  witness and native proposal/reconstruction tests, using existing Live features.
  No new feature or dependency was introduced.
- Fixtures: bounded in-memory catalog rows, strict decimal-text JSON, shared
  configuration DTO fixtures, temporary real configuration/epoch journals,
  deterministic channel/market combinations and concurrent read-only consumers.
  No database, credentials, external exchange, clock sleeps or production data.
- Changed `model/identity.rs`, added `model/metadata.rs` and
  `model/persistent_identity.rs`, model exports, two registered integration-test
  modules, this report and STATUS. Existing shared financial/catalog/job models,
  configuration journal and frozen ports remain unchanged. No dependencies or
  lockfile changes. No implementation changes to other work packages.
- Existing unrelated retained-reservation/CSV dead-code warnings are not
  suppressed. Historical behavioral tests are not claimed by compile checks.
- Preserve/exclude the pre-existing blank-line change in
  `engine/src/live/config/validation.rs` and untracked
  `scripts/run-autonomous-codex.sh` from this local checkpoint.

Earlier WP-05 progress receipts are historical: `20261021T025113Z-2` and
`20261021T032801Z-2`. They do not establish closure of the unresolved identity
lifecycle. **Next authorized package: none. Stop after WP-05. No push.**
