# WP-05 reconciliation

Status: Partial. Date: 2026-10-21. No acceptance claimed.

Inspected identity, cursor, lifecycle, demand, generation, acquisition, acceptance,
configuration and operation models, downstream ports, DEC-012–018, roadmap
R01–R06/C01/C02 and existing WP-06/07 evidence. Existing financial, provenance,
registry and routing functionality is preserved.

Implemented checked advancement for all five independent generation domains;
exhaustion returns Limit, never wraps. Exact owner fencing rejects both stale
and speculative future values. Cursor position comparison returns no ordering
across stream or log epoch namespaces. Tests cover every generation domain,
exhaustion, equal/stale/future fences and incompatible cursor namespaces.

Verification: `./scripts/verify-live.sh full`, exit 0, five gates pass;
95 library tests, 363 Live integration tests, 1 doctest. Raw receipts in
`engine/target/live-verification/20261021T025113Z-2/`.
Changed Rust modules: `model/cursor.rs`, `model/generation.rs`.

Remaining ledger: stable persistent source/derived identity mapping and economic
metadata provisioning must be reconciled with C01 and the compiler/registry
consumers. DEC-014/OPEN-001 forbids guessing perpetual market families; OPEN-002
leaves registry demand allocation/reconstruction open. Epoch non-reuse has a
real FileEpochStore and existing restart/fault tests, preserved. This checkpoint
is a verified repair, not a claim that every WP-05 requirement is finished.

Next work: complete independent foundational reconciliation and WP-08 primitives
while keeping localized identity/adapter/database gates explicit.
Checkpoint: commit carrying this report (see Git history).

Reopened through the WP-09 consumer: perpetual/future instruments with an absent
family were incorrectly treated as merely ambiguous even when the requested
market was Spot. Spot is now a definite mismatch; Linear/Inverse ambiguity stays
unresolved under DEC-014. A production candidate-construction regression test
proves rejection without changing canonical catalog or financial semantics.
Final campaign full run: exit 0, 130 library / 376 integration / 1 doctest.
