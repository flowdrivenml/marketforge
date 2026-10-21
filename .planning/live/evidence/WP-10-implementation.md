# WP-10 — Recovery-aware fan-out repair and ordered admission primitive

Status: Partial. Date: 2026-10-21. No production router readiness claimed.

Requirements inspected: roadmap WP-10; current routing ports, registry, queues,
acknowledgement tracker, recovery capacity, finalization and their consumers.
Existing implementation is preserved rather than replaced.

Implemented:
- `commit_tracked_with_recovery` now performs real queue commitment. Previously
  every otherwise-valid call aborted prepared obligations and returned rejection.
- Full mapping, registry state and recovery reservations are checked before
  activation. Acknowledgement templates are built before irreversible queue
  writes. Queue outcomes flow through the existing recovery owner.
- Public owned RecoveryPending outcomes preserve failed tracking state and
  reservations even on defensive mapping/count errors. Successful queueing is
  Queued, never Committed or Durable. Partial required failures remain explicit.
- Registry registration, removal and shutdown share the commitment fence, so
  registry mutation cannot interleave between validation and queue commitment.
- Destination registration is bounded (configurable limit; 1024 compatibility
  default) and capacity rejection does not change the registry revision.
- StreamAdmissionOrder has an explicit authoritative starting frontier, one
  active admission per stream/log namespace, contiguous offset checks, RAII
  release on uncommitted drop, independent namespaces and checked exhaustion.
  This primitive does not allocate event identities or infer epoch ordering.

Verification:
Focused `cargo test --locked --offline --no-default-features --features live
--test live_suite recovery_aware` (engine/): exit 0, 5 passed.
Focused `... --test live_suite router_obligations`: exit 0, 52 passed before
adding the registration-bound witness (included in final full verification).
Final `./scripts/verify-live.sh full`: exit 0, all five gates pass, 130 library,
376 integration passed / 2 opt-in ignored, 1 doctest. Raw receipts:
`engine/target/live-verification/20261021T032801Z-2/`.
All existing assertions preserved. Historical execution and Python unchanged.

Remaining acceptance work:
- Production EventRouterPort::admit composition and authoritative runtime owner.
- Bind the order permits, cancellation/deadlines and global admission budgets
  into that entry point; the standalone frontier is not end-to-end ordering.
- Real destination worker/completion correlation, best-effort isolation and
  no-subscriber handling, policy-specific delivery/recovery and shutdown.
- Retain required failed delivery payloads and recovery results through real
  owner lifetime and finalization. Returned ownership alone is not a running
  recovery service.
- Router native metrics/control snapshots and end-to-end concurrency/fault tests.
- Consolidate transitional APIs so production cannot bypass obligation tracking.

Changed: routing router/cursor/exports and router_obligations integration tests.
No package accepted. Checkpoint: commit carrying this report. Next authorized
work remains WP-09/WP-10 completion; WP-11 follows satisfied prerequisites.

Final cross-feature receipts: `engine/target/live-verification/campaign-final-1792553404735277127/results.json`. Both compilation
gates exit 0; the two opt-in test commands each execute and pass one test.
