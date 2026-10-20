# MarketForge Live — Implementation Status

| Package | Scope | Status | Evidence |
|---|---|---|---|
| WP-00 | Establish the actual baseline | Accepted | WP-00A-baseline.md, WP-00B-feature-isolation.md |
| WP-01 | Create the module skeleton and working records | Superseded | — |
| WP-02 | Make shared depth/domain models available safely | Accepted | WP-02A-depth-event-sharing.md, WP-02B-trade-sharing.md |
| WP-03 | Extract compatible storage, arithmetic and UI primitives | Accepted | WP-03A-DWP-03B-parquet-contract.md, WP-03D-shared-parquet-primitives.md, WP-03E-shared-trade-builder.md |
WP-04	Finalize dependency and feature wiring	Accepted	WP-04D.md — feature/build verification
WP-05	Implement domain identity, lifecycle and policy types	In Progress	WP-04A, WP-04A.1–A.3, WP-04B, WP-04B.1 — identity, validation, lifecycle, demand
WP-06	Freeze and implement event/delivery/replay contracts	In Progress	WP-04C, WP-04C.1, WP-04C.2 — Live event envelope implemented; delivery/replay contracts pending
| WP-07 | Freeze service ports, management and observation surfaces | Pending | — |
| WP-08 | Implement foundations and real native observability | Pending | — |
| WP-09 | Implement the Configuration System | Pending | — |
| WP-10 | Implement the canonical event router | Pending | — |
| WP-11 | Implement source registry and dependency ownership | Pending | — |
| WP-12 | Implement exchange-profile and format compilation | Pending | — |
| WP-13 | Implement WebSocket Connection Manager | Pending | — |
| WP-14 | Implement REST client and shared request scheduling | Pending | — |
| WP-15 | Implement Subscription Manager | Pending | — |
| WP-16 | Implement trade processing and continuity | Pending | — |
| WP-17 | Implement L2 synchronization and processing | Pending | — |
| WP-18 | Implement Recovery Coordinator | Pending | — |
| WP-19 | Verify the complete acquisition slice | Pending | — |
| WP-20 | Implement Live Retention completely | Pending | — |
| WP-21 | Implement PostgreSQL Recording completely | Pending | — |
| WP-22 | Implement Unix market-data publication completely | Pending | — |
| WP-23 | Implement derived streams completely | Pending | — |
| WP-24 | Finalize the production runtime and reconciler | Pending | — |
| WP-25 | Implement Control server and shared Control client | Pending | — |
| WP-26 | Implement the independent Ratatui Live client | Pending | — |
WP-27 — Verify internal metrics, diagnostics, and observability integration (no Prometheus/Grafana).
| WP-28 | Verify the complete intended exchange matrix | Pending | — |
| WP-29 | Hardening, reproducibility and release | Pending | — |

## Current Development Position

Completed:
- Live source identities, cursors, and generation types.
- SourceStreamKey validation.
- Lifecycle, integrity, and demand ownership models.
- LiveEvent and grouped L2 event envelopes.
- Shared historical/Live Parquet schemas and TradeBatchBuilder.
- Cargo feature and build wiring.

Pending:
- Remaining domain/policy types.
- Delivery and replay contracts.
- Service ports and runtime foundations.

## Implementation Workflow

ChatGPT is the primary architect and Rust code author.
The user copies complete code into the repository.
Qwen/OpenCode is used for targeted source extraction,
repository inspection, and mechanical modifications.

No Git commands.
No broad or expensive tests without explicit authorization.
Preserve historical compatibility.

## Next Task

Continue WP-05/WP-06 by identifying the remaining foundational
contracts from the architecture documents, then implement them
directly in Rust.