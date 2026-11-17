# Historical normalization contracts

Runtime settings come from `data/.config/historical/general.toml` through `shared::historical`. Relative runtime and prepared-batch paths resolve against an explicit project root without changing process CWD.

The production loader is `historical::config::{read_prepared_batch, load_normalization_batch}`. Prepared schema 4 supports concrete tasks and top-level `families` with minimal `family_id` tasks. Structural reading validates references without opening archives; preparation discovers selected concrete futures from supported source records/member names. See [source selection](normalization-source-selection.md) for the authoritative wire contract, exact integer IDs, namespace compatibility and inherited specifications. No network/database lookup is needed.

Preparation and normalization run off the TUI rendering thread. Discovery emits bounded/coalesced task registration, phases and decoded-byte telemetry before execution; registration survives completion-event coalescing. Physical acquisition identity is retained while members have independent reconstruction state.

## Self-contained profiles

Only `profiles/{trades,depth}/**/*.toml` are discovered, deterministically. Schema-2 profile filenames equal `exchange.instrument_type.market_category.data_type`. Profiles contain source, fields, and normalization mappings. No external physical-schema file is required. Existing trade/depth compilers validate generated specs; fingerprints include effective definitions and source interpretation and ignore locations/comments. Unsupported profiles remain inventory-only.

`feeds.snapshot` can provide explicit source, fields and normalization for a Gate.io snapshot representation. Current futures snapshots are gzip JSONL with object levels `{p,s}`; incremental Gate.io rows remain CSV. XLSX support is constrained to the implemented one-sheet, header, timestamp/asks/bids layout with seconds and ascending stable timestamp sorting. Source cadence, observed precision and book depth/frequency facts are descriptive, not truncation controls.

Legacy inline/profile job loading remains a separate compatibility boundary. The CLI accepts prepared batches and legacy job/directory modes using the version1 response envelope. Canonical row schemas remain unchanged; strict outputs use manifest 3/4 and activated processing policies use manifest 5. The old Forge producer must emit the offline authority and family bindings; Rust never queries PostgreSQL.

## Integrity observations

Workers record physical parsing attempts even when later normalization fails. Shared processors expose allocation-free completed-check traces; failing categories replace success within the same validation unit. Quantity transformation observations count normalization units, never canonical rows. CSV snapshot rows have physical parse/scalar validation units, plus a separate assembled-event validation/transformation unit. No-change depth events retain applicable observations. Continuity observations apply only to initialized sequenced incremental events; current book-level validation is InvalidRecord, and no unrelated InvalidBook successes are invented.

Trustworthy event time survives later failures. Scoped metrics include additive `unattributed` category counts (defaulted when reading old reports). Global/file checks retain those observations. A configured hourly/daily policy fails explicitly with `unattributed_window` when relevant eligible observations lack event time, rather than claiming complete window verification. UTC epoch boundaries and existing fail/degrade semantics remain unchanged.

## Source failures and ordering

Normalization uses full-consumption source readers that validate compressed completion, including TAR/GZIP trailers and concatenated GZIP members. Prefix inspection is explicit and does not claim full-container validation. JSONL distinguishes complete malformed records from underlying I/O errors; only complete record errors can enter degradation policy. CSV I/O, corrupt members/workbooks and compression failures are fatal even with policy disabled. Existing MissingSource/CorruptSource result codes now preserve typed source distinctions.

Timestamp processing is independent of integrity acceptance. Raw adjacent-event regressions are observed before reconciliation. Logical processing uses the native-domain high-water timestamp and checked nanosecond arithmetic; equal timestamps are valid. Validated snapshot sequence resets start a new domain. Strict rejects unresolved regressions even with positive tolerance; permissive policies require explicit bounds and compatible integrity settings. Stateful depth is never sorted. See the [processing configuration reference](../content/configuration/normalization.md#processing-policies) for all fields, native-order output and loss semantics.

Prepared schema 4 retires external source bounds and requires explicit family selection. See [source reconciliation and selection](normalization-source-selection.md) for accepted fields, compatibility, observations and Python handoff.

## Family and strict reconciliation extension

Prepared schema 4 now accepts referenced dated-futures families and bounded multi-member discovery/routing; see [source selection](normalization-source-selection.md). Exact catalog IDs remain authoritative, missing concrete IDs use versioned deterministic allocation, and committed manifests carry the resolved catalog and inheritance assertions. This does not recover arbitrary removed legacy IDs.

Reliable trade identities preserve distinct executions, deduplicate matching complete contents and reject strict conflicts. Native decimal IDs are compared exactly without fixed-width or floating-point conversion. Bitget historical spot and perpetual profiles use separate checked `ordered_run` domains; all participating native source orders and the chronological union must validate without resets. See [Bitget reconciliation](bitget-trade-reconciliation.md); no lifetime/global uniqueness is asserted.

Depth replay establishes exact book equivalence at initialization/transitions and maintains it through controlled changed-level validation. Fragmented ownership uses sealed forward partitions and exact checkpoints; metadata-only accounting avoids decoding snapshot levels again. See [execution paths and resource ownership](normalization-performance-diagnostics.md). Verified identical terminal/initial snapshots can be represented once even when the terminal snapshot originally changed canonical levels. A delta and snapshot sharing an ID remain distinct. Overlap snapshot recovery preserves all supplied unique observations as independently initialized histories; it records unproven continuity. Preference exclusions require explicit bounded loss authorization.

Task reports retain structured typed failures, stage, physical source/member, concrete identity and resource details. A task stopped because another failed is distinguished from an input failure. Staged rows remain separately observable; failed jobs report zero committed rows and all unpublished survivors. Only a successfully published manifest establishes a completed dataset.
