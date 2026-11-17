# Bitget historical trade reconciliation

BITGET-T1 ZIP/CSV files expose `trade_id,timestamp,price,side,volume(quote),size(base)`. The existing `normalization.trade_id.field = "trade_id"` mapping was correct: native IDs were retained as exact strings, including IDs above 2^53. Timestamps are epoch milliseconds with observed second precision. Canonical base quantity comes from `size(base)`; canonical quote quantity is price times base quantity. No source fields or timestamps are rewritten by this correction.

The missing profile identity contract caused both strict ambiguity and preference losses. Without a contract, Bitget IDs were not admitted by the legacy identity recognizer. Any intersecting accepted timestamp coverage entered the ambiguous interval branch; `prefer_source=earlier` excluded later-source observations in that interval, including distinct trades at a shared coarse boundary timestamp. Those were charged as unverified observations, not verified duplicates. Increasing the exclusion budget cannot correct that classification.

## Checked identity domain

The spot and linear-perpetual historical profiles now declare `trade_id_scope="ordered_run"` with different domain labels:

- `bitget.spot.spot.public-fills.ordered-run.v1`
- `bitget.linear.perpetual.public-fills.ordered-run.v1`

This is a checked non-reset run over the supplied concrete stream, **not** an assertion of globally unique IDs or instrument-lifetime uniqueness. The label names the contract; it does not infer a reset schedule from dates or filenames. Spot and perpetual data never share an identity domain.

The official [spot market history reference](https://www.bitget.com/legacy-docs/classic/spot/market/Get-Market-Trades) describes native IDs, ID-based pagination and second-precision transaction timestamps. The official [contract market history reference](https://www.bitget.com/docs/catalog/classic-contract-market/classic-contract-market) describes descending trade IDs and symbol-specific historical transactions. The [download-center reference](https://www.bitget.com/legacy-docs/uta/data-download-center) associates archived trades with public fills. These establish the use of native transaction keys; they do not document an unlimited reset lifetime. Runtime validation and corpus observations are therefore kept separate from a lifetime guarantee.

Every ordered-run job must satisfy all of these checks before publication:

1. Every observation has a nonempty decimal native ID; no floating-point conversion or fixed-width requirement is used.
2. Equal identities are compared using the same complete canonical equivalence implementation as other reliable domains. Equal-time distinct IDs remain distinct. Same-ID conflicting contents fail even under source preference; they cannot be made consistent by losing observations.
3. After verified duplicate suppression, each source's native order must have strictly increasing IDs. Repeated verified observations can be suppressed; unproven decreases/resets fail.
4. The entire unique chronological union must have strictly increasing IDs, checked before timestamp discard can remove evidence. Equal timestamps use exact numeric ID ordering. This detects cross-source resets/order conflicts even when their ID ranges do not intersect. All participating sources must have compatible concrete identity and domain metadata.
5. Only after these checks is `source.native_identity_order_validated=true` recorded. The normalizer does not publish a partially validated run.

The implementation reuses bounded external identity/native/chronological sorts and scratch spools. It retains only the preceding leased record for order checks, with its memory reservation. It supports partial, contained, chained, equal-start, multi-source and nonadjacent overlaps. Native ordering is not inferred from filenames or task priority. Existing coverage starts and stable source identities choose deterministic duplicate ownership; proven distinct observations are unioned before considering source preference.

Missing IDs, resets, incompatible domain declarations and conflicting identities are correctness failures, not opportunities for lossy preference fallback. A future archive with incompatible native semantics must fail or supply an independently justified identity-domain contract. A validated run does not establish uniqueness across separately normalized jobs or an unobserved reset. The `ordered_run` domain must not be promoted to `global` merely because the present files pass.

## Real corpus evidence

The focused 2026-11-17 investigation streamed all registered Bitget trade archives in the migrated batch, keeping per-file summaries rather than an in-memory trade inventory:

| Feed | Archives | Trades | Equal-time boundaries | Missing IDs / ID decreases / repeated IDs / timestamp regressions |
|---|---:|---:|---:|---:|
| BTCUSDT linear perpetual | 42 | 4,037,370 | 37 | 0 / 0 / 0 / 0 |
| BTCUSDT spot | 12 | 941,695 | 7 | 0 / 0 / 0 / 0 |

All ID ranges are disjoint and increasing in actual chronological coverage order. Therefore the supplied corpus contains no repeated native identities, including nonadjacent sources. These observations establish properties of these inputs, not all Bitget history.

For the first perpetual boundary, the last ID is `1498674744255070208` and the next ID is `1498674744255070210`; both timestamps are `1793037823000`. There are 42 observations at that timestamp across the two files, all distinct. The reported 1,001-event limit failure occurred later: task 21 begins with 1,322 observations at the prior archive's ending timestamp, and its first native ID is greater than the previous last ID. The old preference branch excluded these legitimate continuations. Across the complete scanned corpus, its interval-ownership rule would unnecessarily exclude 2,711 perpetual observations and 37 spot observations. These totals were measured by the full audit, not extrapolated from the failure threshold.

The historical failing hash `c267594601432a594fc8057ce4786c9f00d55a0bfb3996927c310933693aceb7` was resolved through the production runtime registry to `bitget.linear.perpetual.BTCUSDT.trades`. Profile fingerprint changes intentionally change the runtime stream hash; the canonical stream ID and instrument identity remain unchanged.

## Diagnostics and accounting

Trade reconciliation failures attach bounded context to the existing task report: canonical stream ID/hash, exchange/instrument, policy/limits, up to four source paths/members and coverage/overlap intervals, identity contracts and validation status, duplicate/known/unverified totals and up to two decision samples per sampled source. Strings are capped at 512 characters. Sample truncation and total source count are explicit. Native identity violations identify current and preceding source indexes, resolved to representative physical sources in the context.

The wrapper preserves the primary typed cause, resource classification and cleanup behavior. Retained admission includes a bounded allowance for contextual errors and their reporting copies; configured budgets are unchanged. There is no new reporting subsystem or per-event diagnostic inventory.

A successful strict job reports zero intentional/unverified exclusions when none occurred; verified duplicates are separate from loss. Manifests retain the actual resolved domain and validated-run status. Completeness remains relative to supplied observations, never a claim of complete exchange history. See the completion artifact report for executed tests and focused real-data validation results.
