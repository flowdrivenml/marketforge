# WP-04A.2 — SourceStreamKey Validation

**Phase**: WP-04A — Foundational Live identity and generation types  
**Sub-phase**: WP-04A.2 — SourceStreamKey validation  
**Authoritative spec**: 02 — Domain Models and Contracts.md, Sections 3–5

## Scope

Implement validated construction and consistency checking on `SourceStreamKey` that:

1. Rejects mismatches between `SourceStreamKey.exchange` and `InstrumentIdentity.exchange`.
2. Validates `MarketKind` against `InstrumentKind` / `ContractKind` semantics from the catalog.
3. Explicitly rejects unsupported instrument kinds without inventing mappings.
4. Validates channel-parameter consistency (depth > 0, interval_ms > 0).
5. Reports ambiguities where the catalog metadata cannot disambiguate `MarketKind` (notably perpetuals).

## Design Decisions

### Decision 1: Return-all-issues API, not short-circuiting

**Choice**: `validate(&self) -> ValidationIssues` returns the full set of issues.  
**Rationale**: The caller (configuration compiler) needs to make holistic decisions — some issues may be hard errors, others warnings. Collecting all issues upfront avoids incomplete diagnostics.

### Decision 2: Private `ValidationIssueKind` enum

**Choice**: `ValidationIssueKind` is **not public**; consumers interact through `ValidationIssues.issues: Vec<ValidationIssue>` with pattern matching on `kind`.  
**Rationale**: The enum's exact variants reflect internal validation coverage. Keeping it private allows adding new issue types without breaking downstream code.

### Decision 3: Case-insensitive exchange comparison

**Choice**: `exchange_str.eq_ignore_ascii_case(self.instrument.exchange)`  
**Rationale**: The catalog is loaded from JSON where the exchange string may vary in casing ("bybit" vs "Bybit"). Case-insensitive comparison avoids false positives while still catching genuine mismatches (e.g., "bybit" vs "bitget").

### Decision 4: `new()` is a bare constructor, not a validated factory

**Choice**: `new()` does NOT call `validate()` internally.  
**Rationale**: In-progress construction needs a way to build the struct before all fields are set. Validation is explicit (`validate()`); callers decide whether to enforce or warn.

## Unresolved Ambiguity: Perpetuals and MarketFamily

**Finding**: `InstrumentIdentity.contract_kind` is `Option<ContractKind>` with `#[serde(default)]`. For perpetual instruments this field is always `None`. There is **no way** to determine whether a perpetual is linear or inverse from `InstrumentIdentity` alone.

**Impact**: The validation method reports `MarketKindAmbiguous` for `InstrumentKind::Perpetual`. The caller must resolve this ambiguity via catalog metadata, hardcoded mapping, or explicit configuration. This is a **catalog schema gap**, not a user error.

### Recommendation

The instrument catalog schema should add `contract_kind` to perpetual instruments or introduce a `PerpetualKind { Linear, Inverse }` enum variant. Until then, the configuration compiler is responsible for this disambiguation.

## Implementation

### Modified file

- `engine/src/live/model/identity.rs` — added `validate()` impl block, `ValidationIssues`, `ValidationIssue`, `ValidationIssueKind`.

### Validation rules implemented

| Rule | Condition | Issue Reported |
|------|-----------|----------------|
| Exchange consistency | `SourceStreamKey.exchange` enum → snake_case ≠ `InstrumentIdentity.exchange` | `ExchangeMismatch` |
| MarketKind = Spot | `instrument_kind = Spot` && `market != Spot` | `MarketKindMismatch` |
| MarketKind = Perpetual | `instrument_kind = Perpetual` (contract_kind always None) | `MarketKindAmbiguous` |
| MarketKind = Future(Linear) | `instrument_kind = Future` && `contract_kind = Some(Linear)` && `market != Linear` | `MarketKindMismatch` |
| MarketKind = Future(Inverse) | `instrument_kind = Future` && `contract_kind = Some(Inverse)` && `market != Inverse` | `MarketKindMismatch` |
| Future without contract_kind | `instrument_kind = Future` && `contract_kind = None` | `MarketKindAmbiguous` |
| InstrumentKind = Option | Any `Option` instrument | `UnsupportedInstrumentKind` |
| Channel depth = 0 | `channel.depth = Some(0)` | `InvalidChannelSpec` |
| Channel interval_ms = 0 | `channel.interval_ms = Some(0)` | `InvalidChannelSpec` |

## Verification

### Formatting

```
cargo fmt --check  →  clean (0 warnings)
```

### Build — `live` feature only

```
cargo check --no-default-features --features live  →  0 errors, 0 new warnings
```

### Build — `live, historical` features

```
cargo check --no-default-features --features live,historical  →  0 errors, 0 new warnings
     (12 pre-existing dead-code warnings from formats/trades/csv.rs — unrelated)
```

## Constraint Checklist

| Constraint | Status |
|------------|--------|
| Preserve existing `SourceStreamKey` fields | ✅ No fields added/removed |
| Reject exchange mismatch | ✅ Case-insensitive comparison |
| Validate MarketKind vs instrument_kind | ✅ Spot and Future(contract_kind) |
| Report ambiguity, not invent mappings | ✅ Perpetual reported as Ambiguous |
| Unsupported kinds rejected | ✅ Option → UnsupportedInstrumentKind |
| Channel-parameter consistency | ✅ depth > 0, interval_ms > 0 |
| No new instrument model | ✅ Only reads `instrument_kind` |
| No dependency on WS, registry, runtime | ✅ Stateless pure validation |
| No catalog lookup | ✅ No configuration compiler work |
| Deterministic equality/hash preserved | ✅ Derives unchanged, methods don't affect identity |

## Unresolved Items

1. **Perpetual ambiguity** — the catalog must either add `contract_kind` to perpetuals or introduce a `PerpetualKind` enum. (Catalog-schema gap, not a code issue.)
2. **Exchange → string crosswalk** — the match arms in `validate` must be kept in sync if new exchanges are added to the canonical `Exchange` enum. A `Display` derive on `Exchange` would reduce this risk.
3. **ChannelKind-specific validation** — e.g., `Trades` might require `interval_ms > 0`. Deferred to WP-04B.
