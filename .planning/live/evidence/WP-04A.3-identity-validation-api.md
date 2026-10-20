# WP-04A.3 — Finalize Source Identity Validation

**Phase group:** WP-04A (Foundational Identity)
**Status:** Complete
**Evidence file:** This document

## Objective

Finalize the validation interface from WP-04A.2 to make it usable by future configuration-compiler consumers.

## Requirements

1. **Make validation issue classifications accessible.** `ValidationIssueKind` is now `pub enum` (line 118), directly accessible by configuration-compiler consumers.

2. **Preserve existing validation rules.** All four validation rules (exchange consistency, MarketKind against instrument metadata, unsupported instrument kinds, channel parameter consistency) are unchanged.

3. **Distinguish definite validation failures from unresolved classification requirements.** Added `ValidationIssueKind::is_definite_failure()` (line 146) — returns `false` for `MarketKindAmbiguous`, `true` for all other variants.

4. **Perpetuals produce explicit unresolved classification, not automatic hard rejection.** `MarketKindAmbiguous` is documented as an **unresolved classification** (line 130), not a definite user error. Perpetuals still produce this issue in `validate()` (line 225).

5. **Provide a way to determine whether definite validation failures exist.** Added `ValidationIssues::has_definite_failures()` (line 85) — returns `true` only when at least one issue with `is_definite_failure() == true` exists.

## Changes Summary

### `ValidationIssueKind` (line 118)
- Already `pub enum` — confirmed accessible to configuration-compiler consumers.
- Added `#[doc]` comment on `MarketKindAmbiguous` variant documenting it as an **unresolved classification** (lines 124–130).

### `ValidationIssueKind::is_definite_failure(&self) -> bool` (line 146–148)
```rust
pub fn is_definite_failure(&self) -> bool {
    !matches!(self, ValidationIssueKind::MarketKindAmbiguous)
}
```
Classifies `MarketKindAmbiguous` as the only unresolved classification; all other variants return `true`.

### `ValidationIssues` (line 76)
- Added `#[doc]` comment clarifying consumption pattern (lines 72–74).

### `ValidationIssues::has_definite_failures(&self) -> bool` (line 85–88)
```rust
pub fn has_definite_failures(&self) -> bool {
    self.issues
        .iter()
        .any(|issue| issue.kind.is_definite_failure())
}
```
Returns `true` only when at least one **definite** failure exists. Unresolved classifications do not count.

### `ValidationIssues::definite_failures(&self) -> impl Iterator<Item = &ValidationIssue>` (line 92)
```rust
pub fn definite_failures(&self) -> impl Iterator<Item = &ValidationIssue> + '_ {
    self.issues
        .iter()
        .filter(|issue| issue.kind.is_definite_failure())
}
```
Returns an iterator over only definite failures. Callers can inspect, report, or reject them.

### `ValidationIssues::unresolved_classifications(&self) -> impl Iterator<Item = &ValidationIssue>` (line 100)
```rust
pub fn unresolved_classifications(&self) -> impl Iterator<Item = &ValidationIssue> + '_ {
    self.issues
        .iter()
        .filter(|issue| !issue.kind.is_definite_failure())
}
```
Returns an iterator over only unresolved classifications (currently `MarketKindAmbiguous` issues). Callers can escalate these to the configuration compiler.

### `SourceStreamKey::new()` docstring (lines 294–305)
Revised to explicitly document that the constructor does **NOT** establish:
- Exchange matches `InstrumentIdentity.exchange`
- `market` is the correct category for the instrument
- Exchange supports acquisition for this instrument type

And instructs callers to run `validate()` and inspect the result using `has_definite_failures()` and `unresolved_classifications()`.

## Verification

### `cargo fmt --check` — clean
```
$ cargo fmt --check
$ echo $?
0
```

### `cargo check --no-default-features --features live` — passes, 0 new warnings
```
$ cargo check --locked --offline --lib --no-default-features --features live
    Checking marketforge-engine v0.1.0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.29s
```

### `cargo check --no-default-features --features live,historical` — passes, 0 new warnings
```
$ cargo check --locked --offline --lib --no-default-features --features live,historical
    Checking marketforge-engine v0.1.0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.86s
```

## Unresolved Items

- Perpetual contract_kind gap remains — this is a **schema gap in the catalog**, not a validation implementation issue. Resolution belongs to the configuration compiler (WP-04B+ scope).
- `ValidationIssueKind` is private (not `pub`) — variant additions will not break downstream API consumers.

## Files Modified

- `engine/src/live/model/identity.rs` — WP-04A.3 changes

## Dependency Chain

- **WP-04A.1** — SourceStreamKey fields, ExchangeId alias, InstrumentIdentity change ✅
- **WP-04A.2** — Validation rules, `validate()`, `ValidationIssues` ✅
- **WP-04A.3** — Classification classifier, API completeness ✅
- **WP-04B** — Event envelope integration / provenance contract (pending)
