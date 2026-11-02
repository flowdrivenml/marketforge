
# MarketForge — Code Organization and Testing Conventions

## 1. General Principles

These conventions apply to the entire MarketForge project, including
Rust, Python, and future components.

The objective is to maintain a codebase that is:

- Intuitive to navigate.
- Organized around clear responsibilities.
- Modular without unnecessary fragmentation.
- Easy to extend and maintain.
- Consistent across subsystems.
- Free from obsolete or unused code.
- Easy to test without understanding unrelated components.

Prefer structural clarity over arbitrary uniformity.

Do not reorganize working code merely for aesthetic consistency.

---

## 2. Module Organization

### 2.1 Organize by responsibility

Group modules according to their responsibilities rather than
their implementation details.

Example:

    subsystem/
    ├── mod.rs
    ├── manager/
    ├── routing/
    ├── subscriptions/
    ├── transport/
    └── lifecycle/

Each directory should represent a meaningful subsystem.

Avoid directories such as:

    misc/
    stuff/
    helpers/
    common/

unless they represent a genuinely cohesive responsibility.

### 2.2 Avoid redundant filenames

A filename should not unnecessarily repeat information already
provided by its parent directory.

Avoid:

    subscriptions/
    ├── subscription_pending.rs
    ├── subscription_resolver.rs
    └── subscription_protocol.rs

Prefer:

    subscriptions/
    ├── pending.rs
    ├── resolver.rs
    └── protocol.rs

The complete module path provides the necessary context.

However, preserve descriptive names when shortening them
would introduce ambiguity.

### 2.3 Filesystem and module hierarchy should agree

Physical directory organization should reflect the language's
logical module hierarchy whenever practical.

In Rust, prefer genuine nested modules over a large collection
of flat modules connected through #[path] attributes.

In Python, use equivalent package and subpackage organization.

Avoid maintaining two different organizational structures:
one in the filesystem and another in the language's module system.

### 2.4 Avoid excessive fragmentation

Do not create separate modules for trivial functions merely
to reduce file size.

A module should represent a cohesive responsibility.

Split a module when:

- It contains several distinct responsibilities.
- Its internal components have meaningful ownership boundaries.
- Navigation becomes difficult.
- Separating components improves comprehension.

Do not split modules solely because they exceed an arbitrary
number of lines.

### 2.5 Preserve clear public interfaces

Internal organization should not unnecessarily expose implementation
details to other components.

Prefer a clear public interface at the subsystem boundary.

In Rust, use explicit re-exports where appropriate.

In Python, expose intended package interfaces through clearly
defined modules or package exports.

Internal refactoring should preserve established public interfaces
unless an API change is explicitly authorized.

Use the narrowest appropriate visibility.

---

## 3. Naming Conventions

### 3.1 Names should describe responsibilities

Prefer names such as:

    manager
    state
    resolver
    compiler
    decoder
    dispatcher
    controller
    protocol
    lifecycle
    validation

Use these names according to their actual responsibilities.

Avoid vague or misleading terminology.

### 3.2 Context belongs in the module path

Prefer:

    normalization/trades/decoder.rs

Over:

    normalization/trades/trade_normalization_decoder.rs

The filename should describe its role within the containing module.

### 3.3 Public type names may remain descriptive

Short filenames do not require shortening public type names.

For example:

    subscriptions/pending.rs

may define:

    SubscriptionPendingManager

The module path provides context during navigation, while the
public type remains recognizable when imported elsewhere.

### 3.4 Use consistent terminology

The same architectural concept should use the same terminology
throughout the project.

Do not alternate between:

    manager
    coordinator
    controller
    supervisor

unless those terms represent genuinely different responsibilities.

Naming should communicate ownership and behavior.

---

## 4. Unit Test Organization

### 4.1 Keep unit tests with their implementations

Unit tests should generally reside directly inside the source file
containing the implementation they test.

For Rust:

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn rejects_invalid_input() {
            // Test implementation.
        }
    }

Prefer:

    subscriptions/
    ├── pending.rs
    ├── protocol.rs
    └── resolver.rs

Where each implementation file contains its own unit tests.

Avoid creating directories containing only tests.rs unless
there is a substantial maintainability reason.

### 4.2 Inline tests are the default, not an absolute rule

For small and medium test suites, prefer inline tests.

For exceptionally large suites, a separate module-local
test file is acceptable when it materially improves readability.

The decision should depend on maintainability rather than
an arbitrary line-count threshold.

### 4.3 Unit tests should test local behavior

Examples include:

- Parsing and validation.
- Individual calculations.
- Local state transitions.
- Data structures.
- Error handling.
- Boundary conditions.
- Local compiler and decoder behavior.
- Isolated component invariants.

Unit tests should not require unrelated components merely
to exercise the behavior under test.

### 4.4 Preserve test isolation

Tests should not depend on execution order.

Avoid shared mutable global state unless specifically testing
concurrent access.

Each test should establish its own prerequisites and clean up
resources it owns.

Tests must be deterministic wherever possible.

---

## 5. Integration Test Organization

Integration tests should remain separate from implementation-local
unit tests.

They verify interactions between multiple components and
established public interfaces.

Example:

    tests/
    ├── integration/
    │   ├── subsystem_a/
    │   ├── subsystem_b/
    │   └── workflows/
    ├── contracts/
    ├── network/
    └── support/

Not every component needs all these directories.

Create only directories justified by actual tests.

### 5.1 Integration tests verify boundaries

Examples:

- Several components working together.
- Public API compatibility.
- Complete subsystem workflows.
- Lifecycle coordination.
- Recovery behavior.
- Cross-component failure propagation.
- Persistence and restart behavior.

A test may be asynchronous without being an integration test.

A test may also be entirely offline while still being an
integration test.

Classify tests by what they verify, not by their implementation
language or execution environment.

### 5.2 Contract tests verify extensibility

When a system supports multiple implementations of the same
interface, organize their contract tests consistently.

For example:

    tests/
    └── contracts/
        ├── shared/
        └── cases/
            ├── implementation_a/
            ├── implementation_b/
            └── implementation_c/

Shared contracts define expected behavior.

Implementation-specific cases provide inputs, configuration,
fixtures, and independently defined expected outputs.

Do not calculate expected results using the same production
implementation being tested.

### 5.3 Network tests should be explicit

Tests requiring external services should be distinguishable from
ordinary offline tests.

Examples:

- Public API connectivity.
- Exchange WebSocket connections.
- External database services.
- Third-party network dependencies.

Real-network tests should not run accidentally during routine
offline verification.

Prefer explicit opt-in execution.

Local mock servers may be used in offline integration tests.

---

## 6. Test Naming

Test names should describe observable behavior.

Prefer:

    rejects_reused_request_ids
    preserves_message_order
    prevents_stale_generation_updates
    rejects_invalid_configuration

Avoid:

    test_1
    test_manager
    check_function
    w43_test_case

Do not repeat the entire module path in every test name.

The module hierarchy already provides context.

Test names should clearly indicate:

- The condition being exercised.
- The expected behavior or invariant.

Preserve meaningful nested test groups where they improve
organization.

---

## 7. Test Fixtures and Helpers

### 7.1 Keep helpers close to their consumers

Helpers used by one unit-test module should remain inside
that module.

Helpers shared by multiple integration suites may live
in a dedicated test-support package or directory.

Do not create general-purpose helper modules prematurely.

### 7.2 Keep expected results independent

Expected outputs must not be generated by the same production
logic whose correctness the test is intended to verify.

Use independently specified expected values.

### 7.3 Distinguish fixture provenance

Explicitly distinguish:

- Captured real-world data.
- Synthetic test data.
- Mocked metadata.
- Generated fixtures.

Do not silently replace missing authoritative fixtures with
invented values.

Small, stable fixtures should be version-controlled when
appropriate.

Bulk captures, caches, and generated artifacts should remain
outside ordinary source directories.

### 7.4 Missing coverage must be visible

Distinguish between:

- Supported behavior that passes.
- Unsupported behavior with an expected rejection.
- Behavior that has not been tested.

Missing test cases must not silently count as passing tests.

---

## 8. Dead Code Policy

Remove genuinely obsolete code.

Candidates include:

- Unreferenced files.
- Superseded private implementations.
- Unused private functions and wrappers.
- Unused imports.
- Obsolete test helpers.
- Unreachable or unconstructable private functionality.

However, distinguish dead code from intentionally retained
integration boundaries.

Do not remove a supported public interface merely because
its future caller has not yet been implemented.

### 8.1 Ownership matters

An apparently unread field may still serve an important purpose.

Examples include:

- Retaining a channel sender.
- Keeping a task handle alive.
- Holding a synchronization guard.
- Owning a resource permit.
- Triggering cleanup through destruction.

Never remove ownership-bearing state based solely on
compiler warnings.

### 8.2 Do not hide warnings

Avoid suppressing warnings through:

- Blanket allow attributes.
- Artificial references.
- Unnecessary visibility expansion.
- Renaming variables merely to silence diagnostics.

Fix the underlying problem where appropriate.

---

## 9. Refactoring Rules

Structural refactoring should preserve behavior unless
behavioral changes are explicitly requested.

Allowed structural operations include:

- Moving files.
- Renaming modules.
- Introducing meaningful module boundaries.
- Updating imports.
- Updating module declarations.
- Adjusting narrow visibility boundaries.
- Updating test registrations.
- Relocating unit tests.
- Removing confirmed dead code.

Do not use structural cleanup as an opportunity to:

- Rewrite algorithms.
- Redesign state machines.
- Introduce new runtime behavior.
- Modify persistence semantics.
- Change error contracts.
- Expand public interfaces unnecessarily.
- Introduce unrelated dependencies.

Prefer incremental changes over repository-wide rewrites.

---

## 10. Build Configuration and Registered Tests

Whenever modules or test files move, inspect the relevant
build-system configuration.

For Rust, this includes Cargo.toml and any explicit
[[test]] target declarations.

For Python, this may include pyproject.toml, package metadata,
test discovery configuration, and entry points.

Update paths and registrations as required.

Do not leave obsolete test targets pointing to removed files.

Do not accidentally register the same test suite twice.

Prefer meaningful test targets over excessive fragmentation.

Preserve test isolation when consolidating previously separate
test processes.

---

## 11. Verification Requirements

Before significant refactoring:

1. Establish a clean Git baseline.
2. Record the current commit.
3. Save a recoverable checkpoint.
4. Record existing tests and their identities.
5. Confirm baseline compilation and test results.

After refactoring:

1. Compile production code.
2. Compile test code.
3. Run applicable unit tests.
4. Run offline integration tests.
5. Verify public API compatibility.
6. Check formatting.
7. Check compiler warnings.
8. Compare test discovery before and after.
9. Inspect the actual Git diff.

### 11.1 Prevent disappearing tests

A successful test command is not sufficient if tests were
accidentally removed from discovery.

Compare:

- Test counts.
- Fully qualified test names.
- Ignored-test status.
- Integration target registrations.
- Scenario coverage.

For purely mechanical test relocation, test identities should
remain unchanged whenever possible.

### 11.2 Do not conceal failures

Never treat the following as successful verification:

- A test command executing zero unexpected tests.
- Removing failing assertions.
- Silently skipping unavailable fixtures.
- Repeatedly rerunning flaky tests until one passes.
- Disabling failing tests without authorization.
- Ignoring compilation errors in unrelated test targets.

Report incomplete verification explicitly.

---

## 12. Git Workflow

Before substantial refactoring:

- Verify the working tree is clean.
- Commit unrelated completed work separately.
- Create a named checkpoint when appropriate.
- Perform risky structural changes on a dedicated branch.

During refactoring:

- Keep commits focused.
- Validate coherent groups of changes.
- Do not mix unrelated cleanup with structural migrations.
- Preserve previous checkpoints.

Never push automatically unless explicitly authorized.

A local Git commit does not require pushing to a remote.

---

## 13. Documentation Workflow

Documentation should describe the finalized implementation.

When performing a structural refactor:

1. Complete the source organization.
2. Complete test organization.
3. Validate the resulting implementation.
4. Finalize public interfaces.
5. Write or update documentation afterward.

Do not create extensive documentation for an intermediate
directory structure that is still changing.

---

## 14. Final Principle

The repository should be understandable through its structure.

A developer should be able to determine:

- Where a component belongs.
- Which module owns a responsibility.
- Which interfaces are public.
- Where to add new functionality.
- Where to add unit tests.
- Where to add integration tests.
- How to validate a change.

Prefer:

**Clear ownership + cohesive modules + local unit tests +
explicit integration boundaries + minimal structural complexity.**

The purpose of organization is to reduce the effort required
to understand, extend, test, and maintain the system—not merely
to create a visually uniform directory tree.