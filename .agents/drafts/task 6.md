## MarketForge — Focused Normalization Correctness Fixes

**Status:** Agreed requirements for a later implementation.

**Scope:** Integrity observation accounting, time-window attribution, timestamp-regression detection, source-read error handling, and enforcement of profile-defined behavior.

**Implementation:** Deferred until explicitly requested.

This note supplements the configuration, testing, and normalization-refactor notes. It does not authorize a broad rewrite.

The objective is to close specific gaps between the integrity guarantees exposed by configuration and the behavior enforced by the processing pipeline.

### Assessment and Evidence Boundary

The findings below come from the supplied source snapshots. They are code-review findings, not results from newly executed tests.

They do not establish that previously generated datasets are incorrect. Clean source archives can pass through the successful execution paths without exercising these failure cases.

Recheck the current repository before implementation. If a finding has already been corrected, verify the correction and its regression coverage rather than implementing it again.

Preserve useful existing normalization, reconstruction, persistence, and integrity infrastructure.

## Integrity Observation Accounting

### Problem

Integrity rates are calculated from:

```text
violations / eligible observations
```

The reviewed observation collector increments its denominator only when a caller explicitly records an observation. The reviewed depth-success path updates processing counters and writes the outcome without recording successful integrity observations. :chatgpt-content-reference{index="0"} :chatgpt-content-reference{index="1"}

Consequently, categories receiving only failure observations can have incorrect rates and misleading sample counts.

For example, assuming all 1,000 messages are eligible parsing attempts:

```text
Actual source:
    999 successfully parsed messages
    1 parsing failure

Correct parse-failure accounting:
    eligible = 1000
    violations = 1
    rate = 0.001

Failure-only accounting:
    eligible = 1
    violations = 1
    rate = 1.0
```

A minimum-sample requirement may also remain unsatisfied because successful observations were never recorded.

### Required Correction

Define and implement the eligible observation unit for each integrity category.

Do not assume every category has the same denominator. A parsing attempt, a canonical-value validation, and a sequence-continuity check can have different eligibility requirements.

An observation must represent a check that actually occurred. Do not count a check as successful when processing failed before reaching it.

For example, a record that parses successfully but fails quantity transformation should contribute a successful parsing observation and the applicable transformation failure. It must not disappear from the parsing denominator merely because normalization ultimately failed.

Record successful and failed observations consistently across supported depth input paths, including JSONL, CSV, and XLSX where applicable. Review the trade path for the same stage-accounting requirement rather than assuming its successful-record loop is sufficient.

### Accounting Rules

Keep these quantities distinct:

```text
Physical source records or rows.
Accepted source events.
Eligible checks for a particular integrity category.
Canonical rows emitted.
```

A depth event producing 500 canonical level rows must not automatically contribute 500 source-message observations.

A valid depth event producing no changed canonical rows must still contribute to the checks actually performed on that event.

For multi-row snapshots, explicitly distinguish row-level checks from assembled-event checks. Do not count one assembled snapshot inconsistently depending on how many levels it contains.

Preserve source and instrument attribution. Do not assign an unreadable record to a concrete instrument merely to populate a stream counter.

Centralize common observation-recording logic where useful, but keep applicability decisions at the processing stage that knows which checks occurred.

Do not record the same observation again during final policy evaluation. Evaluation reads accumulated observations; it must not change their population.

### Required Verification

Use small deterministic fixtures to verify denominators, violations, rates, and minimum-sample behavior.

Include valid events, rejected events, multi-row snapshots, one-to-many depth output, and accepted events with no emitted changes.

Verify that aggregating independently collected task metrics produces the same applicable totals as collecting the equivalent observations together.

## Time-Window Attribution

### Problem

The reviewed trade error helper records violations with no event timestamp. The scoped collector only updates hourly and daily buckets when a timestamp is supplied. Errors passing through that helper therefore do not enter those time-window buckets. :chatgpt-content-reference{index="2"} :chatgpt-content-reference{index="3"}

Some rejected records genuinely have no trustworthy timestamp. Others have a valid timestamp but fail later during price, quantity, side, or another transformation.

The implementation must distinguish those situations.

### Required Correction

Preserve a validated source event timestamp when processing has already established one.

Pass that timestamp into the diagnostic and integrity observation when a later stage fails.

Apply the same principle across trade and depth processing paths.

Do not replace missing event time with processing time, the current wall clock, the archive filename date, or an arbitrary timestamp from a neighboring record.

### Unassignable Failures

When no trustworthy event timestamp can be recovered, record the failure in the applicable non-windowed scopes and expose that its time-window attribution is unavailable.

Do not silently discard it from the integrity assessment.

Do not represent a time-window check as fully verified when relevant failures could not be assigned to windows.

Where the requested policy relies on window enforcement that cannot be established, use an explicit documented fallback or reject the unsupported guarantee. Do not invent a fallback silently.

Keep additions to persisted metrics minimal and follow the existing versioning policy for any required serialization changes.

### Required Verification

Use a fixture spanning at least two known time windows.

Verify that a record with valid event time and an invalid later field contributes to the correct window.

Verify separately that a record with invalid or missing event time remains visible in non-windowed accounting and is explicitly identified as unattributed.

Threshold evaluation must use the intended eligible population, not merely the successfully normalized rows in that window.

## Timestamp-Regression Enforcement

### Problem

The reviewed trade processor handles records independently, while timestamp metrics retain minimum and maximum values rather than detecting regressions against preceding records. Minimum/maximum bounds alone do not establish chronological order. :chatgpt-content-reference{index="4"} :chatgpt-content-reference{index="5"}

A sequence such as:

```text
10:00:02
10:00:01
10:00:03
```

can have plausible timestamp bounds while containing a regression.

The supplied normalization paths do not establish that the exposed `timestamp_regression` policy is backed by an effective detector.

### Required Correction

Trace the production ordering and integrity paths before changing code.

Determine where monotonic event time is actually required, where records may legitimately arrive out of timestamp order, and where a profile authorizes sorting.

Do not equate source sequence order with monotonic event timestamps automatically.

For every supported ordering contract, define the applicable detection point and continuity scope. Where monotonicity is required, implement a real comparison against the appropriate previously accepted timestamp or watermark.

The detector must operate on the concrete stream, not on unrelated instruments mixed within a job or family archive.

Equal timestamps are not regressions unless an explicit supported contract requires strictly increasing timestamps.

Do not advance accepted-state tracking arbitrarily because a rejected record contained a timestamp.

### Source Order Versus Output Order

Distinguish:

```text
Source order required for valid interpretation.
Timestamp sorting explicitly allowed by the source profile.
Canonical output ordering promised by the execution contract.
```

An explicitly sortable source must not be rejected solely because its raw rows are unsorted.

Conversely, a source requiring sequence-preserving reconstruction must not be silently sorted to conceal an ordering problem.

Do not introduce a new cross-archive merge or sorting implementation under this task.

If a requested ordering guarantee is unsupported, reject it clearly rather than accepting the setting without enforcement.

### Depth Safety

A rejected depth message may contain state-changing information.

Apply existing synchronization invalidation and recovery requirements when the chosen regression policy rejects such a message. Do not continue with a seemingly valid book that omitted a required update.

Do not reset ordering state at every file boundary unless the supported continuity contract permits that reset.

### Required Verification

Test a backward timestamp, equal timestamps, independent streams, and explicitly sortable input.

Verify both the detector’s observations and the policy outcome.

For depth, verify that handling a regression does not silently damage reconstruction or reorder sequence-dependent updates.

## Recoverable Record Errors Versus Fatal Source Errors

### Problem

The reviewed JSONL decoder wraps source-read errors inside its JSON error type. The depth worker handles errors from that decoder as parsing failures and may continue according to the integrity policy. :chatgpt-content-reference{index="6"} :chatgpt-content-reference{index="7"}

This conflates malformed records with failures of the underlying input stream.

Under a permissive policy, a nonrecoverable read error can be treated as a skippable record. A reader repeatedly returning an error without advancing can then be retried indefinitely.

The reviewed task wrapper defers count/rate thresholds by removing them from the worker policy. Those deferred limits cannot be relied upon to terminate a reader that never makes progress. :chatgpt-content-reference{index="8"}

### Required Correction

Preserve a typed distinction between record-level decoding errors and source-stream failures.

Reuse the existing error infrastructure where practical. Do not classify errors by matching human-readable message strings.

A malformed complete JSON line may be recoverable when the decoder has advanced to a known next-record boundary and the configured policy permits recovery.

A nonrecoverable filesystem, archive, decompression, or stream-read failure must terminate the affected source/task. Do not repeatedly retry it as though another malformed record had been read.

Only explicitly recognized transient conditions may be retried, with bounded and documented behavior.

Do not assume every format error is recoverable. Continuing must be justified by a known decoder state and safe record boundary.

### Preserve Failure Semantics

An affected job must not commit successfully after a fatal source failure.

Preserve available metrics and diagnostics through the existing failure-reporting path.

Do not misclassify all I/O errors as source corruption. Preserve distinctions such as unreadable input, damaged compression, and output-write failure where the engine already supports them.

Do not change the supported policy for malformed records merely to simplify source-error handling.

### Required Verification

Use fault-injecting readers and tiny synthetic archives.

Verify that a valid prefix followed by a persistent read error terminates without repeatedly treating that error as new records.

Verify that a malformed JSON line followed by valid input can recover only when policy and reconstruction requirements allow it.

For depth, verify that recovery after a rejected state-changing message requires the appropriate authoritative initialization.

Do not use a hanging test or a large real archive to prove that the loop is bounded.

## Profile Settings Must Be Enforced

### Problem

The reviewed XLSX decoder directly assumes three columns, a timestamp in the first column, asks and bids in the next two columns, one worksheet, and timestamp-based sorting. Its decoding entry point does not receive a profile-derived column specification. :chatgpt-content-reference{index="9"} :chatgpt-content-reference{index="10"}

These assumptions may be correct for the supported archives. The correctness risk is accepting TOML settings that appear to control these behaviors while execution continues using fixed assumptions.

### Required Correction

Audit operational settings in the unified profiles, focusing on source decoding, mappings, ordering, and event semantics.

For each operational setting, establish one of the following:

| Treatment | Required behavior |
|---|---|
| Configurable and supported | The parsed setting reaches and controls the relevant consumer |
| Fixed implementation behavior | Validate that the profile agrees with the supported behavior |
| Unsupported | Reject the configuration with an actionable error |

Explicitly documentary metadata may remain descriptive, but it must not be presented as an enforced validation or execution option.

Do not silently ignore unsupported operational settings.

### Keep the Implementation Narrow

A specialized decoder is acceptable.

For XLSX, either pass the necessary resolved layout into the decoder or validate that the profile matches its supported fixed layout.

Do not build a universal spreadsheet-processing framework merely to eliminate a few fixed assumptions.

Use the actual unified-profile schema present in the repository. Do not invent another TOML structure.

Keep profile resolution and validation on the production path. Tests must not bypass them by supplying a manually constructed decoder configuration.

### Required Verification

Verify a supported profile with a small valid source.

Then change one meaningful setting in an isolated test copy. The result must either reflect the supported configuration change or fail explicitly.

Examples include incompatible column mapping, worksheet expectations, or unsupported ordering behavior.

Do not modify production profiles during tests.

## Preserve Existing Policy Semantics

These corrections must not silently redesign `action = "fail"` or `action = "degrade"`.

In particular, preserve established immediate-failure behavior for `action = "fail"`. Do not reinterpret:

```toml
action = "fail"
max_count = 1000
```

as permission to tolerate 1,000 violations when the existing evaluator treats the action as immediate failure.

Do not change `general.toml` thresholds, resource budgets, or enabled categories merely to make tests pass.

Correcting observations can legitimately change rate-based outcomes. That is an intended correctness change, but its cause must be demonstrated through deterministic tests.

Keep configuration ownership refactoring separate from these behavioral corrections. Moving a type or evaluator into `shared/` does not fix missing observations in its callers.

## Focused Regression Matrix

| Case | Required result |
|---|---|
| Valid depth records followed by one parsing failure | Correct eligible count, violation count, and rate |
| Valid parsing followed by transformation failure | Successful parsing observation and applicable transformation failure |
| One event emitting many canonical rows | No accidental multiplication of source-level observations |
| Accepted depth event emitting no changes | Applicable checks remain represented |
| Timed rejection in a known hour/day | Failure contributes to the correct window |
| Rejection without trustworthy event time | Failure remains visible with unavailable window attribution |
| Backward timestamp under a monotonicity contract | Regression is observed and the configured policy applies |
| Equal timestamps or independent streams | No false regression from an invalid comparison scope |
| Explicitly sortable source | Supported sorting behavior is respected and verified |
| Persistent source-read error | Bounded termination without successful dataset commit |
| Recoverable malformed record | Recovery occurs only with safe decoder state and permitted policy |
| Profile conflicts with a fixed decoder assumption | Clear validation failure rather than silent acceptance |

Tests should remain lightweight and deterministic.

Reuse the agreed shared test infrastructure, but do not require a real-data workload for fault injection, rate arithmetic, or isolated decoder-contract tests.

Do not execute benchmarks as part of this work.

## Scope and Implementation Boundaries

Apply the smallest changes needed to make the declared guarantees real.

Likely edit areas include normalization workers, integrity observation helpers, timestamp tracking, source decoder error propagation, and profile-to-decoder validation.

These are investigation targets, not instructions to rewrite every listed module.

Do not change canonical trade/depth schemas, dataset grouping, Parquet rollover, the prepared batch protocol, registry responsibilities, or merge/conversion algorithms.

Do not weaken production commit rules or existing failure handling to retain a successful result.

If an unavoidable persisted-metrics change is necessary, document and version it appropriately. Avoid an unrelated reporting-contract redesign.

## Implementation and Verification Report

When this work is later authorized, first reproduce each applicable gap with a focused test or a documented code path.

Implement the correction, then run the focused regression tests and relevant existing unit/integration tests.

The final report must distinguish confirmed defects, already-correct behavior, implemented corrections, and guarantees that remain unsupported.

For each correction, identify its observable behavior change and the test that protects it.

Report missing prerequisites honestly. Do not claim a guarantee was verified merely because configuration parsing or compilation succeeded.

## Acceptance Criteria

The work is complete when integrity rates use documented eligible populations, time-window attribution is truthful, timestamp-regression policies are enforced where supported, and fatal source errors cannot be treated as indefinitely recoverable records.

Operational profile settings must either control execution, match an explicitly supported fixed behavior, or fail validation.

Existing policy semantics, normalization interpretation, commit safety, and useful unit coverage must remain intact outside the identified corrections.

**Final objective: a successful normalization run must not imply integrity guarantees that the processing paths never actually checked.**