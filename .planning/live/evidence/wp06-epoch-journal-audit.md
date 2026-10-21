# FileEpochStore — focused filesystem correctness audit

Date: 2026-10-21. Scope: `engine/src/live/foundation/filesystem.rs` only,
plus this evidence and verification logs. No architecture redesign, historical
changes, PostgreSQL changes, or unrelated WP-06 implementation changes.

## Findings and corrections

1. **Opening existing authority could rewrite it unnecessarily.** The old code
   used creation of the sidecar lock to decide whether to persist during open.
   An existing valid journal without its sidecar was consequently rewritten.
   Initialization now occurs only when the journal is absent, this attempt created
   the sidecar, and no pending artifact exists. Opening an existing journal is
   read-only, including when its pending path is unusable.
2. **Ambiguous duplicate stream keys were accepted by ordinary map decoding.**
   A duplicate such as `"s":8,"s":2` must not select a lower counter silently.
   A narrow deserializer now rejects duplicate stream identities. The version-1
   format is unchanged for journals emitted by the canonical writer.
3. **Descriptor lifetime could outlive the logical lock owner.** An initial
   concurrent test observed a lock still unavailable after the owner threads
   joined while a subprocess test ran concurrently. Closing one file descriptor
   is insufficient when another descriptor shares its lock. A private EpochLock
   guard now explicitly unlocks at scope exit, including post-acquisition open
   errors. Deterministic duplicated-descriptor tests cover this mechanism.
   The initial concurrent subprocess-spawn interleaving is not claimed to have
   been reconstructed exactly. No lock file is deleted on release.
4. **A pending-only directory must not be treated as pristine initialization.**
   Even without a preexisting sidecar, pending data without an authoritative
   journal now causes an explicit error. Pending files are never promoted or
   parsed to reconstruct missing counters.

The existing conservative persistence policy remains: any persistence failure
fences that owner for every stream. Exhaustion and rejected empty identities do
not mutate files or unnecessarily fence other stream identities.

## Persistence boundaries

Private deterministic fault callbacks bracket:

- Lock acquisition, before journal loading/initialization.
- Temporary-file opening and successful opening.
- Temporary write completion, including a simulated partial-content failure.
- Before and after temporary-file synchronization.
- Before and after atomic rename.
- Before and after parent-directory synchronization.

Production operations use a private no-op callback; no public fault API,
dependency, unsafe code, retry loop or alternative persistence implementation was
introduced. Tests assert that each requested injection point was actually reached.

The pre-rename failure tests verify unchanged authoritative bytes. Post-rename
failures verify that the running filesystem exposes the tentative new counter,
the failed owner exposes no epoch and is fenced, and a reopened owner advances
past the visible counter. Before rename, an unexposed tentative value may be
retried from the original authority. Directory-sync ambiguity is never converted
to success, even when the callback fails after a successful sync syscall.

**These tests observe a live filesystem and injected error boundaries. They do
not simulate loss of kernel caches, storage-controller caches, torn sectors,
filesystem recovery, machine reset, or actual power loss. They do not prove
power-loss durability.** Actual guarantees depend on the supported filesystem's
locking, atomic rename and sync semantics.

## Requested audit coverage

All 18 tests are inline in `live::foundation::filesystem::tests`.

| Requested area | Tests / observed policy |
|---|---|
| Interrupted first initialization | `interrupted_lock_only_initialization_fails_closed`; `every_interrupted_initialization_boundary_has_an_explicit_reopen_outcome` exercises lock acquisition and nine write boundaries. Before rename, reopening rejects lock-only/pending-only state; after rename, it reads only the existing authoritative journal |
| Failures before/after temporary sync and rename | `persistence_failure_at_every_boundary_fences_and_never_exposes_an_epoch`; `partial_temporary_contents_never_replace_authority_on_failure`; `real_rename_error_preserves_authority_and_fences` |
| Parent-directory-sync ambiguity | The same boundary matrix includes both sides of directory sync; each failure fences and skips any visibly renamed tentative epoch on reopen |
| Stale pending files | `stale_pending_is_ignored_until_a_locked_write_overwrites_it` covers empty, partial and valid higher-counter pending content; `pending_without_authority_is_never_promoted_or_used_to_initialize` covers absent/present sidecars |
| Concurrent owners | `concurrent_threads_keep_one_owner_across_journal_renames`; `lock_excludes_another_process` uses an actual child test process and pipe handshake, with no arbitrary sleeps |
| Lock release and error cleanup | `lock_owner_drop_releases_even_with_a_duplicated_descriptor`; `lock_guard_releases_on_error_even_with_a_duplicated_descriptor` |
| Missing/corrupt/unsupported/oversized authority | Missing-authority tests above; `malformed_unsupported_and_ambiguous_journals_are_preserved_and_rejected` covers empty/truncated data, unsupported versions, blank identity, negative/overflow counters, duplicate stream/version keys and unknown fields; `journal_size_boundary_is_exact_and_read_only` accepts exactly 1 MiB and rejects 1 MiB + 1 byte |
| Serialization failure before temporary write | `serialization_size_failure_fences_without_touching_files` leaves authority unchanged and creates no pending file |
| Epoch exhaustion/multiple identities | `exhaustion_and_invalid_input_do_not_fence_other_streams_or_write` verifies an exhausted stream, invalid identity rejection, independent advancing counters and reopen continuation |
| Read-only open with a newly created lock | `existing_journal_without_sidecar_is_read_only_even_with_unwritable_pending_path` preserves original bytes and visits no persistence hooks |
| Explicit unsupported operations | `unsupported_lock_deletion_can_split_lock_identity` (Unix) and `unsupported_valid_journal_rollback_is_not_detectable` demonstrate limitations, not guarantees |

The real rename-error fixture deliberately moves its authority aside and puts a
directory at the target to provoke an actual rename syscall failure. The store
does not repair that externally malformed path; the saved authoritative bytes
remain unchanged. This is not presented as a normal cooperating-writer crash.

## Fail-closed behavior and limitations

- A leftover `.lock` without authority remains an error, even when initialization
  never exposed an epoch. It is not automatically reset or reconstructed.
- `.pending` is never authoritative. With a valid journal, normal locked
  persistence may overwrite stale pending bytes; without authority it cannot
  authorize recovery or fresh initialization.
- Valid counters survive serialized owners and atomic journal replacement under
  the stable sidecar lock. Cooperating writers must use the same stable journal
  and sidecar paths and supported local locking semantics.
- **Deleting/replacing the lock file while an owner exists can create a second
  lock inode.** The limitation test retains the first locked descriptor and opens
  a replacement sidecar successfully. Explicit unlock on Drop does not prevent
  this unsupported administrative operation.
- **Restoring valid older journal bytes is undetectable to this local journal.**
  The limitation test deliberately restores an older supported JSON journal and
  demonstrates reuse of a previously issued epoch. This is an unsupported rollback,
  not crash recovery, and not a relaxed production identity guarantee. Preventing
  it requires an additional trusted monotonic authority; none is introduced here.
- External journal/path manipulation and rollback protection are outside the
  cooperating-owner protocol. Missing or invalid state is never automatically
  repaired by guessing. Historical identities and PostgreSQL mapping are untouched.

## Verification

All commands ran from `engine/` with no network access or dependency changes.

| Command | Exit / exact final result |
|---|---|
| `cargo fmt --check` | 0, pass |
| `cargo check --locked --offline --no-default-features --features live` | 0, pass |
| `cargo test --locked --offline --no-default-features --features live --lib live::foundation::filesystem::tests` | 0; 18 passed, 0 failed, 0 ignored, 75 filtered out |
| `cargo test --locked --offline --no-default-features --features live --lib` | 0; 93 passed, 0 failed, 0 ignored, 0 filtered out |
| `cargo test --locked --offline --no-default-features --features live --test live_suite` | 0; 323 passed, 0 failed, 0 ignored, 0 filtered out |

The targeted 18 tests are included in the 93 library tests; they are not 18
additional tests outside that total. The subprocess witness is invoked by its
parent test through the same exact named test and a task-specific environment
variable, with stdin/stdout synchronization.

Full output, executed test names, warnings, durations and command receipts:
`wp06-epoch-journal-audit/verification-1.log` through `verification-5.log`, and
`wp06-epoch-journal-audit/verification.json`. Existing unrelated router/recovery
warnings remain visible and unsuppressed.

Development diagnostics: the first formatting check failed before rustfmt was
applied. The first targeted behavioral run had 15 passed / 1 failed (the lock
lifetime issue above); two descriptor-lifetime tests and the explicit guard were
then added. The corrected targeted run had 18 passed / 0 failed. Original failure
diagnostics remain in the tool transcript. No assertions were weakened or removed.

Only `engine/src/live/foundation/filesystem.rs` changed among implementation files.
Other created files are this report and its six verification artifacts. No commits
or pushes were performed. The C06 mapping blocker and historical baseline issue
remain outside this focused filesystem audit.
