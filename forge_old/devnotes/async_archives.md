# Preliminary Note: Async Historical Acquisition and Database Inventory

## Updated direction

The historical acquisition rewrite should support asynchronous discovery and downloading across multiple exchanges.

This updates the previous note: asynchronous HTTP is now part of the intended rewrite.

PostgreSQL should become the authoritative inventory of discovered remote archives and locally acquired files. New downloads should not depend on manifest sidecars for registration or routine selection.

## Responsibility boundaries

### Shared asynchronous HTTP infrastructure

Place reusable HTTP execution infrastructure under:

```text
forge/shared/http/
├── client.py
├── limiter.py
├── retry.py
├── models.py
└── errors.py
```

This infrastructure owns:

- Asynchronous HTTP requests and response streaming.
- Connection pooling and client lifecycle.
- Request-rate limiting.
- Concurrent-request limits.
- Timeouts, cancellation and retry behavior.
- Shared cooldowns after rate-limit responses.

It must remain independent of archive discovery, exchange response formats and PostgreSQL archive records.

Use a reusable asynchronous client/session. Do not create a new client for every request.

### Historical archive implementation

Place historical-specific behavior under:

```text
forge/historical/archives/
├── sources/
├── models.py
├── repository.py
├── discovery.py
├── planning.py
├── download.py
└── storage/
```

Exchange adapters own endpoint selection, request construction, response parsing, supported archive scopes and default request policies.

The archive services use the shared HTTP infrastructure and persist discovery/acquisition results through the archive repository.

## Bulk execution

Support bounded concurrent work across exchanges and within an exchange where permitted.

A bulk operation may involve:

- Multiple exchanges.
- Multiple instruments or archive scopes.
- Multiple discovery windows.
- Multiple archive downloads.

Use bounded worker queues or equivalent bounded scheduling. Do not create an unbounded task for every archive in a large request.

Keep discovery concurrency and download concurrency independently configurable. Large downloads occupy connections for much longer than metadata requests.

Failure of one independent item should be recorded without automatically discarding successful results from the rest of the batch. Return an aggregate result describing successes, failures and cancellations.

## User-configurable request limits

Every source must have a default request policy. Callers may provide explicit overrides when starting an operation.

The policy should include:

| Setting | Purpose |
|---|---|
| `requests_per_second` | Limit how frequently requests may start. |
| `max_concurrent_requests` | Limit requests simultaneously in flight. |
| `max_concurrent_downloads` | Bound simultaneous archive transfers. |
| Timeouts | Bound connection, read and overall operation behavior as appropriate. |
| Retry settings | Control attempts, backoff and retryable failures. |

Validate policy values before starting work.

Use conservative source defaults initially. Verify applicable exchange limits when implementing each adapter.

### Shared quota ownership

A limiter must be shared by all tasks using the same source quota.

Creating a separate limiter for every instrument, task or request would multiply the effective request rate and defeat the limit.

Adapters must identify the appropriate quota scope. An exchange metadata endpoint and an archive-download host may have different budgets; requests sharing a provider budget must share limiter state.

Per-call overrides must configure the shared operation policy rather than create independent budgets for concurrent tasks.

The initial guarantee may be limited to one Forge process. Multiple independent processes require coordinated limiting or an explicitly documented deployment restriction.

### Rate and concurrency are separate controls

A semaphore alone does not enforce requests per second.

Apply rate control to every outbound attempt, including:

- Discovery requests.
- Archive existence probes.
- Download requests.
- Retries.

Use predictable request pacing without an initial burst unless burst behavior is explicitly configured.

For HTTP 429 responses, honor valid `Retry-After` information and apply the cooldown to other tasks sharing the affected quota. Use bounded retries and backoff with jitter where appropriate.

Rate limits and transient HTTP failures must never be recorded as evidence that an archive does not exist.

## PostgreSQL acquisition inventory

Retain the three-table separation proposed previously:

| Table | Responsibility |
|---|---|
| `historical.remote_archives` | Known source archives and their current download locations. |
| `historical.discovery_scans` | Successfully searched scopes, incomplete searches and cache freshness. |
| `historical.local_archives` | Local acquisition state, storage locations and integrity metadata. |

The source URL and the local storage location are separate facts.

For each local acquisition, record at least:

- Remote archive ID.
- Storage-root identifier.
- Path relative to that storage root.
- Acquisition status.
- Actual file size and SHA256 checksum.
- Completion and verification timestamps.
- Failure information when applicable.

Resolve the storage-root identifier through configuration. For example, a configured root of `data/` plus a stored relative path of `raw/.../archive.zip` identifies the actual local file.

Routine archive selection should query PostgreSQL rather than recursively scan directories or infer identity and coverage from filenames.

## Concurrent download ownership

Prevent concurrent workers from writing to the same destination.

Extend the proposed local inventory as needed with acquisition ownership fields, for example:

```sql
ALTER TABLE historical.local_archives
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN lease_owner TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;
```

Claim acquisition work atomically using a short database transaction.

An expired lease must trigger reconciliation before another worker starts writing. The previous owner may have finished the file but failed before updating PostgreSQL.

Do not hold database transactions open during discovery or file transfer.

## Download lifecycle

For each selected archive:

1. Resolve its remote identity and configured local destination.
2. Claim its acquisition record and mark it `downloading`.
3. Stream the response into an attempt-specific temporary file.
4. Compute the checksum and count actual bytes while streaming.
5. Perform applicable verification.
6. Finalize the file using atomic replacement on the same filesystem.
7. Commit the local inventory record as `complete`.

Use exact source sizes for verification only when the source provides exact values.

Keep memory use bounded. Do not load entire archives into memory.

Blocking filesystem work must not stall the asynchronous event loop. Use an appropriate bounded writing mechanism, and avoid blocking database calls on the event-loop thread.

The database access approach may use an asynchronous driver or bounded offloading of existing synchronous operations. Choose one explicit approach during implementation.

## Interruptions and reconciliation

Filesystem finalization and PostgreSQL commits are not one atomic operation.

Handle:

- Cancellation during transfer.
- Failed or interrupted downloads.
- Finalized files whose database completion update failed.
- Expired acquisition leases.
- Missing or externally moved files.
- Corrupt files.
- Existing files at a proposed destination.

A PostgreSQL `complete` status does not by itself prove the current file exists or remains valid.

Use explicit verification and reconciliation policies. Do not silently replace a verified original when the source appears to have revised an archive.

## Existing manifests

Existing manifest sidecars may be used to import legacy acquisitions into PostgreSQL.

Do not require new downloads to create sidecars as part of the normal acquisition workflow.

Filesystem scanning and manifest parsing remain useful migration or reconciliation tools. They should not remain the primary inventory mechanism.

## Initial acceptance criteria

- Discovery and downloads use asynchronous HTTP.
- Different exchanges can progress concurrently.
- Configured rate limits apply across tasks sharing a quota.
- Rate limits remain effective during retries.
- Concurrency and memory usage remain bounded for large batches.
- Fresh cached discovery avoids redundant exchange requests.
- Successful downloads are registered in PostgreSQL with resolvable storage locations.
- Concurrent requests cannot write to the same destination.
- Cancellation and failures leave recoverable acquisition state.
- Legacy acquisitions can be imported without redownloading verified files.
- Tests cover limiter behavior, shared cooldowns, bounded scheduling, duplicate acquisition attempts and interrupted completion.
- Unrelated scripts, Python live functionality and Rust engine behavior remain unchanged.
