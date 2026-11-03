# Preliminary Note: Persistent Historical Archive Discovery

## Purpose and scope

Introduce a PostgreSQL-backed catalogue of historical raw archives so MarketForge can reuse previous discovery results instead of repeatedly querying exchange APIs, enumerating listings or probing the same URLs.

This note is a preliminary reference for a later Codex implementation assignment. Codex must inspect the current repository before implementing it and adapt the migration to the existing database structure.

The first implementation should remain synchronous. An asynchronous HTTP implementation is a separate task.

Leave the existing Python `live/` directory, unrelated scripts and Rust engine implementation unchanged.

## Database ownership

Use the existing PostgreSQL database. A separate database is unnecessary.

Create a dedicated `historical` schema containing archive discovery and acquisition metadata:

- `historical.remote_archives`
- `historical.discovery_scans`
- `historical.local_archives`

Continue storing archive bytes under `data/raw/`.

Existing processed-dataset records remain separate. Existing raw-format definitions describe how archives are interpreted; they do not replace the new archive inventory.

## Separate the three operations

### Discovery

Discovery contacts an exchange, enumerates or probes archives, and persists the results.

Preserve every valid archive discovered by a response, including entries outside the user's immediate selection when the source returns a broader listing.

Discovery should operate on demand. Do not require a complete crawl of every exchange or instrument.

### Planning

Planning selects known remote archives overlapping a requested interval and determines which require downloading or can be reused locally.

The planner must distinguish:

- Known available archives.
- Successfully searched intervals with no matching archives.
- Unknown, stale or incompletely searched intervals.
- Locally acquired archives and their verification state.

Provide a cache-only mode that performs no exchange discovery and explicitly reports unresolved coverage.

### Downloading

Downloading consumes a resolved acquisition plan.

It should use the selected archives directly, without repeating discovery for the whole request. A stale or invalid download location may require a targeted refresh using the smallest scope supported by the adapter.

A higher-level acquisition operation may coordinate discovery, planning and downloading for convenience.

## Archive identity and discovery scope

An archive's identity must be independent of its current download URL.

Prefer a stable source object identifier or object key. Do not use a potentially temporary URL as the unique archive identity.

Discovery scope must support:

- Individual instruments.
- Base-coin archive bundles.
- Instrument families.
- Source-specific variants that affect archive selection.

An archive must not require an instrument foreign key when the physical archive represents a broader bundle.

Define one canonical scope representation and use it consistently in both archive and discovery records. For example:

```json
{
  "instrument_type": "perpetual",
  "market_category": "linear",
  "kind": "instrument",
  "identifier": "BTCUSDT",
  "variant": {}
}
```

`scope_key` is a deterministic, unambiguous key derived from this representation. Scope identity must include every parameter that changes the archive collection.

Do not assume one archive per day. Existing sources use daily, monthly and hourly archives, and may return multiple physical files for an interval.

## Proposed PostgreSQL schema

The following SQL is a baseline for discussion. Codex should implement it through the project's migration mechanism rather than merely changing the schema-version constant.

```sql
CREATE SCHEMA IF NOT EXISTS historical;

CREATE TABLE historical.remote_archives (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    exchange_id         BIGINT NOT NULL
                            REFERENCES catalog.exchanges(id),

    scope_key           TEXT NOT NULL,
    scope_json          JSONB NOT NULL,
    data_type           TEXT NOT NULL,

    source_object_key   TEXT NOT NULL,
    filename            TEXT NOT NULL,

    start_at            TIMESTAMPTZ NOT NULL,
    end_at              TIMESTAMPTZ NOT NULL,

    download_url        TEXT NOT NULL,
    url_expires_at      TIMESTAMPTZ,

    expected_size_bytes BIGINT,
    source_etag         TEXT,
    source_last_modified TIMESTAMPTZ,

    remote_status       TEXT NOT NULL DEFAULT 'available',

    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_checked_at     TIMESTAMPTZ,

    source_metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,

    UNIQUE (
        exchange_id,
        scope_key,
        data_type,
        source_object_key
    ),

    CHECK (end_at > start_at),

    CHECK (
        expected_size_bytes IS NULL
        OR expected_size_bytes >= 0
    ),

    CHECK (
        remote_status IN ('available', 'unavailable', 'unknown')
    )
);

CREATE INDEX historical_remote_archives_selection_idx
    ON historical.remote_archives (
        exchange_id,
        scope_key,
        data_type,
        start_at
    );


CREATE TABLE historical.discovery_scans (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    exchange_id         BIGINT NOT NULL
                            REFERENCES catalog.exchanges(id),

    scope_key           TEXT NOT NULL,
    scope_json          JSONB NOT NULL,
    data_type           TEXT NOT NULL,

    searched_start_at   TIMESTAMPTZ NOT NULL,
    searched_end_at     TIMESTAMPTZ NOT NULL,

    status              TEXT NOT NULL,

    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    refresh_after       TIMESTAMPTZ,

    archives_found      BIGINT NOT NULL DEFAULT 0,

    adapter_version     TEXT,
    error_message       TEXT,
    scan_metadata       JSONB NOT NULL DEFAULT '{}'::jsonb,

    CHECK (searched_end_at > searched_start_at),

    CHECK (
        status IN (
            'running',
            'complete',
            'partial',
            'failed',
            'cancelled'
        )
    ),

    CHECK (
        status <> 'complete'
        OR (
            completed_at IS NOT NULL
            AND refresh_after IS NOT NULL
        )
    ),

    CHECK (archives_found >= 0)
);

CREATE INDEX historical_discovery_scans_lookup_idx
    ON historical.discovery_scans (
        exchange_id,
        scope_key,
        data_type,
        status,
        searched_start_at,
        searched_end_at
    );


CREATE TABLE historical.local_archives (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    remote_archive_id   BIGINT NOT NULL
                            REFERENCES historical.remote_archives(id),

    storage_root_key    TEXT NOT NULL,
    relative_path       TEXT NOT NULL,

    status              TEXT NOT NULL DEFAULT 'pending',

    bytes_written       BIGINT,
    sha256              TEXT,

    downloaded_at       TIMESTAMPTZ,
    last_verified_at    TIMESTAMPTZ,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    failure_message     TEXT,

    UNIQUE (
        storage_root_key,
        relative_path
    ),

    CHECK (
        status IN (
            'pending',
            'downloading',
            'complete',
            'missing',
            'corrupt',
            'failed',
            'deleted'
        )
    ),

    CHECK (
        bytes_written IS NULL
        OR bytes_written >= 0
    ),

    CHECK (
        status <> 'complete'
        OR (
            bytes_written IS NOT NULL
            AND sha256 IS NOT NULL
            AND downloaded_at IS NOT NULL
        )
    ),

    CHECK (
        sha256 IS NULL
        OR sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE INDEX historical_local_archives_remote_idx
    ON historical.local_archives(remote_archive_id);
```

Use timezone-aware inputs and the half-open interval convention `[start, end)` throughout.

Archive selection uses interval overlap:

```sql
start_at < requested_end
AND end_at > requested_start
```

`expected_size_bytes` must contain an exact source size when available. Approximate display sizes must remain explicitly identified in source metadata and must not be used as exact verification values.

Storage roots should be resolved through application configuration. Store validated relative paths rather than assuming the same absolute filesystem path on every machine.

## Discovery completeness and caching rules

### Record searched coverage separately from discovered files

Missing archive rows must never automatically mean that no archives exist.

Only a successfully completed discovery scan can establish that its scope and interval were enumerated. This includes successful searches returning zero archives.

A complete scan establishes discovery completeness as observed at that time. It does not certify that archive contents provide complete market-data coverage.

### Preserve progress without claiming false completeness

Persist discovered archives incrementally using idempotent upserts.

Commit completed source windows or batches without holding database transactions open during network requests.

If pagination, parsing or another required step fails, do not mark the affected discovery scope complete. Previously completed independent windows remain reusable.

Source disappearance must not delete previously downloaded local files.

### Apply source-aware freshness policies

Recent intervals and negative discovery results require refresh policies because archives can be published later.

Older positive archive records should be retained and reused. Their retention is separate from the freshness of the scan that discovered them.

Support explicit refresh and cache-only behavior. Concrete refresh durations should be configurable and chosen per source.

HTTP failures, rate limits and malformed responses must not be cached as proof that data is absent.

### Respect native adapter behavior

| Source mechanism | Required caching behavior |
|---|---|
| Full archive listing | Preserve all returned archive records and record the successfully enumerated scope and observation horizon. |
| Window-based API | Record completed windows and request unresolved or stale windows when needed. |
| Candidate URL probing | Reuse previous probe outcomes according to their freshness policy. |

When an adapter must repeat a broader native listing, allow that operation. Do not pretend it supports arbitrary incremental discovery.

Cached intervals must be combined correctly when determining whether a request is covered. The earliest and latest known archive dates alone are insufficient.

## Local files and manifests

Retain the existing manifest mechanism during this refactor.

PostgreSQL provides a searchable inventory; manifests preserve verification information beside the files.

Only mark a local archive complete after its file and manifest have been successfully finalized.

Filesystem writes and PostgreSQL commits are not one atomic transaction. Provide reconciliation for interruptions between those steps.

Database status alone must not prove that a local file still exists or is valid. Define explicit verification and reconciliation behavior.

If source metadata suggests an archive was revised, preserve the existing local bytes and their checksum. Do not silently overwrite a verified original or claim that it matches a newer source revision.

## Python organization

Use the agreed historical capability boundary:

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

Responsibilities:

- `sources/`: exchange-specific enumeration, probing and location resolution.
- `repository.py`: persistence and catalogue queries.
- `discovery.py`: cached coverage, refresh decisions and discovery execution.
- `planning.py`: archive selection and download/reuse decisions.
- `download.py`: execution of selected downloads.
- `storage/`: paths, manifests and local reconciliation.

Retain useful existing adapter logic. Do not combine this task with an asynchronous rewrite or unrelated repository restructuring.

## Request control

Pass the intended HTTP client into the planner and source adapters. The reviewed client currently creates an HTTP client without forwarding it to the planner.

Retain conservative source-specific throttling and retry behavior. Caching reduces redundant traffic but does not replace request control.

Ensure planning, availability checks and acquisition can reuse the same persistent discovery service rather than independently rediscovering the same collection.

## Initial acceptance criteria

- Repeating a request with fresh, complete cached discovery performs no discovery network calls.
- Running `plan` followed by `download` reuses the same cached discovery.
- Discovered but undownloaded archives remain selectable.
- Successfully searched empty intervals are distinguishable from unknown intervals.
- Failed or partial discovery never establishes complete coverage.
- Overlapping requests reuse completed cached scopes where the source mechanism permits.
- Temporary URL changes do not create duplicate archive identities.
- Existing local archives can be reconciled from their files and manifests.
- Tests use mocked discovery responses for caching, expiry, pagination failure, interval handling and reconciliation.
- Existing processed-dataset behavior and unrelated live-engine functionality remain unaffected.
