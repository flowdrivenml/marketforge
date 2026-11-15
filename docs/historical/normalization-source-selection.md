# Prepared normalization sources and selection

Python supplies source archives in request-priority order, exact selection intent,
authoritative offline instrument metadata, and output destinations. Rust discovers
selected-event chronology, filters native identities, reconciles supported source
overlap, normalizes each concrete instrument independently, and validates output
before publication. `requested_interval` remains request metadata; this change
adds no event-time clipping.

## Accepted external contract

The active **prepared-batch schema is 4**, `kind = "normalization_batch"`,
`status = "prepared"`. All existing batch/run/ranking/interval, namespace, offline
catalog, job/order/output and task classification fields retain their placement.
`PreparedBatch`, `PreparedTask`, `OfflineInstrument` and `FamilyMember` remain the
production models. See `config/batch.rs` and `normalize-batch-v4.example.json`.
The example deliberately does not invent the absent historical futures authority;
its family source needs that authority before resolution can succeed.

Prepared-task `source_time_bounds` is removed, including its `partition`.
Supplying it, even as null, is an unknown-field error. Prepared schema 3 is no
longer accepted as the active batch contract. No replacement coverage, first/last
archive timestamp, source-pointer file, database or ordering cache is required.
Standalone normalization **job protocol 3**, internal resolved job protocol 1,
CLI result versions, runtime TOML schema 1, and dataset manifest versions are
separate contracts. Standalone profile jobs remain concrete-symbol requests and
do not acquire a new multi-instrument wire interface here.

Old resolved-job adapters may retain coverage fields inside historical descriptive
`marketforge_source` provenance. They do not execute them. On standalone loading,
request priority is rebuilt from the input task array and the internal prepared
routing marker is cleared. Published provenance/manifests are not rewritten.
Legacy concrete-worker calls cannot execute a family selection without the
prepared-session router; they fail explicitly instead of claiming a partial
family population.

Add selection directly to a family task:

```json
"instrument_selection": {"mode": "all"}
```

or:

```json
"instrument_selection": {
  "mode": "include",
  "symbols": ["BTC-USD_UM-261030", "BTC-USD_UM-261225"]
}
```

These names are illustrative, not authoritative specifications. Unknown modes,
unknown fields, `symbols` attached to `all` (including null), missing/empty include
lists, empty/whitespace-only symbols and exact duplicates are rejected. Symbols
are compared exactly, without wildcard, alias guessing or case conversion.
A concrete-symbol task may omit selection, preserving its single-symbol behavior,
or include only that exact symbol. `symbol` together with `all` cannot broaden it.
A family task requires explicit selection; omission is not a compatibility alias
for a changing population.

`all` covers every eligible instrument **encountered in the source**, not just
catalog entries. `include` filters before contract-metadata lookup. An unselected
unknown contract does not require fabricated metadata. An encountered selected
symbol missing authority or unsupported required specifications fails explicitly.
Known family candidates can remain unencountered without their deferred contract
requirements failing the job. No placeholder IDs, tick sizes or contract values
are generated. Duplicate/ambiguous offline instrument identities still fail;
the catalog does not silently pick among historical specification versions.

Native family routing currently supports the existing OKX futures naming grammar
and OKX perpetual `-SWAP` family identity, with exact exchange, instrument kind,
market category, namespace and feed/profile separation. Futures membership reuses
the existing matcher parser rather than prefix selection. Other family identity
schemes are unsupported and require authoritative routing support, not guesses.
Concrete formats/profiles retain their existing support and interpretation.

For an `include` request, absence in one monthly source is allowed. Fulfillment
is checked across applicable selected tasks in the same job, native family,
parent stream and feed. A requested symbol never observed in those tasks fails
as an unsatisfied selection. A present selected record that fails normalization
is observed, not absent; its integrity failure remains separate. Changing monthly
populations are processed against the full supplied authority on every source.

## Priority, chronology and safe emission

Original zero-based task-array position supplies conflict priority. It is captured
before selection/grouping/family expansion and carried in resolved provenance.
Task IDs, stream ranks, filenames, traversal, hashes, arrival order and worker
completion do not supply priority. Derived IDs/ranks are deterministic; supplied
concrete ranks remain reserved. Colliding derived identities fail.

Single concrete sources retain their existing streaming worker/writer path.
Grouped sources spool canonical events to owned scratch storage during their
single forward normalization pass. No complete source record set is retained in
RAM. Trades use bounded external identity sorting, incremental first-source
resolution, then bounded chronological run merging before canonical emission.
Depth spools complete source-event outcomes and boundaries and replays them only
after conservative chronology/recovery checks. Existing bounded XLSX sorting
remains unchanged. See [disk reconciliation](normalization-disk-reconciliation.md)
for accounting, format, cleanup and verification details.

Trade identity is scoped to exchange, authoritative concrete instrument lifetime,
namespace and compatible stream/profile/feed. Current supported persistent native
ID domains are Binance/OKX mapped trade IDs and Bybit UUID match IDs. Other
identity/reset domains are not inferred from a day, filename or expiry cache:
ambiguous overlapping sources, or cross-source collisions without an established
domain, fail. OKX documents native trade-ID uniqueness per instrument in its
[identifier guide](https://my.okx.com/docs-v5/trick_en/#identifiers). Canonical IDs
must refer to the correct historical instrument instance; no timestamp-based
identity expiration is introduced.

Equivalent cross-source identity/payload emits once; conflicting payloads retain
the higher-priority source. Timestamp changes in a conflict cannot make a
lower-priority record authoritative early: winner selection precedes output
sorting. Different IDs with equal timestamps survive. Within-source duplicate
handling is retained. Ties use priority, concrete source/member and physical
decoder-record ordinals. No price/quantity/side tuple is used as a universal ID.
Unique events inside an overlap and its tail survive for supported domains.

Equivalent physical source/member/interpretation/selection requests retain the
first selection. Canonical paths and, on Unix, device/inode identity establish
physical equality; basename, length, date and coverage do not. No mandatory
whole-file hash is computed. Different profiles, filters, member mappings or
instrument specifications are not automatically redundant. Selection-list order
is canonicalized because it does not carry priority.

Grouped sources with rejected records plus another selected source can lack the
preferred event identity needed to rule out an unintended fallback. That case
fails explicitly rather than silently choosing a lower-priority valid version.
A redundant request alone does not cause that failure. A source-order regression
that prevents chronological reconciliation also fails; source-ordered stateful
records are never repaired by arbitrary timestamp sorting. The single-source
integrity-policy path remains available with its existing semantics.

## Depth and mixed-source routing

Depth grouping orders disjoint selected-event source ranges observed during
normal processing, independent of request priority. Each source must retain its
existing independent snapshot initialization and sequence/recovery evidence.
Complete outcomes and their event ordinals are retained atomically. Unrecovered
boundary gaps fail. Incremental and snapshot feeds are not combined.

Apart from equivalent physical-source suppression, partial/equal-time depth
source overlap currently fails with an explicit explanation: independent book
level diffs and a timestamp or sequence alone do not establish event equivalence
or a safe recovery splice. The implementation does not discard a lower-priority
tail, deduplicate individual levels, bridge a gap or manufacture synchronization.
This is an intentional supported-case limitation.

Mixed CSV trades and JSONL depth are decoded once per compatible original family
source/member and routed using the profile/native symbol field. Decoded records
are routed to per-concrete scratch spools with original member and decoder-record ordinals. Concrete workers consume
one-shot internal decoded inputs; they do not reopen and decode the entire
archive for each symbol. Each instrument has independent timestamp validation,
identity tracking, depth processor, sequence state, book, metrics and writer route.
Interleaved instruments do not share a global timestamp domain.

Authoritative explicit member mappings remain usable for include requests.
`all` cannot claim complete population by silently scanning only a finite member
map: it requires a native-symbol mixed data member (or one data member).
Arbitrary multi-member archive enumeration is not implemented. Ambiguous member
selection fails through the existing reader checks. XLSX mixed-symbol routing is
unsupported because those profiles do not expose the required native identity.
Distinct requests with different filters/profiles can still decode the same
physical archive separately; the per-symbol repeated-decoding default is removed.

Trade CSV parse errors still use the existing Fail/Degrade policy and bounded
file diagnostics. An unidentifiable corrupted family depth message cannot be
assigned to a safe per-instrument recovery history; this unsupported case fails
explicitly, even when skipping a standalone malformed record could be allowed.
Selected missing metadata is not classified as an invalid market record.

## Resources and observations

The authoritative runtime file remains `data/.config/historical/general.toml`.
Existing deployment values/defaults are not raised. One new defaulted field in
`[resources.normalization_limits]` is added:

| Field | Default | Unit/scope | Enforcement and admission |
|---|---:|---|---|
| `reconciliation_bytes` | 67,108,864 | accounted resident application bytes per grouped/family execution unit | Reusable leases for codec buffers, decoded frames, external-sort chunks/cursors and event replay; retained charges for source/range inventories; added once to stream admission |

The limit now constrains retained active state, rather than cumulatively charging
all historical records. Scratch is enforced separately on compressed bytes before
writes through a shared process-local capacity pool using the configured
`scratch_budget_bytes`; defaults are unchanged. Canonical staging is not scratch.
Fixed two-way merges and sequential writers retain finite file admission. No
identity map, suppression set or run-path inventory grows with record count.
Oversized individual frames, insufficient codec/chunk capacity, disk exhaustion
or provenance-range exhaustion fail explicitly. No source is truncated.

Family candidate count and source ownership ranges use the existing
`metric_entries` quota. Prepared expansion checks a reusable footprint against
the runtime memory budget before retaining each derived task and avoids copying
a whole family-symbol list into every child. Source/window metrics retain their
existing caps. Concrete writers open/finalize sequentially under the unit's one
persistent-writer reservation; no unbounded writer fan-out or per-archive output
partition is introduced. One active reader/member is counted at a time. The
reservation survives finalization. Source-only estimates retain zero static scratch reservation; grouped execution
uses actual, nonblocking scratch growth accounting.
The effective configuration/provenance includes the new limit; old runtime schema-1
documents default it deliberately. The local runtime TOML is ignored by this
checkout's Git rules and was updated without force-adding it.

Estimated/reserved memory, checked application capacity and process RSS remain
separate. Initial JSON/catalog loading and third-party codec/parser/encoder
allocations retain the limitations in `normalization-resource-safety.md`. This
implementation does not establish a hard RSS ceiling or a shared pool across
independently launched processes.

Task reports add optional typed `source` observations through the existing
success/failure reporting mechanism: original task/priority/member, accepted
first/last and min/max timestamps, ordering status, observed-selected and physical
record counts, filtered/out-of-scope counts, missing/unsupported metadata,
redundant source, duplicate and conflict counts. Empty/entirely filtered sources
have no selected accepted range. These observations are not asserted archive
coverage and are not resumable byte pointers. Family physical reads are charged
once to a deterministic derived owner; selected rows are counted in their target
worker, and filtered/unroutable rows in that physical source's owner. Aggregate
physical reads are not multiplied by the number of selected symbols.

Counters add default-zero `rows_suppressed`, `rows_unpublished`,
`records_unprocessed` and `records_unrouted`. Suppression is not a parse failure.
On success normalized canonical rows equal written plus suppressed rows; on
failure normalized-but-unpublished rows remain explicit. Routed-but-unprocessed
records are distinct from rejected market records. Depth accepted events remain
separate from canonical level rows. Existing diagnostic samples remain bounded.

## Output compatibility and failure behavior

Disjoint contiguous stream output retains manifest 3. Interleaved trade ownership
or deliberate suppression, and family candidates never encountered (which produce
no empty concrete stream), use **manifest 4**, retaining canonical Parquet schemas
but adding per-source half-open `ranges` to `SourceSpan`. Bounding start/end fields
summarize those ranges; the range sum must match that task's written rows and the
whole stream's ownership must cover physical rows exactly once. Old manifest-3
spans still deserialize/validate without the field.

The current merge resolver explicitly admits only manifest 2/3. It therefore
rejects new manifest-4 datasets rather than treating bounding envelopes as
contiguous ownership. No merge/conversion algorithm was changed. Supporting
manifest-4 merge consumption is a separate compatibility follow-up; do not feed
it through old readers that assume one contiguous range per source.

Source failure stops further normalization submissions in the affected unit.
Already decoded family records retain read/selection/unprocessed context. Worker,
ordering, metadata, reconciliation, resource or writer errors prevent successful
dataset commit. Available metrics and recoverable staging remain. Independent
jobs retain definitive results through the existing scheduler/session collection.
Stream-wide errors do not invent an individual culprit; selected metadata errors
retain their known concrete/physical source context. Existing machine-readable
stdout and public failure codes remain unchanged.

## Python handoff

1. Emit prepared schema 4; keep standalone job protocol 3 separate.
2. Remove task `source_time_bounds` entirely, not as null; send no replacement
   event range or external pointer index.
3. Keep archive task order intentional: first array entry wins supported genuine
   conflicts. Do not sort the request merely to imitate chronological output.
4. Add explicit `all` or exact `include.symbols` to every family request. Concrete
   symbol tasks retain omitted selection, or include that one exact symbol.
5. Supply the existing offline namespace/catalog with authoritative historical
   IDs, tick sizes and required contract specifications. Do not fabricate futures
   entries to make `all` pass. Keep member mappings where required by supported
   include routing.
6. Treat unsupported overlap/domain/member cases and unsatisfied selections as
   failures. Honor manifest 4 explicitly in downstream consumers.

## Fixture and remaining authority blocker

The 357-source fixture now has schema 4, removes all 103 formerly present task
bounds fields, and adds `all` to 21 family tasks. A comparison against the checkout
at task start confirms source paths, task/job/dataset IDs, source-array order,
requested interval, outputs, namespace and complete instrument catalog are
otherwise identical. No workload member or historical specification was replaced.
The supplied catalog has no concrete authority for three requested OKX family
scopes: inverse perpetual `BTC-USD`, linear future `BTC-USD_UM`, and linear
perpetual `BTC-USDT` (seven source tasks each, 21 total). Its only OKX catalog
entry is spot `BTC-USDT`, which cannot substitute for those contracts. Actual
eligible contracts in those files cannot be determined without reading them,
which was not done. Resolution reports
that missing authority; no illustrative contract specs were added to the fixture.
