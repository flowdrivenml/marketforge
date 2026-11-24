# Time, selection, and coverage

An interval is a request or a scope. Observed timestamps are measurements from data. Read the origin and meaning of each timestamp before comparing it to another.

| Time | Meaning |
| --- | --- |
| Event timestamp | Source-defined time of the market event or observation |
| System timestamp | Optional additional source clock, such as capture, receive or recording time when documented by the profile |
| Source observation time | Time attached to a provider statistic or specification observation |
| Discovery or fetch time | When Forge checked a source or retrieved bytes or metadata |
| Requested interval | Time scope expressed in the recipe or job |
| Nominal archive interval | Provider-defined scope of the archive or member |
| Observed endpoints | Inclusive minimum and maximum timestamps for the measured population |
| Specification validity / assumed applicability | Extent applicable to economic terms under the stated evidence or inference contract |
| Run start and finish | Orchestration wall-clock timestamps, distinct from event time and Engine elapsed time |
| Publication or registration time | Commit or catalogue registration timing, where recorded by the corresponding evidence |

Use timezone-aware bounds. The provider defines its archive-day boundary, which may differ from a local calendar day. Offsets describe instants and can be converted to UTC. The Engine uses signed Unix nanoseconds in historical jobs. For exact comparisons preserve the integer or all timestamp digits. A recipe expressed as a datetime can have a different precision from the Engine wire value.

## Half-open requests and observed bounds

For `[2026-10-27T00:00:00Z, 2026-10-28T00:00:00Z)`, the event filter treats the boundaries as follows.

```text
2026-10-27T00:00:00.000000000Z   included
2026-10-27T23:59:59.999999999Z   included
2026-10-28T00:00:00.000000000Z   outside
```

A file with the two included events can report them as inclusive observed endpoints.

| Operation | How it uses time |
| --- | --- |
| Acquisition | Selects overlapping nominal archives within the provider contract. A selected archive may be downloaded whole |
| Normalization | Resolves selected archives or members. Tasks may read the whole physical input and produce observations beyond the requested interval |
| Merge | Selects market events in the Engine job's half-open event interval. Causal depth replay can carry an earlier segment prefix marked reconstruction-only |
| Conversion | Processes each selected complete merge publication. The recipe interval constrains selection. Forge rejects disjoint publication intervals and requires whole-publication approval when the publication scope or observed bounds extend beyond the request under its coverage checks |

## Read coverage evidence

| Observation | What it establishes |
| --- | --- |
| Supported request | The operation can attempt discovery for the stated selection |
| Successful discovery, including an empty result | The provider response was processed under the requested scope. An empty response can be a valid result |
| Nominal archive coverage | The selected advertised extents overlap or span a time range |
| Observed endpoints or periods | The measured timestamps and any recorded spans for the stated file, stream or publication |
| Sequence continuity | The available observations meet continuity checks under the specific feed and sequence domain |
| Proven missing input | Concrete evidence such as a known required part missing or a detected sequence break under the applicable contract |
| Unknown coverage | Available evidence cannot establish completeness for the interval |

A min/max envelope leaves gaps and population counts unknown. Sparse trades can legitimately have long gaps or no events in a period. A stateful book can instead require an initialization and subsequent sequence continuity. Successful archive discovery and a current instrument listing leave historical event completeness and membership unresolved.

Browse order is independent of stored event order. For example, Markets freshness sorting uses the stored measurement/window-end timestamp, while its stale indicator uses fetch time. Dataset and run views expose their own sort fields. [Markets CLI](../reference/cli/markets.md) · [Datasets CLI](../reference/cli/datasets.md) · [Ordering](ordering.md)
