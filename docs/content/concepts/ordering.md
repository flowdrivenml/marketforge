# Ordering and causality

MarketForge carries both event time and the order needed to interpret dependent observations. The ordering used to process a file is part of its contract.

| Ordering concept | Role |
| --- | --- |
| Event time | Source timestamp used for intervals and chronological order |
| Source sequence | Feed-defined continuity within a sequence domain |
| Stream-local causal order | The order a stream needs for reconstruction, including valid source dependencies |
| Cross-stream order | How the merger interleaves available observations from different streams |
| Stream rank | Prepared binding used to break equal-time ties and correlate parts of a stream. It expresses a scoped priority |
| Timeline sequence | Physical order of causal merged events. It carries forward through supported causal conversion |
| Source provenance | Stream rank, task, observation ordinal and source row span allow a reader to relate timeline events to normalized inputs |
| Physical layout | Partition and file boundaries used for storage. A boundary can carry a validated checkpoint rather than resetting state |

## Chronological and causal merge

Chronological output orders compatible events by timestamp, with stream rank for cross-stream ties. Causal merge advances the available head of a stream and preserves that stream's dependency order. A subsequent head may carry an earlier timestamp.

For example, a depth initialization at timestamp 220 precedes a dependent update at timestamp 210 in native sequence. Causal output can write the initialization first and the update second. Sorting those rows by timestamp would apply the update before state existed. Other available streams can interleave by timestamp and rank without removing this dependency. This is why the physical timeline sequence is the replay order.

Ranks are resolved during preparation and shared by parts of the same stream under the supported contract. They can be overridden in merge recipes. A rank gives a deterministic priority, independent of market importance or a claim that one equal-time observation occurred earlier in the world.

## Context and boundaries

A causal merge can emit an earlier segment prefix to reconstruct in-range depth. Such rows carry `reconstruction_only` and count toward the publication's physical events. For that format, subtract the reconstruction count from total events for the in-range count. Inspect the field scope when comparing bounds. The merged manifest’s overall extrema can include reconstruction timestamps. Market-only membership or coverage claims require the relevant population evidence. A checkpoint helps start at a validated boundary. It leaves the required source history and ordering contract intact.

A file, partition or task boundary can fall between dependent events. Read the manifest's order and required supporting evidence. For conversion, validated checkpoints carry price state across supported partition boundaries. For depth, replay the segment initialization and applicable context or use a reader that understands the supported checkpoint. [Merge compatibility](../reference/data-formats/prepared-merge.md#merge-compatibility) · [Merged counts and layout](../reference/data-formats/publication-merge.md) · [Normalization timestamp policy](../reference/configuration/normalization-runtime.md#9-timestamp-regression)
