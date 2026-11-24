# Configuration and prepared work

A recipe describes what you want. A profile supplies source interpretation. Runtime settings determine operation policy and execution limits. Preparation resolves that combination into concrete, inspectable work.

| Layer | Responsibility |
| --- | --- |
| Forge application TOML | Workspace behavior such as storage-root resolution, HTTP settings and presentation. Global `--config` selects this layer |
| Acquisition recipe | Provider/source, requests and requested interval |
| Normalization recipe | Select registered raw inputs, resolve processing scope and ordering evidence |
| Merge recipe | Select normalized inputs, interval, ranks and timeline policy |
| Conversion recipe | Bind supported upstream intent or an exact publication and define target quote and valuation policy |
| Pipeline recipe | Coordinate acquisition, normalization and merge with retained stage handoffs |
| Normalization profile | Describe source record layout, field meanings, units and book semantics. The same interpretation responsibility applies to acquired and imported bytes |
| Engine runtime | Operation-specific processing settings, resource limits and output configuration |
| Prepared batch or job | Resolved inputs, exact identities, compatibility requirements, ordering and output destinations |
| Retained runtime and evidence | The values, provenance and fingerprints associated with a concrete preparation or submission |

## Where settings live

The default workspace configuration is under `data/.config/`. Managed workflow recipes use the relevant directories under `workflows/data_engineering/`. The Engine base files are `engine/normalize.toml`, `engine/merge.toml`, and `engine/convert.toml`. Pipeline runtime `engine/pipeline.toml` supplies stage overrides for normalization and merge. Forge validates the bases and composed stage settings, and retained pipeline state pins its stage files.

A setting can originate in a loader default, an explicit base document or a permitted override. Read the retained preparation for the effective values. For example, conversion caps recipe resource requests against base ceilings, then stores the effective resource and Parquet settings in each Rust job. The job includes its required publication and catalogue bindings. Its standalone execution uses the job's embedded settings and evidence.

## What is pinned

The Bybit example uses source-specific profiles to decode its trade and depth records. Preparation records the selected archive/member bindings and applicable metadata and snapshots the effective runtime where the operation requires it. Changing an editable file affects a later preparation. A retained job continues to use its pinned fields and snapshots, and a changed dependency can block submission. Generate a fresh preparation to intentionally change that work.

Structural validation checks the declaration's type, required fields and local constraints. A registry-backed resolution can check input identities and selection. Engine validation checks the prepared operation against its own schema and compatibility rules, including applicable input evidence. See the [configuration index](../reference/configuration/index.md), [normalization profiles](../workflows/normalization/author-profile.md), [runtime reference](../reference/runtime.md), and [prepared job references](../reference/data-formats/prepared.md).
