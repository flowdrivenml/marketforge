# Developer overview

Forge is the Python orchestration layer. It owns request selection, catalogue and registry access, managed storage, job preparation, and run evidence. The Rust Engine owns source interpretation, event processing, and operation-specific publication. Changes to a public contract must account for its writer, validator, and readers.

## Locate the change

| Change | Where to start |
| --- | --- |
| CLI, TUI, registry, acquisition, preparation, or recovery | [Forge architecture](forge.md) |
| Source decoding, canonical values, reconciliation, or depth reconstruction | [Normalization Engine](normalize.md) |
| Timeline ordering or materialization across publications | [Merge Engine](merge.md) |
| Currency references or conversion output | [Conversion Engine](convert.md) |
| A stored job, manifest, or registry contract | [Orchestration and persistence](orchestration.md) and [compatibility](compatibility.md) |

The [system overview](architecture.md) traces the boundaries between these components. The [public reference](../reference/index.md) defines the user-visible contracts they implement.

## Set up and verify a change

Use [local development setup](setup.md) and the [first contribution walkthrough](first-contribution.md) to check your environment. Then choose an [extension point](extending.md) and keep a representative fixture close to the code it exercises. The [Bybit listing walkthrough](sources.md#walkthrough-accept-a-revised-bybit-directory-listing) shows how to carry one provider change through discovery, validation, tests, and documentation.

Use the [testing guide](testing.md) to choose fixture and database coverage. For a failure, [trace retained evidence](debugging.md) before [measuring performance](performance.md). Review [compatibility and contribution checks](compatibility.md) before handing off a change.
