# Extending MarketForge

> **Documentation draft.** This page outlines future contributor guides. It does not prescribe an unverified implementation recipe.

## Add a source or external format

A guide should cover source discovery and acquisition boundaries, explicit input identity, compatible format definitions, instrument metadata, and small representative fixtures. External-file import tooling can inspect and draft format descriptions; drafts still need semantic review and Engine compatibility checks. Arbitrary formats are not automatically supported.

## Add a processing operation

A guide should establish the Engine interface, configuration contract, canonical input/output semantics, resource behavior, and focused correctness tests. Forge planning and registration changes should follow the relevant boundary rather than duplicate processing logic.

**Implementation status not assessed:** general extension readiness and the full path for a new integration. **Planned documentation:** verified examples for adding a source and an operation. These documentation gaps do not imply existing integrations are unimplemented.
