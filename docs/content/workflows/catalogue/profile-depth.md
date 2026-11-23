# Depth profile sample checks

Start with the [complete depth profile](../../reference/profiles.md#3-create-a-depth-profile) and run the [recording check](../../examples/catalogue/index.md). Compare the starting levels with the snapshot. Apply the emitted update rows in order and check that replacement, deletion, and insertion produce the expected final levels.

When working with a bounded observation, compare both sides as recorded at one instant and check the observation span in the publication evidence. For a sequenced feed, add a fixture that exercises a real ordering boundary such as a documented reset or the beginning of a new range. [Bounded observations](../../reference/profile-fields.md#normalizationsnapshotlevels_per_side) · [Sequence policies](../../reference/profile-fields.md#sequence-and-book-metadata)
