# Create a depth profile

Use the [synthetic bundle](../../examples/catalogue/index.md) from the checkout with the [testing prerequisites](../../getting-started/installation.md). The [source observation](../../examples/catalogue/synthetic-depth.jsonl) is one JSON object with timestamp, bids and asks.

```bash
python -m json.tool docs/content/examples/catalogue/synthetic-depth.jsonl
```

1. Inspect the [depth profile](../../examples/catalogue/example_exchange.equity.equity.depth.toml). Each side is an array of price/quantity pairs. The declared order is descending for bids and ascending for asks. The source supplies no order count or sequence. Declare those fields only when the source provides them.
2. Select a snapshot rule and map the timestamp in nanoseconds. Declare one supplied level per side. The scope is bounded to those levels. For incremental sources, map snapshots and updates separately, preserve native source order and establish whether quantities replace existing levels, whether zero removes a level and which sequence domain can reset. [Depth mapping reference](../../reference/profiles.md#snapshot-and-update-semantics)
3. Run the [synthetic bundle commands](../../examples/catalogue/index.md) to validate, install, register and normalize.
4. Inspect the depth publication. The supplied snapshot has bid 233.61 × 10 and ask 233.63 × 12. Use its observation span and initialized boundary along with the level rows to compare the state with the source. One observed state cannot establish continuity.

If a level disappears, inspect side mappings and zero-quantity handling. If initialization fails, check the snapshot filter and ordering before inspecting updates. [Troubleshoot profiles](profile-troubleshooting.md)
