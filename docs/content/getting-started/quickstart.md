# First dataset: Bybit BTCUSDT trades and depth

Acquire one UTC day of trades and L2 depth for Bybit’s BTCUSDT linear USDT perpetual. Normalize it into canonical Parquet, verify the registered primary files, then check the first reconstructed book state against its retained boundary.

## Prepare the workspace

From the checkout, run `./install.sh --dev --skip-market-init` to set up the managed Engine, Arrow, and a migrated testing registry. If you already have a standard installation and want to add only the prerequisites for this guide, follow [manual testing setup and inspection tools](installation.md#prepare-the-testing-registry). Keep that registry selected with `--testing` throughout this example. Use a workspace with space for both compressed archives, normalized Parquet, and Engine scratch files.

The installed profiles must match this venue and contract. Validate them before downloading:

```bash
mf normalize profiles validate bybit.perpetual.linear.trades
mf normalize profiles validate bybit.perpetual.linear.depth
```

The catalogue identifies this instrument as `bybit.perpetual.linear.BTCUSDT`. The acquisition recipe uses the native symbol with separate venue, instrument type and market category fields. It requests `trades` and `order_book_l2`; the Engine depth profile ends in `.depth`.

<a id="discover-and-acquire-both-inputs"></a>

## Plan and acquire a day

Use the [Bybit recipe](../examples/bybit/acquisition.toml) for the half-open UTC interval `[2026-10-27T00:00:00Z, 2026-10-28T00:00:00Z)`. Discovery selects whole overlapping daily archives. A shorter interval inside this day can still select both complete archives. Normalization processes the selected inputs without trimming their records to the request.

```bash
recipe=docs/content/examples/bybit/acquisition.toml
library=data/.config/workflows/data_engineering/acquisition
name=bybit-btc-20261027
mkdir -p "$library"
if [ ! -e "$library/$name.toml" ]; then
  cp "$recipe" "$library/$name.toml"
fi
cmp "$recipe" "$library/$name.toml"
```

If `cmp` reports a difference, choose an unused name and repeat the copy before proceeding.

```bash
mf acquisition map "$name" --testing
mf acquisition plan "$name" --testing --cache-only
```

Map contacts Bybit for discovery and records the result without downloading archive bodies. The plan reads registered discovery and local inventory. Check that the two requests resolve to the expected candidates before downloading:

| Request | Expected archive |
| --- | --- |
| Trades | `BTCUSDT2026-10-27.csv.gz` |
| L2 depth | `2026-10-27_BTCUSDT_ob200.data.zip` |

```bash
mf acquisition download "$name" --testing
mf storage list --testing --exchange bybit --instrument-type perpetual \
  --market linear --instrument BTCUSDT --status complete --wide \
  --start 2026-10-27T00:00:00Z --end 2026-10-28T00:00:00Z
```

Download registers verified managed copies. Check for both filenames in storage. Use a returned local ID with `mf storage show LOCAL_ID --testing` to inspect the managed path and checksum. The [historical acquisition guide](../workflows/acquisition/historical-data.md) explains discovery and retries. The [storage guide](../workflows/acquisition/managing-acquisitions.md) explains local records and verification.

<a id="resolve-metadata-and-prepare"></a>

## Resolve specification evidence

Preparation needs a catalogue entry and instrument specification evidence applicable to the selected archives. A fresh, dedicated testing workspace can use Forge initialization after acquisition so enrichment can consider the newly registered files:

```bash
mf --json init --testing > bybit-init.json
mf markets show bybit.linear.BTCUSDT --testing
mf catalog specs show bybit.perpetual.linear.BTCUSDT --testing
```

Initialization collects market metadata from providers, attempts historical metadata enrichment, and publishes standard rankings. Review the recorded stages even if ranking fails after catalogue collection. Use [catalogue declarations](../workflows/catalogue/specifications.md) if you have verified historical terms to register. An inferred applicability decision is a reproducible policy decision. It does not establish that the exchange’s terms were historically verified. Read [identity and historical applicability](../concepts/identity.md) for that distinction.

<a id="execute-and-inspect"></a>

## Prepare and run normalization

```bash
mf normalize prepare "$recipe" --testing --dry-run | cpout
```

Look for a prepared plan with separate trades and depth jobs. Resolve any reported blockers before execution. The dry run previews the plan and prospective paths. The execution command prepares and validates its own batch.

```bash
mf --json normalize execute "$recipe" --testing > bybit-result.json
```

Forge retains the submitted batch and execution evidence. A complete result with `verification = "verified"` and `registration = "registered"` records the verified normalized publications and their registered file IDs. Use those IDs in the same database environment.

<a id="compare-with-the-checked-result"></a>

## Verify both outputs

```bash
python - <<'PY' > bybit-file-ids.txt
import json
from pathlib import Path

result = json.loads(Path("bybit-result.json").read_text())
assert result["status"] == "complete", result["status"]
assert result["verification"] == "verified", result["verification"]
assert result["registration"] == "registered", result["registration"]
assert result["registrations"], "No primary files were registered"
for item in result["registrations"]:
    print(item["catalogue_dataset_id"])
PY
while IFS= read -r id; do
  mf datasets show "$id" --testing
  mf datasets schema "$id" --testing
  mf datasets head "$id" --testing --limit 3 --precise
  mf datasets streams "$id" --testing
  mf datasets verify "$id" --testing
done < bybit-file-ids.txt
```

Look for both content types and inspect the manifests referenced by the result. Several primary files can share one publication manifest. Trades carry a trade ID and exact decimal values. Depth rows describe level changes, while the event index maps book observations to row spans. One observation can emit many levels or zero rows, so source record counts, indexed observation counts, and emitted row counts can differ.

Run the [depth inspection script](../examples/bybit/inspect_depth.py) on this example’s report:

```bash
python docs/content/examples/bybit/inspect_depth.py bybit-result.json
```

On a retained run for this recipe, the script matched 400 initial levels to the saved boundary. Your actual row counts and file IDs come from your own run report. It establishes the initial book state for this publication. Continuous reconstruction and external historical coverage require additional [depth evidence](../concepts/depth.md). Compare the requested interval, reported nominal source coverage, and observed output bounds separately.

<a id="diagnose-a-failure"></a>

## Recover and continue

- If discovery fails, inspect the map report and provider availability before retrying. A cache miss alone cannot establish archive unavailability.
- If preparation is blocked, follow the selected archive’s diagnostics for missing inputs, profiles, or applicable specifications.
- If the Engine committed output but registration failed, retain the execution intent and retry [registration only](../reference/cli/normalize.md) against the original registry.

Continue with [normalize historical data](../workflows/normalization/index.md) to adapt this workflow or [inspect datasets](../workflows/inspection/datasets.md) to work with the output.

To value a verified merged output in a target quote, continue with [Convert historical data](../workflows/conversion.md).
