# Quick Start with Bybit BTC perpetual trades and depth

Use the Bybit **BTCUSDT linear USDT perpetual** for October 27, 2026 UTC. The catalogue reference is `bybit.perpetual.linear.BTCUSDT`, while acquisition takes the native symbol `BTCUSDT` with separate `perpetual` and `linear` fields. Recipes use `trades` and `order_book_l2`. Engine jobs and installed profiles use `trades` and `depth`. Dataset filters also accept `l2`.

Run from the checkout with the [guarded PostgreSQL environment, normalizer and Arrow inspection tools](installation.md). Use the installed `bybit.perpetual.linear.trades` and `bybit.perpetual.linear.depth` profiles.

## Discover and acquire both inputs

The [shared recipe](../examples/bybit/acquisition.toml) uses the half-open interval `[2026-10-27T00:00:00Z, 2026-10-28T00:00:00Z)`. Discovery selects overlapping whole daily archives. Normalization processes those selected inputs without trimming their records to the requested window. A shorter window within this day still needs both archives.

| Input | Compressed bytes |
| --- | ---: |
| `BTCUSDT2026-10-27.csv.gz` | 39,760,400 |
| `2026-10-27_BTCUSDT_ob200.data.zip` | 196,159,585 |

These sizes were checked against the Bybit archive endpoints. Allow about 225 MiB for the two compressed files, about 675 MiB for the observed publications, and additional scratch space. [Trade listing](https://public.bybit.com/trading/BTCUSDT/) · [Linear depth listing](https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/)

```bash
recipe=docs/content/examples/bybit/acquisition.toml
mkdir -p data/.config/acquisition
saved=data/.config/acquisition/bybit-btc-20261027.toml
if [ ! -e "$saved" ]; then
  cp "$recipe" "$saved"
fi
cmp "$recipe" "$saved"
mf acquisition map bybit-btc-20261027 --testing
mf acquisition plan bybit-btc-20261027 --testing --cache-only --verify-local
mf acquisition download bybit-btc-20261027 --testing
mf storage list --testing --exchange bybit --instrument-type perpetual --market linear --instrument BTCUSDT \
  --start 2026-10-27T00:00:00Z --end 2026-10-28T00:00:00Z --wide
```

If `cmp` reports a difference, stop and choose an unused recipe name before mapping or downloading. In storage, look for the two names above with complete status. For each local ID, `mf storage show ID --testing` reports its managed path and recorded evidence.

## Resolve metadata and prepare

```bash
mf catalog specs show bybit.perpetual.linear.BTCUSDT --testing
mf --json normalize prepare "$recipe" --testing > bybit-prepare.json
python - <<'PY'
import json
with open("bybit-prepare.json") as f:
    report = json.load(f)
print(report["status"], report["engine_ready"], report["batch_path"])
for job in report["job_summaries"]:
    print(job["content_type"], job["task_count"], job["stream_count"])
PY
```

Expect a ready batch with one trades job, one depth job and one task per job. In the preparation report, inspect `requests[].selected_archives[].identity_resolution.specification_evidence`. The checked registry resolved both archives with exact symbol matching and policy-retained `catalogue_inferred` evidence spanning the selected day. This is an operational applicability assumption. Historical verification requires evidence tied to that period.

**Fresh catalogue prerequisite.** If the venue, instrument or applicable specification is absent, register [applicable historical declarations](../workflows/catalogue/specifications.md) for this exact contract when you have evidence. The built-in [catalogue inference workflow](../configuration/markets.md#historical-metadata-inference) currently runs through `mf init --testing` after acquisition and also collects broad current-market metadata and publishes rankings. Forge exposes no scoped inference command for just this instrument. Rankings themselves are unnecessary for this selection.

## Execute and inspect

```bash
batch=$(python -c 'import json; print(json.load(open("bybit-prepare.json"))["batch_path"])')
mf --json normalize run "$batch" --testing > bybit-result.json
python - <<'PY' > bybit-file-ids.txt
import json
with open("bybit-result.json") as f:
    result = json.load(f)
assert result["status"] == "complete", result["status"]
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

The file IDs come from the testing registry. Each job can publish several parts under one shared manifest. Check that the result contains both trade and depth registrations, then inspect the shared manifests for processing counters and quality. Source records, accepted book observations and canonical level rows count different things. Use the depth event index and boundary records in the manifest to identify row spans, including zero-row observations, and initialization points. A preview shows stored level rows. [Publication formats](../reference/publications.md) describes the additional evidence needed for reconstruction.

Distinguish the requested day, the raw archives’ nominal coverage and the dataset’s observed bounds. A nominally gap-free selection does not establish event completeness or continuous book state.

## Compare with the checked result

Forge reported `status = complete`, `verification = verified` and `registration = registered` in a guarded registry clone. Both manifests reported clean integrity and zero rejected source records.

| Content | Source records | Canonical rows | File IDs in the checked run | Observed UTC bounds |
| --- | ---: | ---: | --- | --- |
| Trades | 1,074,411 | 1,074,411 | 277 | Oct 27 00:00:00.093200 to 23:59:58.827 |
| Depth | 863,402 | 51,150,256 | 278–283 | Oct 27 00:00:00.493 to Oct 28 00:00:00.393 |

IDs and part counts depend on the workspace and runtime. The trade preview began with a sell of 0.001 BTC at 80,114.90. The depth preview began with a bid of 0.257 BTC at 80,076.50 from the initial snapshot. Both schemas have an `int64` nanosecond event timestamp and exact decimal price and quantity fields. Trades carry a trade ID and depth carries nullable order counts.

Run the [depth inspection script](../examples/bybit/inspect_depth.py) against your result:

```bash
python docs/content/examples/bybit/inspect_depth.py bybit-result.json
```

It uses the first index entry to read the snapshot's 400 rows and compare the reconstructed bids and asks with the initial boundary state. The checked event index contains 863,402 observations. This check covers the first state. The publication reports valid native reconstruction and `external_historical_coverage_established = false`. In particular, the final observed depth event extends 393 ms past the requested end.

## Diagnose a failure

- For unavailable archives, check the map report and the two provider listings. A cache-only plan with an unresolved selection cannot establish unavailability.
- For missing or conflicting specifications, inspect the preparation report and catalogue evidence. Resolve evidence applicability before execution.
- For a nonzero run, inspect its execution result and the Engine diagnostics before retrying. If Engine committed files and registry registration failed, use [registration-only recovery](../reference/cli/normalize.md).
- An unknown acquisition feed label appeared as a warning on the checked depth archive. Inspect the chosen profile and integrity results for snapshot and update interpretation.

Continue with [dataset inspection](../workflows/datasets.md) or [register your own recording](../workflows/acquisition/external-data.md).
