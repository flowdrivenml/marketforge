# Merge a compatible trade file

Complete the [synthetic normalization example](index.md) first and install the current managed merger with `mf engine build merge`. Run from the checkout in the same guarded registry. The example selects the single trade file and a one-nanosecond interval containing its execution. Check the [layout restrictions](../../workflows/merge.md#supported-layouts) before adapting the recipe to other files.

```bash
python - <<'PY'
import json
from pathlib import Path

result = json.loads(Path("synthetic-result.json").read_text())
assert result["status"] == "complete"
for job in result["jobs"]:
    manifest = json.loads(Path(job["manifest_path"]).read_text())
    if manifest["content_type"] == "trades":
        (file_id,) = job["files"].values()
        break
else:
    raise SystemExit("Missing synthetic trade publication.")

with Path("synthetic-merge.toml").open("x") as recipe:
    recipe.write(
        f"dataset_ids = [{file_id}]\n"
        "start_ns = 1730125800000000000\n"
        "end_ns = 1730125800000000001\n"
    )
PY
mf merge validate ./synthetic-merge.toml
mf merge prepare ./synthetic-merge.toml --testing --dry-run | cpout
mf --json merge run ./synthetic-merge.toml --testing > synthetic-merge-result.json
python - <<'PY'
import json
with open("synthetic-merge-result.json") as f:
    result = json.load(f)
assert result["status"] == "complete", result.get("error")
print(*result["catalogue_dataset_ids"].values(), sep="\n")
PY
```

This fixture expects one registered output file. Use a fresh isolated fixture workspace and registry when exercising the example. Use the printed ID with `mf datasets head ID --testing` and compare the trade with the original CSV. Run `mf datasets lineage ID --testing --publication` to see the selected input. The example creates the recipe exclusively. To use it again, review the existing recipe and reuse it if its file ID is still correct.

If prepare reports a job parse error after a code update, check [managed Engine compatibility](../../reference/configuration/merge-runtime.md#managed-engine-troubleshooting) and retry persisted preparation. If the engine reports no active stream, check the selected interval and stream coverage. The example's bounds contain exactly the supplied trade timestamp.
