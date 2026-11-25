# Convert historical data

Use Conversion to value historical trades and order-book changes in a target quote currency. The output keeps the original market observations and appends target values and valuation evidence. See [historical quote conversion](../concepts/conversion.md) for how the Engine computes those values.

## Before you start

Set up a [workspace and registry](../getting-started/installation.md) and install the managed converter with `mf engine build convert`. Use the same registry for discovery, preparation, execution, and inspection. This walkthrough uses testing and an existing verified Bybit BTCUSDT perpetual, linear, combined trades-and-depth merge. If you need that upstream output, follow [Merge normalized data](merge.md).

## 1. Choose a source publication

List recent merges in testing and inspect a suitable publication.

```bash
mf datasets list --testing --operation merge --view publications --sort time --order desc
mf datasets show --publication-id PUBLICATION_ID --testing
```

Replace `PUBLICATION_ID` with the positive numeric **registry publication ID**. File IDs, Engine publication IDs, Forge run IDs, recipe names, and paths refer to different objects. Check the layout, instrument and coverage. Conversion operates on full committed publications, including any reconstruction context.

## 2. Create and inspect a recipe

In **Workflows → Conversion → New recipe**, choose the publication route to
find a registered merged publication by its exact identity. Use the saved
recipe's **Edit** action for the conversion policy and internal reference
trades. For an external reference, use **Browse external trade references**.
That action checks the selected source mode and uses the registered trade
streams of the chosen reference publication.

```bash
mf convert new getting_started_usdt \
  --from-publication PUBLICATION_ID --target-quote USDT --testing
mf convert show getting_started_usdt
```

This pins the exact merge, its registry and committed interval. In an interactive terminal, creation opens the editor. Accept the generated values for this identity example. The recipe is stored under `data/.config/workflows/data_engineering/conversion/`.

| Source option | Input identity | When to use it |
| --- | --- | --- |
| `--from-publication` | Exact merged registry publication | Use an existing committed merge independently of its producing recipe |
| `--from-merge` | Merge recipe name or TOML path | Discover committed outputs related to a managed merge |
| `--from-normalize` | Normalization recipe name or TOML path | Find merges through retained normalization requests and lineage |
| `--from-acquisition` | Acquisition recipe name or TOML path | Find merges through registered raw contributions and normalized lineage |

For example, `mf convert new from_merge --from-merge getting_started --target-quote USDT --testing` fingerprints the upstream TOML. Recipe-based creation is local. All four routes ultimately require eligible committed merges. Run the missing upstream stages separately if preparation cannot discover their outputs. For competing revisions, pin the intended publication, or use a direct binding. See [recipe selectors](../reference/configuration/conversion.md).

## 3. Review reference trades

The example values USDT in USDT at rate 1. Automatic selection discovers
observed trade streams from the committed publication and uses their historical
base and quote currencies to find potential routes. An identity valuation needs
no trade references for a trades or combined source. When there is only one
eligible route, preparation can proceed without ranking evidence.

Use **Edit conversion policy and references** to switch to Manual and search
for eligible trade streams, or use `mf convert edit getting_started_usdt` to
supply exact IDs. To configure external candidates, select a specific
trade-only merge permitted by the source layout. Combined publications give
internal trades precedence. See [reference eligibility and layout rules](../concepts/conversion.md#how-references-become-eligible)
and [selection settings](../reference/configuration/conversion.md#reference-selection).

## 4. Validate and preview

```bash
mf convert validate getting_started_usdt
mf convert prepare getting_started_usdt --testing --dry-run
```

Validation checks the TOML locally. Preparation resolves the guarded registry, full inputs, historical instrument metadata, references and runtime settings. A dry run can show planned Rust job paths. It creates no prepared jobs and skips Rust validation of the proposed jobs. Reuse checks can validate an earlier retained job. To retain and validate a job before running it, use `mf convert prepare getting_started_usdt --testing`. Inspect the selected references, potential routes, ranking evidence, and validation in the preparation report. A missing route or an ambiguous primary route blocks preparation with the affected currency and reference candidates. See [prepared conversion jobs](../reference/data-formats/prepared-conversion.md).

## 5. Run conversion

```bash
mf convert run getting_started_usdt --testing
```

For the same operation with an interactive progress monitor, use `mf convert run getting_started_usdt --testing --tui`. A run by recipe handles preparation and can reuse verified completed work. To run a previously retained submission, pass the saved collection or individual retained job path. The [CLI reference](../reference/cli/convert.md) explains their requirements.

## 6. Inspect the result

```bash
mf runs list --testing --operation convert
mf runs show RUN_ID --testing
mf runs report RUN_ID --testing
mf datasets list --testing --view publications --operation convert
mf datasets show --output-ref OUTPUT_REF --testing
mf datasets schema --output-ref OUTPUT_REF --part 1 --testing
mf datasets verify --output-ref OUTPUT_REF --testing
```

Copy the Forge run ID from Runs and the full indexed output reference from execution output or Datasets. The processed input has a registry publication ID. The converted result has a durable output reference and a manifest with its conversion job ID and physical inventory. Forge verifies and indexes conversion outputs without catalogue registration. For this identity case, expect identity valuations on selected BTCUSDT observations. Review status totals and the verified file inventory before consumption. See [converted outputs](../reference/data-formats/publication-converted.md), [Runs](inspection/runs.md), and [Datasets](inspection/datasets.md).

For blocked preparation, unavailable valuations, or interrupted execution, see [Conversion troubleshooting](../maintenance/conversion.md).

<a id="recorded-case-bybit-combined-merge-to-usdt"></a>
<a id="verify-the-portable-result-without-a-terminal-ui"></a>

Use the [portable conversion fixture](../development/testing.md#conversion-verification) to verify standalone execution with synthetic data.
