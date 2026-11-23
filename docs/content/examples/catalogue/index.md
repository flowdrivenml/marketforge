# Synthetic trade and depth recordings

This bundle uses a fictional exchange and AAPL specifications. The inputs are [one CSV trade](synthetic-trades.csv) and [two JSONL book events](incremental/synthetic-depth.jsonl). The [authoring guide](../../reference/profiles.md) explains their mappings.

## Check the emitted records

Use a fresh [testing workspace and registry setup](../../getting-started/installation.md) with the managed normalizer. Run from the checkout where the files below are available. Set up the venue, instrument, and specifications, then install the validated profiles. To check a draft, use its path in the matching iteration of the loop.

```bash
example=docs/content/examples/catalogue
for entity in exchange instrument specs; do
    mf catalog "$entity" add --from "$example/$entity.toml" --testing | cpout
done
for kind in trades depth; do
    id="example_exchange.equity.equity.$kind"
    profile="$example/$id.toml"
    if [ "$kind" = depth ]; then profile="$example/incremental/$id.toml"; fi
    mf normalize profiles add "$id" --from "$profile" --testing | cpout
done
mf acquisition register plan "$example/incremental/registration.toml" --testing | cpout
mf acquisition register run "$example/incremental/registration.toml" --testing | cpout
mf --json normalize run "$example/normalization.toml" --testing | cpout
```

The JSON result gives the registered primary file IDs. Substitute each ID for `FILE_ID` to preview the records.

```bash
mf datasets head FILE_ID --limit 10 --wide --precise --testing | cpout
```

The trade preview should show a buy of 10 shares at 233.62 and event time `1730125800000000000` ns. The depth snapshot at that timestamp has a bid of 10 at 233.61 and an ask of 12 at 233.63. Apply the next event’s absolute changes to get bid 233.61 × 4 and ask 233.64 × 8, with the old ask removed. Check the [dataset inspection guide](../../workflows/datasets.md) for other ways to examine the publication.
