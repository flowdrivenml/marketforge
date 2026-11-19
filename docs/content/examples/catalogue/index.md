# Synthetic trade and depth recordings

This bundle uses a fictional exchange and fictional AAPL specifications. Run from the checkout with the [testing setup](../../getting-started/installation.md). The input files are [one CSV execution](synthetic-trades.csv) and [one JSONL book observation](synthetic-depth.jsonl).

1. Install the [venue](exchange.toml), [instrument](instrument.toml) and [specifications](specs.toml).
2. Validate and install the profiles using the [profile workflow](../../workflows/catalogue/profiles.md).
3. Register both files with the [registration recipe](registration.toml) and use the [normalization recipe](normalization.toml).

```bash
example=docs/content/examples/catalogue
for entity in exchange instrument specs; do
    mf catalog "$entity" add --from "$example/$entity.toml" --testing
done
for kind in trades depth; do
    id="example_exchange.equity.equity.$kind"
    mf acquisition profiles validate --file "$example/$id.toml"
    mf acquisition profiles add "$id" --from "$example/$id.toml" --testing
done
mf acquisition register plan "$example/registration.toml" --testing
mf acquisition register run "$example/registration.toml" --testing
mf --json normalize run "$example/normalization.toml" --testing > synthetic-result.json
```

Inspect each `catalogue_dataset_id` in the result's `registrations` using [dataset inspection](../../workflows/datasets.md). Expect a buy of 10 shares at 233.62 with event time 1730125800000000000, and one initialized book observation at that time containing a bid of 10 at 233.61 and ask of 12 at 233.63. These expectations come directly from the supplied source files.
