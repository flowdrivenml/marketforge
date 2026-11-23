# Trade profile sample checks

Start with the [complete trade profile](../../reference/profiles.md#2-create-a-trade-profile) and run the [recording check](../../examples/catalogue/index.md). Compare the first record in the registered dataset with the source row. The dataset preview can expose a reversed side mapping or a quantity that was interpreted in the wrong units even when compilation passed.

For a signed feed, include one buy and one sell in the sample. Verify the decoded side and quantity independently. If the source provides a native trade ID, include two different trades with the same timestamp in the check so you can confirm that identity comes from the native field. [Side transforms](../../reference/profile-fields.md#normalizationsidetransform) · [Native trade ID scope](../../reference/profile-fields.md#sourcetrade_id_scope)
