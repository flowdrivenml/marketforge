# First contribution

Strengthen an existing Bybit archive discovery test so it checks which instrument Forge associates with each returned archive. This is a test-only exercise. Use the Python environment from [Local development](setup.md). The saved provider pages and mock transport make the exercise independent of PostgreSQL and live HTTP.

1. Locate the implementation. The test calls `ExchangeSource("bybit", http).discover(...)` in `forge/historical/archives/sources/exchanges.py`. The Bybit branch checks the page and link scope, then constructs `DiscoveredArchive` records. An archive includes the source request as well as its nominal time interval and object identity.

2. Read `test_observed_bybit_directory_variants` and the nearby negative-page test. The existing positive test checks that spot, linear and inverse fixtures return three distinct archive identities inside the interval. The negative test covers denied listings, incorrect scope and unsafe links. The positive test can also assert that each record preserves the exact requested scope.

3. Run the positive and negative cases before editing.

    ```bash
    uv run --project forge --locked --group dev pytest -c forge/pyproject.toml \
      forge/tests/unit/historical/archives/test_source_regressions.py -k 'observed_bybit or bybit_negative'
    ```

4. Edit the existing test. Give the request a name, pass that object to `discover`, and add the assertion after constructing `results`.

    ```bash
    nvim forge/tests/unit/historical/archives/test_source_regressions.py
    ```

    Replace the existing `async with open_http_client(...)` block with the following. The snippet starts at the test body indentation. Keep the preceding fixture read and the remaining assertions.

    ```python
        request = selection("bybit", market)
        async with open_http_client(
            limiter=ImmediateLimiter(1),
            transport=httpx.MockTransport(lambda _: httpx.Response(200, text=page)),
        ) as http:
            source = ExchangeSource("bybit", http)
            results = [archive async for archive in source.discover(request, interval)]
        assert all(archive.request.scope == request.scope for archive in results)
    ```

    Keep the existing count assertion. Without it, the scope assertion would pass for an empty result. The expected scope comes from the selection supplied to the test. A different directory or market rendering must still produce a record owned by the same requested instrument.

5. Rerun the selection and check the edited file using the repository tooling.

    ```bash
    uv run --project forge --locked --group dev ruff check \
      forge/tests/unit/historical/archives/test_source_regressions.py
    uv run --project forge --locked --group dev ruff format --check \
      forge/tests/unit/historical/archives/test_source_regressions.py
    git diff -- forge/tests/unit/historical/archives/test_source_regressions.py
    ```

6. Review the impact beyond the assertion. `archive_scope_document` and `archive_scope_key` serialize scope for storage and later source matching. If a real adapter change alters the scope emitted by discovery, review the [acquisition configuration](../configuration/acquisition.md) and [historical acquisition guide](../workflows/acquisition/historical-data.md), and document the new behavior there. In this test-only exercise the public behavior is established by the existing contract, so the production example needs no edit.

A contribution description can say that the Bybit fixture tests now check returned archive scope as well as count, uniqueness and bounds, with the positive and negative mocked cases verified. State that the check covers adapter output and leave live availability and download verification to their respective tests. [Adapter source](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/archives/sources/exchanges.py) · [Fixture tests](https://github.com/flowdrivenml/marketforge/blob/main/forge/tests/unit/historical/archives/test_source_regressions.py)
