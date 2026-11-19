# Develop MarketForge

From the checkout, use Python 3.12 or newer, uv, and a Rust toolchain supporting edition 2024. Install contributor and research dependencies with `uv sync --project forge --group dev --group lab`. Use the [guarded testing setup](../getting-started/installation.md) for integration checks.

```bash
uv run --project forge python -m pytest forge/tests/unit/historical/archives/test_source_regressions.py
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge
```

For managed execution, build the normalizer and opt into the [isolated Forge integration suite](../examples/bring-your-own-data/databento/index.md). The Databento case needs the two JSONL exports, and the suite uses a guarded database with disposable schemas. Real-data Engine fixtures require their referenced archives and can be large.

## Documentation

```bash
python3 -m venv docs/.venv
docs/.venv/bin/python -m pip install -r docs/requirements.txt
(cd docs && .venv/bin/zensical build --strict)
(cd docs && .venv/bin/zensical serve)
python3 docs/check_links.py
```

Review the rendered pages and run the relevant executable examples. See [architecture](architecture.md) and [extension points](extending.md) before changing producer or consumer behavior.

## Engineering investigations and benchmarks

The repository's `docs/historical/` contains dated investigations and benchmark methodology, including `normalization-parameters-20261117.md`. Refer to the current runtime and contract references for operational behavior. Benchmark workloads must be explicitly supplied. Use isolated output destinations under `data/benchmarks/historical/` and keep the fixture, runtime, binary and results together when comparing runs.
