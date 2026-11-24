# Run MarketForge Live

## Launch and inspect

The default Live configuration directory and publication socket paths beginning with `data/` resolve from the MarketForge workspace. Live uses `MARKETFORGE_PROJECT_ROOT` when set and otherwise discovers the enclosing checkout. Relative `--config-dir` and `--general` arguments are resolved from the invocation directory.

From the repository root, build and perform static offline validation for the example Binance profile (the quota below must be replaced with a verified deployment quota):

```sh
cargo build --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features live --bin marketforge-live
engine/target/debug/marketforge-live --validate-only \
  --config-dir data/.config/live --general examples/engine/live/general.toml \
  --profiles binance_spot --rest-weight binance_spot=1200/60
```

Validation checks static configuration/formats/limiter/WS compilation only; it deliberately says persisted definitions were **not loaded**, and performs no DB/network access. To run the restored graph, export the database variable securely and then:

```sh
install -d -m 700 data/.live
engine/target/debug/marketforge-live \
  --config-dir data/.config/live --general examples/engine/live/general.toml \
  --rest-weight binance_spot=1200/60
```

In another terminal:

```sh
curl --fail http://127.0.0.1:9091/metrics
cargo run --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features stream-client --example stream-consumer -- \
  data/.live/marketforge-data.sock EXISTING_STREAM_ID
```

The socket parent must satisfy the existing Unix server's ownership/permission checks; inspect `publication.socket_path` in the example and create a private directory before launch. `--profiles` only restricts static/empty diagnostics; normal profiles come from enabled persisted Sources. Give all selected weighted profiles explicit quotas. `--empty-definitions` is a deliberate empty resolved-graph diagnostics mode: it starts the same application/export/Unix services without restoring a DB graph or acquiring exchange demand. It is useful for signal/binding checks, not a persisted-data mode or alternate backend. Logging is structured stderr at the existing configured level; lifecycle logs include stream/generation/reason, not each market event.

See [Live configuration](../../reference/live/configuration.md), [administration](management.md), [stream consumption](consumption.md) and [runtime architecture](../../development/live/architecture.md).
