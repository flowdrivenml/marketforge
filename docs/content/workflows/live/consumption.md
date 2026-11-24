# Consume Live market data

## Public entry points

`live::streaming::transport::unix::UnixStreamingServer::bind(Arc<StreamRouter>, PublicationConfig)`
returns an owned server; `shutdown().await` joins its connections and removes only
its own socket inode. Bind it to the application's existing Router, after loading
`GeneralConfig` through the existing loader. The transport never starts, creates,
or reconfigures streams. The headless `marketforge-live` executable now uses the owned
[LiveApplication](../../development/live/architecture.md), which drives demand and lifecycle services.
The streaming protocol itself remains read-only.

`live::streaming::client::MarketForgeStreamClient` requires `stream-client`, not `live`
or `postgres`. `connect_unix(path, ClientOptions)` negotiates v1; `list_streams`,
`describe_stream`, `subscribe`, `ping`, and `close` are asynchronous. A Subscription
has cancellation-safe `recv()` and deterministic `close()`. Clone the client for
concurrent requests. All subscriptions share one connection and its writer.

Run the consumer against an application embedding the server:

```sh
cargo run --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features stream-client --example stream-consumer -- /private/path/data.sock stream_id
```

`stream_transport` is a runnable integration target exercising the production
server and client over local sockets with actual Router, SourceProcessor,
production Merge, and production Convert. Exchange input is normalized test data.
No historical dataset is read.

See the [streaming protocol](../../reference/live/streaming-protocol.md) and the [Rust consumer example](https://github.com/flowdrivenml/marketforge/blob/main/examples/engine/live/stream-consumer.rs).
