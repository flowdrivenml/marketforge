# Live startup and shutdown

## Limits, startup and shutdown

See the [Live configuration reference](../../reference/live/configuration.md) for application limits.

The graph is immutable for this owner lifetime. Derived definitions require `runtime.processing`; without it a Source-only application uses one default idle ProcessingService/pool for a uniform shutdown owner. Router subscription capacity is preflighted from configured external slots plus declared dependency inputs, capped at 16384. Producer count is capped by max_definitions. Manual demand registration is bounded at 4096 owners; the shared DemandRegistry has a hard 65536-entry ceiling. Existing processing, publication, queue, frame and deadline settings retain their meanings. REST recovery currently uses fixed bounded limits: 128 commands/requests, 64 jobs, 8 concurrent jobs; REST has 8 concurrent requests and a 4 MiB response bound.

Graph, schemas, Source subscription parameters, shared limiter identity, recording support, and processing capacities are checked before acquisition. Binding failures roll back previously bound services and owners. Native catalog parameters not derivable from definitions/configuration (for example an ambiguous exchange-specific market argument) cause preflight failure; the application does not guess them. `StreamDefinitionRepository::snapshot_bounded` reads definitions/dependencies in one read-only repeatable-read transaction, with LIMIT+1 checks. Restored runtime state is fresh/stopped, with no persisted sockets, demand or authorities. The executable bounds database connect and coherent restore separately by `runtime.shutdown_timeout_seconds`; errors redact connection/query details.

Shutdown stops admissions/reconciliation, closes Unix sessions, retires processing and Source ownership, then shuts down WebSocket/recovery infrastructure. CPU retirement is awaited asynchronously by existing owners, never joined on an async executor thread. Executing CPU code is not forcibly cancellable. Final collection occurs after cleanup and before exporter retirement. Handles can read the retained final snapshot/report. `ShutdownReport.complete` requires completed cleanup without errors. `discarded_publications` counts retained work disposed in the final report drain, not records committed or earlier retired work. Errors are capped at 64 entries of 512 bytes.

A timed-out `shutdown()` returns `complete=false, cleanup_continues=true`, increments the incomplete-wait counter, and retains the task; another wait can observe completion. Drop requests owned cleanup without blocking. The executable exits nonzero for incomplete/fatal shutdown; its Tokio runtime has a one-second shutdown wait, so process exit may abandon still-incomplete work. This is reported, not described as a successful drain. SIGINT and SIGTERM follow the same explicit protocol.

## WebSocket subscription ownership

The manager admits a bounded number of attachment and detachment mutations
and owns their tasks through completion. A successful attach reply carries a
binding that the caller claims before returning. If the caller cancels
before claiming it, the task detaches the binding, even if the exchange has
already acknowledged it. Detachment continues after caller cancellation.
Shutdown closes mutation admission, drains admitted operations, then stops
the slots. Cancelling the shutdown waiter leaves the owned shutdown running.

A slot publishes its latest desired subscriptions through a watch channel.
The physical session compares that projection with acknowledged native
subscriptions and keeps one transaction in flight through ACK completion.
The callback-only embedding path retains its 25 ms bounded poll. Managed
slots wake on demand changes and coalesce intermediate revisions. Each
pending wire outcome belongs to its original transaction and physical
generation. Gate.io refreshes initial subscription timestamps at command
admission while retaining the request ID and ACK identity.

Detach revokes local delivery before updating demand. Pending native
unsubscribe holds capacity until acknowledgment or physical generation
revocation. The event loop retains the retired selector to discard matching
late frames. After publishing removal, the manager records the last
generation that could have read old demand. Once its fence loses authority,
the event loop removes the selector before clearing the manager's
allocation exclusion. The history is bounded. Reattachment to a matching
identity uses a different slot while the selector remains unsafe to reuse.

Correlated native errors, ambiguous or missing ACKs, uncertain sends, and
rejected unsubscribes recycle the physical generation. The reviewed public
market protocols expose correlation and failure fields. The supported
profiles lack a verified correlated rejection class that establishes
non-application. For example, Binance documents parser errors without a
request ID, and Gate documents duplicate-subscription errors that can
coexist with an active subscription. Definitive isolation would require
a verified no-mutation error class or authoritative native inventory.
[Binance Spot](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#error-messages)
[Binance USD-M](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)
[Bybit](https://bybit-exchange.github.io/docs/v5/ws/connect)
[OKX](https://app.okx.com/docs-v5/en/)
[Bitget](https://www.bitget.com/docs/uta/error-code/websocket)
[Gate](https://www.gate.com/docs/developers/futures/ws/)

Loopback regressions cover unclaimed bindings after native ACK, cancelled
retirement and shutdown, selector reuse across successive generations,
Bybit and OKX shared-slot mutations, rejected unsubscribe, and serialized
restoration while heartbeat and demand changes remain active. Subscription
restoration establishes transport acquisition authority. The existing
Source lifecycle owns financial recovery.

### WebSocket acceptance verification

A compatible attachment can share the survivor's physical connection. Its
own acquisition starts after native acknowledgment. Surviving subscriptions
keep their physical stamp and acquisition generation across ordinary add
and remove operations. An atomic duplicate-selector rejection leaves the
existing slot intact. A failure after routing ownership becomes uncertain
can retire the slot. Final detachment releases it.

Offline acceptance on the reconciliation branch passed 3,003 Rust tests,
with 23 intentionally ignored and zero failures. All 95 existing fixture
files were imported from a prepared worktree, and all eight fixture script
tests passed. Captured trades and depth cover all 15 configured profiles.
Source preparation and managed loopback tests cover Gate.io Linear USDT and
USD1 routes and Bitget Linear USDT-FUTURES and USDC-FUTURES product types.
The alternate settlement inputs in those tests are synthetic routing inputs.
Loopback runs also verify Gate.io's decimal-size handshake header across
reconnect, dynamic shared-slot acquisition, and conservative fallback after
an uncertain unsubscribe.

Public WebSocket acceptance requires explicit opt-in and coordination with
other workloads on the same egress. Run the 30-case baseline sequentially
under one shared limiter. The report counts physical upgrades, internally
correlated native ACKs, and validated data separately. The 30-second
observation window starts after confirmation. A case is **PASS** after
matching data and clean shutdown, or **IDLE** after a healthy, confirmed
subscription sees no matching observations during that window and shuts
down cleanly. A disconnect, invalid or unrelated frame, ACK deadline, or
shutdown error is **FAIL**. An established external access restriction or
missing verified prerequisite is **BLOCKED**, which leaves verification
incomplete. An explicit HTTP 429 pauses the remaining checks for that
exchange.

`MF_LIVE_REQUIRE_DATA=1` requires a data observation in every selected
case. Fixture collection always requires valid native observations and
publishes only after successful shutdown. It preserves the existing
95-file inventory and cannot publish an idle capture.

```bash
env -u MF_LIVE_FILTER cargo test --locked --manifest-path engine/Cargo.toml \
  --no-default-features --features management-api,management-tui,stream-client \
  --test live_network all_exchange_profiles_live -- --ignored --nocapture --test-threads=1
```

On 27 November 2026, the baseline selected and executed 30 cases.
All 30 completed physical upgrade and internal ACK confirmation. Matching
native data was observed in 29 cases, with zero failures and zero blockers.
Gate.io inverse `BTC_USD` trades was **IDLE** after a confirmed ACK and
clean shutdown. Its depth subscription passed with native order-book
updates. Offline replay confirmed the exact supplied native trade ACK.
The quiet observation window does not establish whether the exchange
executed any trades during that interval.

The additional native variant test accepts fresh, unmodified public exchange
instrument catalogue JSON files. Fetch each catalogue once on the coordinated
egress. Stop if either endpoint throttles or rejects the request.

```bash
curl --fail-with-body --max-time 12 -o /tmp/gate-usd1-contracts.json \
  https://api.gateio.ws/api/v4/futures/usd1/contracts
curl --fail-with-body --max-time 12 -o /tmp/bitget-usdc-instruments.json \
  'https://api.bitget.com/api/v3/market/instruments?category=USDC-FUTURES'
MF_GATE_USD1_METADATA=/tmp/gate-usd1-contracts.json \
MF_BITGET_USDC_METADATA=/tmp/bitget-usdc-instruments.json \
  cargo test --locked --manifest-path engine/Cargo.toml \
  --no-default-features --features management-api,management-tui,stream-client \
  --test live_network native_linear_variants_live -- --ignored --nocapture --test-threads=1
```

The variant test uses the exchange metadata for contract and asset semantics
with test-local registration identities. It uses the production Source builder,
transport manager and native ACK correlation. It establishes acquisition without
starting recovery, REST depth snapshots, or database writes. Financial readiness
and continuity require downstream normalization, integrity and recovery checks.

The same day, the additional variant run selected and executed four cases.
All four completed physical connection and internal ACK confirmation.
Two depth checks passed, two trade subscriptions were idle, and there
were zero failures or blockers.

| Profile | Settlement | Listed instrument | Resolved WebSocket route or product | Trades | Depth |
| --- | --- | --- | --- | --- | --- |
| Gate.io linear | USD1 | `BTC_USD1` | `usd1` | ACK confirmed, idle | ACK and native update |
| Bitget linear | USDC | `BTCPERP` | `usdc-futures` | ACK confirmed, idle | ACK and native snapshot |

The prepared Gate.io source connected to the configured USD1 endpoint
with decimal-size header configuration. The returned native depth quantity
was a string. Gate.io's upgrade header was independently inspected on
the same production connector in loopback. Bitget's ACK and depth
envelopes identified both `usdc-futures` and `BTCPERP`. Its public
instrument catalogue identified the REST product as `USDC-FUTURES`.
Both variant runs used fresh public catalogue responses because no
matching registrations were available from the accessible testing
catalogue and the production catalogue lookup failed.

The reviewed reconciliation branch separately passed
`websocket_manager_shared_slot_lifecycle_live` against public Binance futures
in 4.68 seconds. That run verified representative shared-slot subscription
lifecycle behavior on a real exchange.
