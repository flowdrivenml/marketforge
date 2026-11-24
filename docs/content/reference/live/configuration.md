# Live configuration

## Application

Existing general configuration remains the loader and authority. Optional `[runtime.application]` defaults:

| Setting | Default | Validation |
|---|---:|---|
| command_capacity | 64 | 1–1024 |
| max_definitions | 256 | 1–1024 |
| max_lifecycle_operations | 260 | at least definitions + Source concurrency, at most 2048 |
| source_start_concurrency | 4 | 1–64 |
| reconcile_interval_ms | 25 | 1–60000 |
| source_readiness_timeout_ms | 30000 | 1–3600000 |

## Management

Default bounds in `[control.management]`: command queue 32, active operations 16, completed history 256, plans 128, leases 128, event history 256, HTTP connections 128, concurrent requests 64, SSE clients 16, request body 64 KiB, response 4 MiB, request/socket-progress deadline 10 s, plan TTL 60 s, maximum lease 300 s, observation interval 1 s.

See the [management API](management-api.md) for request and observation behavior.
