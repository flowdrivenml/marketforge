# Develop MarketForge

This handbook is for contributors familiar with a shell, Git, and either Python or Rust. Start with a small local fixture. The [Bybit Quick Start](../getting-started/quickstart.md) introduces the market-data workflow if acquisition, trades, and L2 depth are new to you.

| I want to… | Start here |
| --- | --- |
| Set up a checkout and prove it works | [Local development](setup.md) |
| Make a small, reviewable change | [First contribution](first-contribution.md) |
| Trace a request through Forge, Engine, and storage | [Architecture](architecture.md) and [Historical operation lifecycles](operations.md) |
| Add or change a provider, profile, reader, or processing step | [Extension decision guide](extending.md) and [Source integration](sources.md) |
| Change a CLI, registry operation, or published contract | [Orchestration and persistence](orchestration.md) |
| Select a test or maintain a fixture | [Testing](testing.md) |
| Investigate a failure or quantify a regression | [Debugging](debugging.md) and [Performance](performance.md) |
| Review impact and prepare a contribution | [Compatibility and contributions](compatibility.md) |

The handbook explains implementation and verification. The [technical reference](../reference/index.md) is the place to look up the wire and storage contracts.
