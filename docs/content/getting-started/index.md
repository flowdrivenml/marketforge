# Getting Started

Start with [Installation](installation.md), then the [Quick Start](quickstart.md). This draft assumes a local repository checkout; it does not establish a standalone packaged deployment.

## A few core concepts

A **source or import** identifies where inputs come from. A **recipe** describes a requested selection using TOML. A **format profile** supplies interpretation rules; it does not make arbitrary external data compatible automatically. A **prepared job or batch** is a processing artifact, distinct from the recipe and the runtime settings.

Forge owns acquisition, planning, and registry concerns. Engine owns data-intensive processing. Their current handoff needs verification before an end-to-end walkthrough can be presented as runnable.
