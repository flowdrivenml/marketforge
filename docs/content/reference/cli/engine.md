# Engine CLI

## Managed engine

```bash
mf engine build
mf engine build normalize --dry-run
mf engine build normalize --offline --jobs 4
mf engine build merge
mf engine status
```

The explicit default set contains only `normalize`: package `marketforge-engine`, Cargo binary `marketforge-normalize`, release profile, no default features, approved features `historical,monitor-tui`. Build prerequisites are native Linux/macOS, Cargo and a compatible Rust edition-2024 toolchain. PostgreSQL, credentials and market files are unnecessary. Linux builds were exercised; macOS support is unverified.

Build uses `--locked`; ordinary builds may fetch lockfile dependencies, while `--offline` prohibits fetching. It respects Cargo artifact paths, including custom target directories. A configured foreign target is rejected. `--jobs N` takes a positive compiler job count. `--dry-run` prints command, prerequisites and destination without compiling or installing. The optional target is `normalize` or `merge`; omit it to build the default set. No Rust installation, dependency update, optimization override or workload submission occurs.

The executable is an independent copy at repository-root `.bin/marketforge-normalize` or `.bin/marketforge-merge`, independent of the data root. Forge checks its non-processing `--help` before atomic replacement. Existing launchers are preserved. A file lock prevents competing Forge installers. `.bin/manifest.json` records toolchain, features, host, smoke result, timestamps, size, SHA-256 and revision/dirty indication. The latter is not an exact modified-source snapshot.

Status is read-only and distinguishes missing installation, missing/malformed metadata, non-executable files, unsafe symlinks, verified checksums and mismatches. It never executes existing bytes. Executable and manifest replacements are separate; interruption can leave a detectable mismatch. An explicit build repairs it. Normalization never builds automatically, modifies shell PATH or installs system-wide.

Maintainers approve future targets by extending the typed `TARGETS` catalogue in `forge/historical/engine/managed.py`, specifying package, binary, installed filename, features, default-feature policy, enabled/default selection, non-processing smoke arguments and the expected output marker. Reuse the same build/install service. Build the merge target with `mf engine build merge`.
