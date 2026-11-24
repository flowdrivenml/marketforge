# Managed Engine commands

Plan, build and inspect Forge-managed executables. [Workflow and prerequisites](../../development/setup.md#build-the-executable-forge-will-use) · [Shared CLI behavior](index.md)

## Approved targets

| Target | Binary installed in `.bin/` | Default build | Features | Smoke check |
| --- | --- | --- | --- | --- |
| `normalize` | `marketforge-normalize` | Yes | `historical,monitor-tui` | Help advertises `--batch` |
| `merge` | `marketforge-merge` | Explicit target | `historical,monitor-tui` | Help advertises `--validate-job` |
| `convert` | `marketforge-convert` | Explicit target | `historical,monitor-tui` | Help advertises `--job` |

A build uses the locked Cargo release profile on Linux or macOS and installs a separate checksum-pinned copy. A dry run reports the command, destination and missing prerequisites without invoking Cargo. `--offline` uses cached dependencies and `--jobs` requires a positive integer. See [the target registry](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/engine/managed.py).

## Synopsis and arguments

### `mf engine status`

```text
mf engine status [-h]
```

### `mf engine runtime`

```bash
mf engine runtime --help
mf --json engine runtime pipeline
```

Choose one operation. Output includes the effective settings and the source
path. Pipeline output includes the stage settings and source TOML hashes.
This command uses the managed binaries and works without a database connection.
[Runtime schema and inheritance](../runtime.md)

### `mf engine build`

```text
mf engine build [-h] [--dry-run] [--offline] [--jobs JOBS]
                                [target]
```

| Argument or option | Parser contract |
| --- | --- |
| `target` | Optional. |
| `--dry-run` | Default `false`. |
| `--offline` | Default `false`. |
| `--jobs` | Optional. |

## Understand Cargo Build Options

The managed Engine builder uses Cargo internally.

For the historical normalizer, the approved build configuration includes:

| Setting | Value |
|---|---|
| Cargo package | `marketforge-engine` |
| Cargo binary | `marketforge-normalize` |
| Build profile | Release |
| Default Cargo features | Disabled |
| Approved features | `historical`, `monitor-tui` |
| Dependency lockfile | Enforced with `--locked` |

These settings are managed by Forge.

You do not need to provide the corresponding Cargo arguments manually.

### Why use release builds?

Rust supports different compilation profiles.

Debug builds prioritize development convenience, while release builds apply optimizations intended for execution performance.

Historical normalization can involve large workloads, so the managed normalizer uses the release profile.

### What are Cargo features?

Cargo features allow Rust projects to enable optional capabilities.

For example, MarketForge's historical normalizer uses features associated with historical processing and terminal monitoring.

Forge selects the approved features for the target.

This helps ensure that the installed binary includes the capabilities expected by the relevant processing workflow.

### Why disable default features?

A Rust package may contain functionality that is not required by every executable.

By disabling default features and explicitly enabling the required ones, the managed build can select a narrower configuration.

This helps avoid unintentionally including unrelated optional functionality.

### Why use `--locked`?

Cargo uses a lockfile to record resolved dependency versions.

The managed build uses:

```text
--locked
```

This tells Cargo to use the existing dependency lockfile without updating it.

If the lockfile needs changes that the build is not permitted to make, Cargo reports an error.

This helps keep builds consistent with the repository's recorded dependencies.

However, a lockfile does not guarantee identical compiled bytes across different operating systems, compilers, or build environments.

---

## How MarketForge Installs Compiled Engines

Once Cargo successfully compiles an approved target, Forge installs it into the managed `.bin/` directory.

This installation process includes several safeguards.

### The executable is checked before installation

Forge performs a non-processing startup check using the compiled executable's help interface.

The purpose is to verify that the selected executable can start and respond as expected.

This is sometimes called a **smoke test**.

For example, the managed normalizer is expected to respond to its supported non-processing help invocation.

A failed smoke check prevents the build from being accepted as a successful managed installation.

### Why check the executable?

Successful compilation proves that Cargo produced an executable.

However, it does not necessarily prove that the executable can start correctly in the current environment.

A smoke check provides an additional verification step before installation.

It is deliberately limited to non-processing behavior.

It does not execute normalization, read historical archives, or publish datasets.

### Existing installations are protected

Forge prepares and checks the new executable before replacing an existing managed binary.

The executable replacement is performed atomically.

This reduces the risk of leaving a partially copied executable as the installed binary.

If the build or pre-installation checks fail, an existing installation is preserved rather than deliberately replaced by the failed candidate.

### Concurrent builds are controlled

Forge uses a file lock to prevent competing managed installers from modifying the same installation state at the same time.

This helps protect the managed binary directory when multiple commands attempt to build or install engines concurrently.

### Why not copy Cargo output manually?

Manually copying an executable into `.bin/` bypasses the managed installation process.

The copied file may not match the recorded build information.

This can cause `mf engine status` to report a checksum or metadata problem.

Use:

```bash
mf engine build normalize
```

or:

```bash
mf engine build merge
```

to create supported managed installations.

---

## Build Information and the Manifest

MarketForge keeps a record of its managed Engine installations.

This information is stored in:

```text
.bin/manifest.json
```

### Why does MarketForge need a manifest?

Imagine compiling the normalizer today and then updating the Rust source code next week.

You may want to know which executable is currently installed and how it was built.

The manifest helps answer questions about the installed binary.

For example, it can record:

- The selected build target.
- The Rust toolchain used.
- The enabled Cargo features.
- The target host.
- The installed executable.
- The executable's file size.
- The SHA-256 checksum.
- The result of the startup smoke check.
- Build-related timestamps.
- Repository revision and dirty-state information.

These details help MarketForge verify the installed executable and provide useful information when investigating a build.

### What is an executable checksum?

A SHA-256 checksum is calculated from the executable's contents.

If the installed file changes, its checksum will normally change as well.

MarketForge records the checksum during installation.

Later, it can compare the current executable against the recorded value.

This helps detect unexpected file changes.

### Does the manifest prove which source code produced the binary?

Not completely.

The manifest retains repository revision and dirty-state information.

However, this does not represent an exact snapshot of every modified source file used during compilation.

For example, a repository may contain uncommitted changes when the engine is built.

The manifest can indicate that the working tree was dirty, but that information alone does not reconstruct the precise source contents.

For reproducible engineering investigations, retain the relevant source revision and build environment separately.

### Should you edit the manifest manually?

No.

The manifest belongs to the managed build system.

Manually changing it can make the recorded installation information inconsistent with the actual executable.

If the manifest is missing, damaged, or inconsistent, use the supported build command to recreate a valid installation.

---

## Check Installed Engines

After building an engine, you should verify that MarketForge recognizes the installation.

Use:

```bash
mf engine status
```

This command inspects the managed Engine installation state.

### Why check engine status?

An executable may exist on disk but still be unsuitable for managed execution.

For example:

- Its manifest may be missing.
- The recorded checksum may not match.
- The executable may no longer have the correct permissions.
- The path may have been replaced by an unsafe symbolic link.
- The recorded installation information may be malformed.

MarketForge therefore checks more than the presence of a filename.

### What does status inspect?

The status command examines the installed targets and their retained build information.

It can distinguish conditions such as:

| Condition | Meaning |
|---|---|
| Missing installation | The managed executable is not installed. |
| Missing metadata | The executable or its installation record is incomplete. |
| Invalid metadata | The manifest cannot be interpreted correctly. |
| Non-executable file | The installed file cannot be executed as required. |
| Unsafe path | The installed path does not satisfy the managed-file safety checks. |
| Checksum mismatch | The current executable differs from its recorded checksum. |
| Verified installation | The applicable file and metadata checks passed. |

The exact status fields should be read from the command's output.

### Does status run the executable?

No.

**`mf engine status` is a read-only inspection command.**

It does not execute the installed Engine binary.

Instead, it checks the recorded installation metadata and the relevant filesystem properties.

This makes it useful when investigating an unknown or potentially damaged installation.

### Does status require PostgreSQL?

No.

Engine status does not need database credentials or historical market-data files.

It inspects the managed installation in the project workspace.

### Check after building

A normal sequence is:

```bash
mf engine build normalize
mf engine status
```

Confirm that the normalizer appears as a verified managed installation.

After building the merger, run the same status command to inspect the managed targets.

---

## What Happens When a Build Changes?

As MarketForge evolves, the Rust source code may change.

For example, you might update:

- Historical normalization logic.
- Trade decoding.
- Order-book reconstruction.
- Merge behavior.
- Processing integrity checks.
- Engine command-line behavior.

These source changes are not automatically applied to an executable that was compiled earlier.

### Source code and compiled binaries are different

Consider the following:

```text
Rust Source Code
      |
      v
   Compile
      |
      v
Installed Binary
```

Once compiled, the binary contains the code produced by that build.

Editing the Rust source afterward does not modify the installed executable.

### Rebuild after changing Engine code

If you change Rust code used by the normalizer, rebuild it:

```bash
mf engine build normalize
```

If you change code used by the merger, rebuild that target:

```bash
mf engine build merge
```

The managed builder compiles and installs a new executable using the supported build configuration.

### Does `mf engine status` detect every source change?

No.

Status verifies the installed binary against its recorded installation information.

It does not perform a complete comparison between the current Rust source tree and the exact source contents used during compilation.

For example, an installed binary may still pass its checksum verification after you edit the Rust source code.

This means:

**A verified installation is not necessarily a build of the latest working-tree changes.**

Rebuild after making changes that should affect the Engine.

### Does Forge rebuild automatically before normalization?

No.

Managed normalization does not automatically compile the Engine.

This is intentional.

Compilation and historical processing are separate operations.

It prevents a normalization request from unexpectedly starting a potentially expensive Rust build.

Make sure the required executable is installed before running the corresponding workflow.

---

## Understanding Engine Compatibility

A successful build is necessary, but it does not prove that every historical processing request will succeed.

For example, Forge prepares normalization jobs using a defined contract.

The Rust Engine must understand the prepared job format and support the requested source interpretation.

### Build validity and processing compatibility are different

Consider these checks:

| Check | What it establishes |
|---|---|
| Cargo compilation | The selected Rust target compiled successfully. |
| Startup smoke check | The executable responded to the expected non-processing invocation. |
| Engine status | The installed file matches its recorded installation information. |
| Prepared-job validation | The Engine understands the submitted processing contract. |
| End-to-end execution | The requested workload was actually processed. |
| Output verification | The produced artifacts satisfy the applicable output checks. |

These checks establish different things.

For example, a normalizer can compile successfully while a particular profile or prepared batch is invalid.

Similarly, a successfully processed source does not automatically establish complete historical market-data coverage.

### When should you rebuild?

Rebuild the relevant executable when:

- Updating Engine code.
- Switching to a repository revision with relevant Engine changes.
- Changing the approved build configuration.
- Repairing a damaged installation.
- Investigating a suspected binary mismatch.

### When should you investigate the processing configuration?

If the installed Engine is verified but normalization rejects a particular job, investigate the processing request.

For example, the issue might involve:

- An unsupported profile.
- Missing historical instrument specifications.
- An invalid prepared batch.
- Incorrect source metadata.
- A source-data integrity failure.
- An unsupported output or merge layout.

Rebuilding the Engine does not automatically correct these problems.

Use the relevant normalization or merge diagnostics to identify the actual failure.

---

## Repair an Engine Installation

Sometimes an installation becomes inconsistent.

For example, the executable might have been modified after installation or the manifest may no longer match the file.

MarketForge is designed to detect these situations.

### Step 1: Inspect the installation

Run:

```bash
mf engine status
```

Review the reported state of the affected target.

### Step 2: Identify the problem

Common conditions include:

| Problem | What it means |
|---|---|
| Missing binary | The executable was not installed or is no longer present. |
| Missing manifest | The recorded installation metadata is unavailable. |
| Checksum mismatch | The executable contents differ from the recorded version. |
| Non-executable file | The file does not satisfy executable-file requirements. |
| Unsafe symbolic link | The managed path fails the safety checks. |
| Invalid manifest | The installation metadata is malformed or inconsistent. |

### Step 3: Rebuild the target

For the normalizer:

```bash
mf engine build normalize
```

For the merger:

```bash
mf engine build merge
```

The managed builder creates a new executable and installation record.

### Step 4: Verify the repaired installation

Run:

```bash
mf engine status
```

Confirm that the affected target is now verified.

### Why not manually repair the manifest?

The executable and manifest describe one managed installation.

Changing only one can create another mismatch.

Use the build command so that compilation, executable verification, installation, and metadata recording follow the supported process.

### What if installation was interrupted?

Although the executable replacement itself is atomic, the binary and manifest are installed as separate filesystem updates.

An interruption between those updates can leave an executable and manifest that do not match.

MarketForge can detect this condition during status inspection.

A new managed build is the supported repair path.

---

## Troubleshooting Engine Builds

Build problems can occur because of missing tools, unavailable dependencies, compiler errors, or installation inconsistencies.

The appropriate solution depends on where the build fails.

### Rust compiler not found

**Possible cause:** Rust is not installed or the compiler is unavailable in your terminal environment.

Check:

```bash
rustc --version
```

And:

```bash
cargo --version
```

Install or configure a compatible Rust toolchain before retrying.

### Unsupported Rust edition

**Possible cause:** The installed Rust compiler is too old to support edition 2024.

MarketForge Engine uses Rust edition 2024.

Inspect the installed version:

```bash
rustc --version
```

Update the toolchain to a compatible version.

### Cargo dependencies cannot be downloaded

**Possible cause:** Network access is unavailable, a dependency source cannot be reached, or required dependencies are missing from the local cache.

Inspect the Cargo error.

If you intentionally require an offline build, use:

```bash
mf engine build normalize --offline
```

However, this succeeds only when the required dependencies are already available locally.

### Offline build fails

**Possible cause:** Cargo cannot find a required dependency in its local sources.

Offline mode does not download missing packages.

You may need to prepare the dependency cache in an environment with network access before attempting the offline build again.

### Cargo reports a lockfile problem

**Possible cause:** The current dependency declarations and lockfile are inconsistent.

Managed builds use `--locked`.

This prevents the build from silently updating the dependency lockfile.

Inspect the reported dependency or lockfile problem.

Do not remove `--locked` from the managed build simply to hide an unresolved repository inconsistency.

### Compilation runs out of memory

**Possible cause:** The selected compiler parallelism exceeds available system resources.

Try a smaller job count:

```bash
mf engine build normalize --jobs 2
```

Or:

```bash
mf engine build normalize --jobs 1
```

Reducing parallel compiler jobs can lower memory pressure.

It does not guarantee that every compilation will fit on a resource-constrained machine.

### A custom Cargo target directory is configured

MarketForge respects the applicable Cargo build artifact paths.

The managed builder resolves the compiled executable rather than assuming it always appears in one fixed Cargo directory.

The installed executable still goes into the project-local `.bin/` directory.

If the build cannot locate the expected output, inspect the selected target and Cargo configuration.

### Cargo is configured for another platform

The managed build checks the configured compilation target.

A foreign target is rejected.

This prevents an executable compiled for another platform from being installed as though it were a native MarketForge Engine.

Use a supported native target for the current system.

### The build succeeds but installation fails

**Possible cause:** The startup smoke check, executable installation, workspace permissions, or another managed installation safeguard failed.

Inspect the reported error.

The installer checks the candidate executable before accepting it.

Do not assume that successful Cargo compilation alone means the managed installation succeeded.

### The executable is missing

Inspect:

```bash
mf engine status
```

If the normalizer is missing, build it:

```bash
mf engine build normalize
```

For the merger:

```bash
mf engine build merge
```

### Engine status reports a checksum mismatch

**Possible cause:** The installed executable changed after its installation record was written.

Rebuild the affected target using the managed builder.

For example:

```bash
mf engine build normalize
```

Then check the installation again.

### Engine status reports missing metadata

**Possible cause:** The installation manifest is missing or inconsistent.

Use the supported build command to recreate the managed installation.

Do not manually invent checksum values or metadata entries.

### The Engine is verified but normalization fails

A verified Engine installation does not guarantee that every processing request is valid.

Inspect the normalization result and the relevant Engine diagnostics.

Possible causes include invalid profiles, missing instrument specifications, unsupported source formats, or data-integrity failures.

See [Historical Normalization](../../workflows/normalization/index.md).

### The merger is installed but rejects a dataset

The merger only supports compatible input formats and layouts.

Inspect the merge preparation result and supported compatibility rules.

The presence of a verified merger executable does not make an unsupported input layout eligible.

See [Historical Merge](merge.md).

---
