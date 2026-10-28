#!/usr/bin/env python3
"""Discover Cargo JSON test artifacts and supervise exact libtest cases sequentially."""

import argparse
import json
import os
import re
import signal
import subprocess
import time
from pathlib import Path


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(0.2)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--targets", default=".*")
    parser.add_argument("--cases", default=".*")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.run and (args.output / "results.json").exists():
        parser.error(
            "use a unique output directory; previous results must be preserved"
        )
    package_dir = Path(__file__).resolve().parents[1]
    if Path.cwd() != package_dir:
        parser.error(f"run from the Rust package directory: {package_dir}")
    env = os.environ.copy()
    env["CARGO_MANIFEST_DIR"] = str(package_dir)
    env["CARGO_MANIFEST_PATH"] = str(package_dir / "Cargo.toml")
    sysroot = subprocess.check_output(
        ["rustc", "--print", "sysroot"], text=True
    ).strip()
    host = next(
        x.split(": ", 1)[1]
        for x in subprocess.check_output(["rustc", "-Vv"], text=True).splitlines()
        if x.startswith("host:")
    )
    paths = [str(Path(sysroot) / "lib" / "rustlib" / host / "lib")]
    env["LD_LIBRARY_PATH"] = os.pathsep.join(paths + [env.get("LD_LIBRARY_PATH", "")])
    artifacts = {}
    for line in args.artifacts.read_text().splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if (
            entry.get("reason") == "compiler-artifact"
            and entry.get("profile", {}).get("test")
            and entry.get("executable")
        ):
            artifacts[entry["executable"]] = entry["target"]
    artifact_dirs = sorted({str(Path(executable).parent) for executable in artifacts})
    env["LD_LIBRARY_PATH"] = os.pathsep.join(artifact_dirs + [env["LD_LIBRARY_PATH"]])
    results = []
    discovery = []
    for executable, target in artifacts.items():

        def listed(extra):
            r = subprocess.run(
                [executable, "--list", "--format=terse", *extra],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode:
                raise RuntimeError(f"Listing failed: {executable}: {r.stderr}")
            return [
                line[:-6] for line in r.stdout.splitlines() if line.endswith(": test")
            ]

        try:
            names = listed([])
            ignored = set(listed(["--ignored"]))
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as error:
            results.append(
                dict(
                    target=target["name"],
                    name=None,
                    executable=executable,
                    status="BLOCKED_ENVIRONMENT",
                    detail=str(error),
                )
            )
            continue
        for name in names:
            row = dict(
                target=target["name"],
                kind=target["kind"],
                name=name,
                executable=executable,
                ignored=name in ignored,
            )
            discovery.append(row)
            result = dict(row, status="NOT_RUN")
            if row["ignored"]:
                result["status"] = "IGNORED_EXISTING"
            elif "network" in target["name"]:
                result["status"] = "SKIPPED_NETWORK"
            elif "benchmark" in target["name"]:
                result["detail"] = "Benchmark execution is intentionally excluded"
            elif (
                args.run
                and re.fullmatch(args.targets, target["name"])
                and re.fullmatch(args.cases, name)
            ):
                logfile = args.output / f"{len(results):05d}.log"
                started = time.monotonic()
                with logfile.open("w") as log:
                    process = subprocess.Popen(
                        [
                            executable,
                            "--exact",
                            name,
                            "--nocapture",
                            "--test-threads=1",
                        ],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        env=env,
                        start_new_session=True,
                    )
                    try:
                        code = process.wait(
                            timeout=max(0, started + 30 - time.monotonic())
                        )
                        status = "PASS" if code == 0 else "FAIL"
                    except subprocess.TimeoutExpired:
                        stop_group(process)
                        code = process.returncode
                        status = "TIMED_OUT_30S"
                output = logfile.read_text(errors="replace")
                if status == "PASS" and not re.search(
                    r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output
                ):
                    status = "FAIL"
                result.update(
                    status=status,
                    exit_code=code,
                    seconds=time.monotonic() - started,
                    log=str(logfile),
                )
            results.append(result)
            with (args.output / "results.jsonl").open("a") as stream:
                stream.write(json.dumps(result) + "\n")
    if args.run and not results:
        results.append(
            dict(
                target=None,
                name=None,
                status="BLOCKED_ENVIRONMENT",
                detail="No libtest cases discovered from Cargo test artifacts",
            )
        )
    (args.output / "discovery.json").write_text(json.dumps(discovery, indent=2))
    (args.output / "results.json").write_text(json.dumps(results, indent=2))
    counts = {
        status: sum(r["status"] == status for r in results)
        for status in sorted(set(r["status"] for r in results))
    }
    print(json.dumps(counts, indent=2))
    return (
        int(
            any(
                r["status"] not in ("PASS", "IGNORED_EXISTING", "SKIPPED_NETWORK")
                for r in results
            )
        )
        if args.run
        else 0
    )


if __name__ == "__main__":
    raise SystemExit(main())
