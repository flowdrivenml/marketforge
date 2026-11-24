"""Measure bounded preview and streamed queries in isolated Python processes."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any

from forge.datasets import parquet

CASES = (
    "first_trade",
    "first_depth",
    "count",
    "scan",
    "instrument",
    "hour",
    "partition",
    "schema",
    "summary",
)


def reader(path: Path | None) -> ModuleType:
    if path is None:
        return parquet
    spec = importlib.util.spec_from_file_location("marketforge_baseline_reader", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load reader from {path}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def physical_bytes() -> int | None:
    path = Path("/proc/self/io")
    if not path.is_file():
        return None
    return int(
        next(
            line.split()[1]
            for line in path.read_text().splitlines()
            if line.startswith("read_bytes:")
        )
    )


def measure(root: Path, case: str, baseline: Path | None) -> dict[str, Any]:
    backend = reader(baseline)
    manifest = json.loads((root / "manifest.json").read_text())
    files = [root / part["path"] for part in manifest["files"]]
    if case == "partition":
        files = files[:1]
    kind = "depth" if case == "first_depth" else "trade"
    projection = [
        "event_timestamp_ns",
        "instrument_id",
        "event_kind",
        "stream_id",
        kind,
    ]
    terms: list[parquet.Predicate] = [("event_kind", "=", int(kind == "depth"))]
    if case == "instrument":
        terms.append(("instrument_id", "=", manifest["streams"][0]["instrument_id"]))
    if case == "hour":
        lo = manifest["time_range"]["start_ns"] + 10 * 3600 * 10**9
        terms += [
            ("event_timestamp_ns", ">=", lo),
            ("event_timestamp_ns", "<", lo + 3600 * 10**9),
        ]
    old_columns = None if baseline is not None else projection
    if case == "count":
        old_columns = ["event_kind"]
    before = physical_bytes()
    started = time.perf_counter()
    if case == "schema":
        rows = len(backend.schema(files))
    elif case == "summary":
        rows = backend.metadata(files)["rows"]
    elif case in {"first_trade", "first_depth", "partition"}:
        frame = backend.preview(files, n=20, columns=old_columns, predicates=terms)
        if baseline is not None:
            frame = frame.select(projection)
        rows = len(frame)
    else:
        rows = sum(
            batch.num_rows
            for batch in backend.batches(files, columns=old_columns, predicates=terms)
        )
    elapsed = time.perf_counter() - started
    after = physical_bytes()
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "seconds": elapsed,
        "rss_mib": peak / (1024**2 if sys.platform == "darwin" else 1024),
        "storage_bytes": after - before
        if before is not None and after is not None
        else None,
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("publication", type=Path)
    parser.add_argument(
        "--baseline", type=Path, help="Prior parquet.py from a trusted local checkout."
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--case", choices=CASES)
    args = parser.parse_args()
    if args.case:
        print(json.dumps(measure(args.publication, args.case, args.baseline)))
        return
    if args.baseline is None or args.repeats < 1:
        parser.error("Comparison requires --baseline and positive --repeats.")
    for case in CASES:
        runs: dict[str, list[dict[str, Any]]] = {"before": [], "after": []}
        for repetition in range(args.repeats):
            for mode in tuple(runs)[:: -1 if repetition % 2 else 1]:
                command = [
                    sys.executable,
                    __file__,
                    str(args.publication),
                    "--case",
                    case,
                ]
                if mode == "before":
                    command += ["--baseline", str(args.baseline)]
                result = subprocess.check_output(
                    command, text=True, env={**os.environ, "POLARS_MAX_THREADS": "4"}
                )
                runs[mode].append(json.loads(result))
        if len({row["rows"] for samples in runs.values() for row in samples}) != 1:
            raise ValueError(f"Result row counts differ for {case}.")
        print(
            json.dumps(
                {
                    "case": case,
                    "measured": {
                        label: {
                            "seconds": statistics.median(
                                row["seconds"] for row in samples
                            ),
                            "rss_mib": statistics.median(
                                row["rss_mib"] for row in samples
                            ),
                            "storage_bytes": [row["storage_bytes"] for row in samples],
                            "rows": samples[0]["rows"],
                        }
                        for label, samples in runs.items()
                    },
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
