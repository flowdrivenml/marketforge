#!/usr/bin/env python3
"""Prepare the native captures consumed by offline MarketForge Live tests."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import NamedTuple

WORKSPACE = Path(__file__).resolve().parents[3]
ROOT = WORKSPACE / "data/tests/live"
PROFILES = tuple(
    f"{exchange}_{market}"
    for exchange in ("binance", "bitget", "bybit", "gateio", "okx")
    for market in ("spot", "linear", "inverse")
)
FAMILIES = {
    "websocket-fixtures/trades": "WebSocket trades",
    "websocket-fixtures/depth": "WebSocket depth",
    "rest-fixtures": "Standalone REST",
    "rest-service-fixtures": "Production REST service",
}
BASE_REST = {
    (profile, endpoint)
    for profile in PROFILES
    for endpoint in (
        "depth_snapshot",
        "historical_trades" if profile.startswith("gateio_") else "recent_trades",
    )
}
SERVICE_EXTRA = {
    ("binance_inverse", "aggregate_trades"),
    ("binance_spot", "historical_trades"),
    *((profile, "historical_trades") for profile in PROFILES if profile.startswith("okx_")),
}
MAX_BYTES = 16 * 1024 * 1024


def inventory():
    paths = [
        f"{family}/{profile}.json"
        for family in tuple(FAMILIES)[:2]
        for profile in PROFILES
    ]
    for family, endpoints in (
        ("rest-fixtures", BASE_REST),
        ("rest-service-fixtures", BASE_REST | SERVICE_EXTRA),
    ):
        paths.extend(f"{family}/{profile}__{endpoint}.json" for profile, endpoint in sorted(endpoints))
    return tuple(paths)


REQUIRED = inventory()


class Fixture(NamedTuple):
    status: str
    contents: bytes = b""
    detail: str = ""


def reject_non_json_number(value):
    raise ValueError(f"{value} is not a JSON number")


def object_list(value):
    return isinstance(value, list) and bool(value) and all(
        isinstance(item, dict) and item for item in value
    )


def native_ws_shape(messages, exchange, depth):
    if not object_list(messages):
        return False
    if depth and len(messages) != 12:
        return False
    for index, message in enumerate(messages):
        if exchange == "binance":
            valid = message.get("e") == ("depthUpdate" if depth else "trade") and isinstance(message.get("s"), str)
        elif exchange == "gateio":
            channel = message.get("channel", "")
            result = message.get("result")
            valid = (
                isinstance(channel, str)
                and channel.endswith(".order_book_update" if depth else ".trades")
                and message.get("event") == "update"
                and (isinstance(result, dict) and bool(result) or object_list(result))
            )
        elif exchange == "bybit":
            topic = message.get("topic", "")
            valid = isinstance(topic, str) and topic.startswith("orderbook." if depth else "publicTrade.")
            if depth:
                valid = valid and isinstance(message.get("data"), dict) and message.get("type") == (
                    "snapshot" if index == 0 else "delta"
                )
            else:
                valid = valid and object_list(message.get("data"))
        else:
            arg = message.get("arg")
            valid = isinstance(arg, dict) and arg.get("channel" if exchange == "okx" else "topic") == (
                "books" if depth else "trades-all" if exchange == "okx" else "publicTrade"
            ) and object_list(message.get("data"))
            if depth:
                valid = valid and message.get("action") == ("snapshot" if index == 0 else "update")
        if not valid:
            return False
    return True


def native_rest_shape(body, profile, endpoint, standalone):
    exchange = profile.split("_", 1)[0]
    if endpoint != "depth_snapshot":
        if exchange == "bybit":
            if not isinstance(body, dict) or body.get("retCode") != 0 or not isinstance(body.get("result"), dict):
                return False
            records = body["result"].get("list")
        elif exchange in {"bitget", "okx"}:
            if not isinstance(body, dict) or body.get("code") != ("00000" if exchange == "bitget" else "0"):
                return False
            records = body.get("data")
        else:
            records = body
        if not object_list(records):
            return False
        if standalone:
            # The fixture-backed REST decoder regression checks the full population.
            expected = 60 if profile == "bybit_spot" else 500 if exchange in {"bybit", "binance"} else 100
            return len(records) == expected
        return len(records) <= 10_000

    if not isinstance(body, dict):
        return False
    bid, ask = "bids", "asks"
    if exchange == "bybit":
        if body.get("retCode") != 0:
            return False
        body, bid, ask = body.get("result"), "b", "a"
    elif exchange == "bitget":
        if body.get("code") != "00000":
            return False
        body, bid, ask = body.get("data"), "b", "a"
    elif exchange == "okx":
        if body.get("code") != "0" or not object_list(body.get("data")):
            return False
        body = body["data"][0]
    return isinstance(body, dict) and all(
        isinstance(body.get(side), list) and body[side]
        and all(isinstance(level, (list, dict)) and level for level in body[side])
        for side in (bid, ask)
    )


def inspect(root, relative, source_path=None):
    path = root / relative if source_path is None else source_path
    try:
        if path.is_symlink():
            return Fixture("invalid", detail="fixture must be a regular file")
        if not path.exists():
            return Fixture("missing")
        if not path.is_file():
            return Fixture("invalid", detail="fixture must be a regular file")
        with path.open("rb") as stream:
            contents = stream.read(MAX_BYTES + 1)
        if not contents or len(contents) > MAX_BYTES:
            return Fixture("invalid", detail="empty fixture or over 16 MiB")
        value = json.loads(contents, parse_constant=reject_non_json_number, parse_float=str)
        exchange = path.name.split("_", 1)[0]
        if relative.startswith("websocket-fixtures/"):
            valid = native_ws_shape(value, exchange, "/depth/" in relative)
        else:
            profile, endpoint = path.stem.split("__")
            valid = native_rest_shape(value, profile, endpoint, relative.startswith("rest-fixtures/"))
        if not valid:
            return Fixture("invalid", detail="unexpected native JSON representation")
        return Fixture("available", contents)
    except (OSError, ValueError, RecursionError) as error:
        return Fixture("invalid", detail=str(error))


def check(root):
    complete = True
    total_bytes = 0
    for family, label in FAMILIES.items():
        members = [path for path in REQUIRED if path.startswith(family + "/")]
        available = 0
        size = 0
        problems = []
        for path in members:
            fixture = inspect(root, path)
            if fixture.status == "available":
                available += 1
                size += len(fixture.contents)
            else:
                problems.append((path, fixture))
                candidate = root / path
                if not candidate.is_symlink() and candidate.is_file():
                    try:
                        size += candidate.stat().st_size
                    except OSError:
                        pass
        print(f"{label}: {available}/{len(members)} available, {size:,} bytes stored")
        for path, fixture in problems:
            print(f"  {fixture.status}: {path}" + (f" ({fixture.detail})" if fixture.detail else ""))
        complete &= not problems
        total_bytes += size
    print(f"Total stored size of required fixture files: {total_bytes:,} bytes")
    return complete


def publish(root, relative, contents, replace):
    destination = root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (root / ".publish.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not replace and inspect(root, relative).status == "available":
            return False
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(contents)
                stream.flush()
                os.fsync(stream.fileno())
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
    return True


def import_fixtures(root, source, replace, paths=REQUIRED):
    counts = dict.fromkeys(("imported", "existing", "missing", "invalid"), 0)
    for relative in paths:
        if not replace and inspect(root, relative).status == "available":
            counts["existing"] += 1
            print(f"existing: {relative}")
            continue
        fixture = inspect(source, relative)
        family, name = relative.split("/", 1)
        if fixture.status == "missing" and family in {"rest-fixtures", "rest-service-fixtures"}:
            # The former ignored Rust producers wrote these names under engine/target.
            fixture = inspect(source, relative, source / f"live-{family}" / name)
        if fixture.status != "available":
            counts[fixture.status] += 1
            print(f"{fixture.status}: {relative}" + (f" ({fixture.detail})" if fixture.detail else ""))
            continue
        try:
            written = publish(root, relative, fixture.contents, replace)
        except OSError as error:
            counts["invalid"] += 1
            print(f"could not publish: {relative} ({error})", file=sys.stderr)
            continue
        status = "imported" if written else "existing"
        print(f"{status}: {relative}")
        counts[status] += 1
    print(", ".join(f"{value} {key}" for key, value in counts.items()))
    return not (counts["missing"] or counts["invalid"])


def read_weights(values):
    weights = {}
    for value in values:
        match = re.fullmatch(r"(binance_(?:spot|linear|inverse))=([1-9][0-9]*)/([1-9][0-9]*)", value)
        if not match:
            raise ValueError("expected --rest-weight binance_PROFILE=CAPACITY/WINDOW_SECONDS")
        profile, capacity, window = match.groups()
        if profile in weights or max(int(capacity), int(window)) >= 2**64:
            raise ValueError(f"duplicate or out-of-range REST weight for {profile}")
        weights[profile] = value
    return weights


def collect(root, replace, weights, run=subprocess.run):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".collect.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("another fixture collection is running for this worktree", file=sys.stderr)
            return False
        return collect_locked(root, replace, weights, run)


def collect_locked(root, replace, weights, run):
    needed = [path for path in REQUIRED if replace or inspect(root, path).status != "available"]
    rest = [path for path in needed if path.startswith("rest")]
    websocket = [path for path in needed if path.startswith("websocket")]
    blocked = [
        path for path in rest
        if Path(path).name.startswith("binance_") and Path(path).name.split("__")[0] not in weights
    ]
    for path in blocked:
        print(f"collection blocked: {path} (provide verified --rest-weight for this profile)")
    rest = [path for path in rest if path not in blocked]
    success = not blocked
    with tempfile.TemporaryDirectory(prefix=".capture-", dir=root) as directory:
        stage = Path(directory)
        for paths, test in (
            (websocket, "websocket::bulk::collect_websocket_fixtures"),
            (rest, "rest::collect_rest_fixtures"),
        ):
            if not paths:
                continue
            environment = os.environ.copy()
            for key in ("LIVE_REST_PROFILE", "LIVE_REST_MAX", "LIVE_REST_ALL", "MF_LIVE_FILTER"):
                environment.pop(key, None)
            environment.update(
                MF_LIVE_FIXTURE_STAGE=str(stage),
                MF_LIVE_FIXTURE_PATHS="\n".join(paths),
                MF_LIVE_FIXTURE_WEIGHTS="\n".join(weights.values()),
            )
            command = [
                "cargo", "test", "--locked", "--manifest-path", "engine/Cargo.toml",
                "--no-default-features", "--features", "live", "--test", "live_network",
                test, "--", "--exact", "--ignored", "--nocapture",
            ]
            try:
                result = run(command, cwd=WORKSPACE, env=environment, check=False)
                success &= result.returncode == 0
            except OSError as error:
                print(f"collector could not start: {error}", file=sys.stderr)
                success = False
            success &= import_fixtures(root, stage, replace, paths)
    return check(root) and success


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="validate the required local fixture inventory")
    importer = commands.add_parser("import", help="reuse an existing tree of native captures")
    importer.add_argument("--from", dest="source", type=Path, required=True)
    collector = commands.add_parser("collect", help="collect missing fixtures from public exchanges")
    collector.add_argument("--rest-weight", action="append", default=[], metavar="PROFILE=CAPACITY/SECONDS",
                           help="verified Binance REST request-weight budget (repeat per profile)")
    for command in (importer, collector):
        command.add_argument("--replace", action="store_true", help="replace valid fixtures only with validated input")
    args = parser.parse_args()
    if args.command == "check":
        return 0 if check(ROOT) else 1
    if args.command == "import":
        source = args.source
        if not source.is_absolute():
            source = WORKSPACE / source
        if not source.is_dir():
            parser.error(f"fixture source directory does not exist: {source}")
        imported = import_fixtures(ROOT, source, args.replace)
        return 0 if check(ROOT) and imported else 1
    try:
        weights = read_weights(args.rest_weight)
    except ValueError as error:
        parser.error(str(error))
    return 0 if collect(ROOT, args.replace, weights) else 1


if __name__ == "__main__":
    sys.exit(main())
