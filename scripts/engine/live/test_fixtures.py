import contextlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import fixtures


def sample(relative, identity=1):
    path = Path(relative)
    exchange = path.name.split("_", 1)[0]
    depth = "depth" in relative
    book = {"b": [["1", "2"]], "a": [["3", "4"]]}
    if relative.startswith("websocket"):
        if exchange == "binance":
            message = {"e": "depthUpdate" if depth else "trade", "s": "BTCUSDT", **book}
        elif exchange == "gateio":
            message = {"channel": "spot.order_book_update" if depth else "spot.trades",
                       "event": "update", "result": book}
        elif exchange == "bybit":
            message = {"topic": "orderbook.200.BTCUSDT" if depth else "publicTrade.BTCUSDT",
                       "data": book if depth else [{"i": identity}]}
        else:
            field = "channel" if exchange == "okx" else "topic"
            channel = "books" if depth else "trades-all" if exchange == "okx" else "publicTrade"
            message = {"arg": {field: channel}, "data": [book]}
        records = [dict(message, identity=identity) for _ in range(12 if depth else 1)]
        if depth and exchange in {"bybit", "okx", "bitget"}:
            field = "type" if exchange == "bybit" else "action"
            records[0][field] = "snapshot"
            for record in records[1:]:
                record[field] = "delta" if exchange == "bybit" else "update"
        return records

    profile = path.name.split("__")[0]
    bids_asks = {"bids": book["b"], "asks": book["a"]}
    count = 60 if profile == "bybit_spot" else 500 if exchange in {"binance", "bybit"} else 100
    rows = [{"id": identity}] * count
    if exchange == "bybit":
        return {"retCode": 0, "result": book if depth else {"list": rows}}
    if exchange == "bitget":
        return {"code": "00000", "data": book if depth else rows}
    if exchange == "okx":
        return {"code": "0", "data": [bids_asks] if depth else rows}
    return bids_asks if depth else rows


def write(root, relative, identity=1):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    original = json.dumps(sample(relative, identity), indent=1).encode() + b"\n"
    path.write_bytes(original)
    return original


class FixturesTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name)
        self.root = self.base / "destination"
        self.source = self.base / "legacy"

    def run_quietly(self, function, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            result = function(*args)
        return result, out.getvalue()

    def test_missing_and_invalid_json_are_reported(self):
        relative = fixtures.REQUIRED[0]
        write(self.root, relative)
        (self.root / relative).write_text("[NaN]")
        result, output = self.run_quietly(fixtures.check, self.root)
        self.assertFalse(result)
        self.assertIn(f"invalid: {relative}", output)
        self.assertIn(f"missing: {fixtures.REQUIRED[1]}", output)
        self.assertIn("WebSocket trades: 0/15", output)

    def test_complete_inventory_import_preserves_original_bytes(self):
        for relative in fixtures.REQUIRED:
            write(self.source, relative)
        valid, output = self.run_quietly(fixtures.import_fixtures, self.root, self.source, False)
        self.assertTrue(valid)
        self.assertIn("95 imported", output)
        self.assertTrue(self.run_quietly(fixtures.check, self.root)[0])
        for relative in fixtures.REQUIRED:
            self.assertEqual((self.root / relative).read_bytes(), (self.source / relative).read_bytes())

    def test_import_preserves_existing_then_replaces_only_with_valid_input(self):
        relative = fixtures.REQUIRED[0]
        old = write(self.root, relative)
        new = write(self.source, relative, 123)
        paths = [relative]
        self.assertTrue(self.run_quietly(fixtures.import_fixtures, self.root, self.source, False, paths)[0])
        self.assertEqual((self.root / relative).read_bytes(), old)
        self.assertTrue(self.run_quietly(fixtures.import_fixtures, self.root, self.source, True, paths)[0])
        self.assertEqual((self.root / relative).read_bytes(), new)
        (self.source / relative).write_text("{}")
        self.assertFalse(self.run_quietly(fixtures.import_fixtures, self.root, self.source, True, paths)[0])
        self.assertEqual((self.root / relative).read_bytes(), new)

    def test_import_accepts_legacy_target_families_without_substitution(self):
        name = "okx_spot__recent_trades.json"
        standalone = f"rest-fixtures/{name}"
        service = f"rest-service-fixtures/{name}"
        original = write(self.source, standalone)
        (self.source / standalone).parent.rename(self.source / "live-rest-fixtures")
        result, _ = self.run_quietly(
            fixtures.import_fixtures, self.root, self.source, False, [standalone, service],
        )
        self.assertFalse(result)
        self.assertEqual((self.root / standalone).read_bytes(), original)
        self.assertFalse((self.root / service).exists())

    def test_workspace_relative_import_from_another_directory(self):
        script = self.base / "workspace/scripts/engine/live/fixtures.py"
        script.parent.mkdir(parents=True)
        shutil.copyfile(fixtures.__file__, script)
        workspace = script.parents[3]
        relative = fixtures.REQUIRED[0]
        original = write(workspace / "legacy", relative)
        result = subprocess.run(
            [sys.executable, str(script), "import", "--from", "legacy"],
            cwd=self.base, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("1 imported", result.stdout)
        self.assertEqual((workspace / "data/tests/live" / relative).read_bytes(), original)

    def test_partial_collection_publishes_only_valid_staged_results(self):
        first, second, third = fixtures.REQUIRED[:3]
        previous = write(self.root, first)
        calls = []

        def failing_collector(command, *, cwd, env, check):
            calls.append(command)
            self.assertEqual(cwd, fixtures.WORKSPACE)
            self.assertFalse(check)
            stage = Path(env["MF_LIVE_FIXTURE_STAGE"])
            self.assertEqual(stage.parent, self.root)
            if command[-5] == "websocket::bulk::collect_websocket_fixtures":
                write(stage, second, 2)
                write(stage, third)
                (stage / third).write_text("[]")
            return subprocess.CompletedProcess(command, 101)

        result, output = self.run_quietly(fixtures.collect, self.root, True, {}, failing_collector)
        self.assertFalse(result)
        self.assertEqual(len(calls), 2)
        self.assertEqual((self.root / first).read_bytes(), previous)
        self.assertEqual(fixtures.inspect(self.root, second).status, "available")
        self.assertEqual(fixtures.inspect(self.root, third).status, "missing")
        self.assertIn("provide verified --rest-weight", output)
        self.assertFalse(list(self.root.glob(".capture-*")))

    def test_complete_collection_does_not_start_network_tests(self):
        for relative in fixtures.REQUIRED:
            write(self.root, relative)
        def unexpected(*args, **kwargs):
            self.fail("complete fixture set should skip collection")
        self.assertTrue(self.run_quietly(fixtures.collect, self.root, False, {}, unexpected)[0])

    def test_weight_validation(self):
        for value in ("binance_spot=0/60", "bybit_spot=10/5", "binance_inverse=1/0"):
            with self.assertRaises(ValueError):
                fixtures.read_weights([value])
        self.assertEqual(
            fixtures.read_weights(["binance_spot=10/5"]),
            {"binance_spot": "binance_spot=10/5"},
        )


if __name__ == "__main__":
    unittest.main()
