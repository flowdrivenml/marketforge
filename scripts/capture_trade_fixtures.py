#!/usr/bin/env python3

import asyncio
import json
import time
from pathlib import Path

import websockets

# ============================================================
# MARKETFORGE WEBSOCKET TRADE FIXTURE COLLECTOR
# ============================================================

OUTPUT = Path("../data/.cache/live/websocket-fixtures/trades")

TIMEOUT_SECONDS = 180
TARGET_RECORDS = 10

# Capture one complete native trade-data message per profile.
# A message may contain multiple trades.

PROFILES = {
    # ========================================================
    # BINANCE
    # ========================================================
    "binance_spot": {
        "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
        "subscribe": None,
        "path": "$",
    },
    "binance_linear": {
        "url": "wss://fstream.binance.com/ws/btcusdt@trade",
        "subscribe": None,
        "path": "$",
    },
    "binance_inverse": {
        "url": "wss://dstream.binance.com/ws/btcusd_perp@trade",
        "subscribe": None,
        "path": "$",
    },
    # ========================================================
    # BYBIT V5
    # ========================================================
    "bybit_spot": {
        "url": "wss://stream.bybit.com/v5/public/spot",
        "subscribe": {
            "op": "subscribe",
            "args": ["publicTrade.BTCUSDT"],
        },
        "path": "data",
    },
    "bybit_linear": {
        "url": "wss://stream.bybit.com/v5/public/linear",
        "subscribe": {
            "op": "subscribe",
            "args": ["publicTrade.BTCUSDT"],
        },
        "path": "data",
    },
    "bybit_inverse": {
        "url": "wss://stream.bybit.com/v5/public/inverse",
        "subscribe": {
            "op": "subscribe",
            "args": ["publicTrade.BTCUSD"],
        },
        "path": "data",
    },
    # ========================================================
    # OKX
    # ========================================================
    "okx_spot": {
        "url": "wss://ws.okx.com/ws/v5/business",
        "subscribe": {
            "op": "subscribe",
            "args": [{"channel": "trades-all", "instId": "BTC-USDT"}],
        },
        "path": "data",
    },
    "okx_linear": {
        "url": "wss://ws.okx.com/ws/v5/business",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "channel": "trades-all",
                    "instId": "BTC-USDT-SWAP",
                }
            ],
        },
        "path": "data",
    },
    "okx_inverse": {
        "url": "wss://ws.okx.com/ws/v5/business",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "channel": "trades-all",
                    "instId": "BTC-USD-SWAP",
                }
            ],
        },
        "path": "data",
    },
    # ========================================================
    # BITGET V3
    # ========================================================
    "bitget_spot": {
        "url": "wss://ws.bitget.com/v3/ws/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "instType": "spot",
                    "topic": "publicTrade",
                    "symbol": "BTCUSDT",
                }
            ],
        },
        "path": "data",
    },
    "bitget_linear": {
        "url": "wss://ws.bitget.com/v3/ws/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "instType": "usdt-futures",
                    "topic": "publicTrade",
                    "symbol": "BTCUSDT",
                }
            ],
        },
        "path": "data",
    },
    "bitget_inverse": {
        "url": "wss://ws.bitget.com/v3/ws/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "instType": "coin-futures",
                    "topic": "publicTrade",
                    "symbol": "BTCUSD_CM",
                }
            ],
        },
        "path": "data",
    },
    # ========================================================
    # GATE.IO
    # ========================================================
    "gateio_spot": {
        "url": "wss://api.gateio.ws/ws/v4/",
        "subscribe": {
            "channel": "spot.trades",
            "event": "subscribe",
            "payload": ["BTC_USDT"],
        },
        "path": "result",
    },
    "gateio_linear": {
        "url": "wss://fx-ws.gateio.ws/v4/ws/usdt",
        "subscribe": {
            "channel": "futures.trades",
            "event": "subscribe",
            "payload": ["BTC_USDT"],
        },
        "path": "result",
    },
    "gateio_inverse": {
        "url": "wss://fx-ws.gateio.ws/v4/ws/btc",
        "subscribe": {
            "channel": "futures.trades",
            "event": "subscribe",
            "payload": ["BTC_USD"],
        },
        "path": "result",
    },
}


# ============================================================
# GENERIC JSON PATH EXTRACTION
# ============================================================


def get_path(value, path):
    if path == "$":
        return value

    for part in path.split("."):
        if not isinstance(value, dict):
            return None

        value = value.get(part)

    return value


# ============================================================
# DATA MESSAGE VALIDATION
# ============================================================


def is_trade_message(profile, message):
    """
    Reject subscription acknowledgments, heartbeats,
    and exchange errors.

    Accept only messages containing actual trade records.
    """

    name = profile["name"]

    if not isinstance(message, dict):
        return False

    if message.get("error"):
        raise RuntimeError(f"Exchange error: {message['error']}")

    if message.get("event") == "error":
        raise RuntimeError(f"Subscription error: {message}")

    if message.get("op") == "subscribe":
        if message.get("success") is False:
            raise RuntimeError(f"Subscription rejected: {message}")
    # ========================================================
    # GATE.IO — IGNORE SUBSCRIPTION ACKNOWLEDGMENTS
    # ========================================================

    if name.startswith("gateio_"):
        # A subscription acknowledgment may contain a
        # nonempty "result" object, but it is not trade data.
        #
        # Only actual trade updates are valid fixtures.

        if message.get("event") != "update":
            return False

        expected_channel = "spot.trades" if name == "gateio_spot" else "futures.trades"

        if message.get("channel") != expected_channel:
            return False

        records = message.get("result")

        if name == "gateio_spot":
            return (
                isinstance(records, dict)
                and "id" in records
                and "price" in records
                and "amount" in records
            )

        return (
            isinstance(records, list)
            and len(records) > 0
            and all(
                isinstance(record, dict)
                and "id" in record
                and "price" in record
                and "size" in record
                for record in records
            )
        )
    records = get_path(
        message,
        profile["path"],
    )

    if name.startswith("binance_"):
        return (
            message.get("e") == "trade"
            and isinstance(records, dict)
            and "t" in records
            and "p" in records
            and "q" in records
        )

    if not records:
        return False

    if isinstance(records, list):
        return all(isinstance(item, dict) for item in records) and len(records) > 0

    return isinstance(records, dict)


# ============================================================
# CAPTURE ONE PROFILE
# ============================================================


async def capture(name, settings):
    profile = {
        **settings,
        "name": name,
    }

    print(f"\nCONNECTING {name}", flush=True)

    subscribe = settings["subscribe"]

    if subscribe is not None:
        subscribe = dict(subscribe)

        if name.startswith("gateio_"):
            subscribe["time"] = int(time.time())

    messages = []
    total_records = 0

    try:
        async with asyncio.timeout(TIMEOUT_SECONDS):
            async with websockets.connect(
                settings["url"],
                open_timeout=12,
                ping_interval=20,
                max_size=8 * 1024 * 1024,
            ) as ws:

                if subscribe is not None:
                    await ws.send(json.dumps(subscribe))

                while total_records < TARGET_RECORDS:
                    raw = await ws.recv()

                    if isinstance(raw, bytes):
                        continue

                    if raw in ("ping", "pong"):
                        if raw == "ping":
                            await ws.send("pong")
                        continue

                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if not is_trade_message(
                        profile,
                        message,
                    ):
                        continue

                    records = get_path(
                        message,
                        settings["path"],
                    )

                    count = len(records) if isinstance(records, list) else 1

                    messages.append(message)
                    total_records += count

        print(
            f"CAPTURED {name}: "
            f"{len(messages)} messages, "
            f"{total_records} trade records",
            flush=True,
        )

    except TimeoutError:
        print(
            f"TIMEOUT {name}: " f"{total_records}/{TARGET_RECORDS} records",
            flush=True,
        )

    except Exception as exc:
        print(
            f"FAILED {name}: " f"{type(exc).__name__}: {exc}",
            flush=True,
        )

        return {
            "status": "FAILED",
            "error": str(exc),
            "records": total_records,
        }

    # ========================================================
    # PERSIST CAPTURED MESSAGES
    # ========================================================

    if not messages:
        return {
            "status": "FAILED",
            "records": 0,
        }

    OUTPUT.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination = OUTPUT / f"{name}.json"
    temporary = OUTPUT / f"{name}.json.tmp"

    # Store a batch of original native WebSocket messages.
    #
    # Do not merge trades across message boundaries.
    # Do not flatten native envelopes.
    temporary.write_text(
        json.dumps(
            messages,
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    temporary.replace(destination)

    status = "PASS" if total_records >= TARGET_RECORDS else "PARTIAL"

    print(
        f"{status} {name}: "
        f"{total_records} records across "
        f"{len(messages)} messages",
        flush=True,
    )

    return {
        "status": status,
        "records": total_records,
        "messages": len(messages),
        "path": str(destination),
    }


# ============================================================
# MAIN
# ============================================================


async def main():
    print("=" * 65)
    print("MARKETFORGE — WEBSOCKET TRADE FIXTURE CAPTURE")
    print("=" * 65)

    semaphore = asyncio.Semaphore(3)

    async def limited_capture(name, settings):
        async with semaphore:
            return await capture(name, settings)

    results = await asyncio.gather(
        *(limited_capture(name, settings) for name, settings in PROFILES.items())
    )

    report = dict(zip(PROFILES, results))

    OUTPUT.mkdir(
        parents=True,
        exist_ok=True,
    )

    (OUTPUT / "capture-report.json").write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )

    passed = sum(result["status"] == "PASS" for result in results)

    partial = sum(result["status"] == "PARTIAL" for result in results)

    failed = sum(result["status"] == "FAILED" for result in results)

    failed = len(results) - passed

    print("\n" + "=" * 65)
    print("CAPTURE REPORT")
    print("=" * 65)

    for name, result in report.items():
        print(f"{name:<22} {result['status']}")

    print()
    print(f"Profiles: {len(results)}")
    print(f"Partial:  {partial}")
    print(f"Passed:   {passed}")
    print(f"Failed:   {failed}")
    print(f"Output:   {OUTPUT.resolve()}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
