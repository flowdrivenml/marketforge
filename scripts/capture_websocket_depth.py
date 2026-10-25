#!/usr/bin/env python3

import asyncio
import json
import time
from pathlib import Path

import websockets

# ============================================================
# MARKETFORGE — WEBSOCKET DEPTH FIXTURE COLLECTOR
# ============================================================

OUTPUT = Path("../data/.cache/live/websocket-fixtures/depth")

TIMEOUT_SECONDS = 90
TARGET_MESSAGES = 12
MAX_MESSAGES = 30
CONCURRENCY = 3

# For snapshot-based channels, require at least one
# genuine snapshot and one genuine incremental update.
#
# REST-synchronized channels do not need a WS snapshot.

# ============================================================
# EXCHANGE PROFILES
# ============================================================

PROFILES = {
    # ========================================================
    # BINANCE — DIFF DEPTH
    # ========================================================
    "binance_spot": {
        "url": "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms",
        "subscribe": None,
        "family": "binance",
        "symbol": "BTCUSDT",
        "snapshot_required": False,
    },
    "binance_linear": {
        "url": "wss://fstream.binance.com/ws/btcusdt@depth@100ms",
        "subscribe": None,
        "family": "binance",
        "symbol": "BTCUSDT",
        "snapshot_required": False,
    },
    "binance_inverse": {
        "url": "wss://dstream.binance.com/ws/btcusd_perp@depth@100ms",
        "subscribe": None,
        "family": "binance",
        "symbol": "BTCUSD_PERP",
        "snapshot_required": False,
    },
    # ========================================================
    # BYBIT V5 — SNAPSHOT + DELTA
    # ========================================================
    "bybit_spot": {
        "url": "wss://stream.bybit.com/v5/public/spot",
        "subscribe": {
            "op": "subscribe",
            "args": ["orderbook.200.BTCUSDT"],
        },
        "family": "bybit",
        "symbol": "BTCUSDT",
        "topic": "orderbook.200.BTCUSDT",
        "snapshot_required": True,
    },
    "bybit_linear": {
        "url": "wss://stream.bybit.com/v5/public/linear",
        "subscribe": {
            "op": "subscribe",
            "args": ["orderbook.200.BTCUSDT"],
        },
        "family": "bybit",
        "symbol": "BTCUSDT",
        "topic": "orderbook.200.BTCUSDT",
        "snapshot_required": True,
    },
    "bybit_inverse": {
        "url": "wss://stream.bybit.com/v5/public/inverse",
        "subscribe": {
            "op": "subscribe",
            "args": ["orderbook.200.BTCUSD"],
        },
        "family": "bybit",
        "symbol": "BTCUSD",
        "topic": "orderbook.200.BTCUSD",
        "snapshot_required": True,
    },
    # ========================================================
    # BITGET V3 — BOOKS
    # ========================================================
    "bitget_spot": {
        "url": "wss://ws.bitget.com/v3/ws/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "instType": "spot",
                    "topic": "books",
                    "symbol": "BTCUSDT",
                }
            ],
        },
        "family": "bitget",
        "symbol": "BTCUSDT",
        "inst_type": "spot",
        "snapshot_required": True,
    },
    "bitget_linear": {
        "url": "wss://ws.bitget.com/v3/ws/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "instType": "usdt-futures",
                    "topic": "books",
                    "symbol": "BTCUSDT",
                }
            ],
        },
        "family": "bitget",
        "symbol": "BTCUSDT",
        "inst_type": "usdt-futures",
        "snapshot_required": True,
    },
    "bitget_inverse": {
        "url": "wss://ws.bitget.com/v3/ws/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "instType": "coin-futures",
                    "topic": "books",
                    "symbol": "BTCUSD_CM",
                }
            ],
        },
        "family": "bitget",
        "symbol": "BTCUSD_CM",
        "inst_type": "coin-futures",
        "snapshot_required": True,
    },
    # ========================================================
    # GATE.IO — INCREMENTAL DEPTH
    # ========================================================
    "gateio_spot": {
        "url": "wss://api.gateio.ws/ws/v4/",
        "subscribe": {
            "channel": "spot.order_book_update",
            "event": "subscribe",
            "payload": ["BTC_USDT", "100ms"],
        },
        "family": "gateio",
        "symbol": "BTC_USDT",
        "channel": "spot.order_book_update",
        "snapshot_required": False,
    },
    "gateio_linear": {
        "url": "wss://fx-ws.gateio.ws/v4/ws/usdt",
        "subscribe": {
            "channel": "futures.order_book_update",
            "event": "subscribe",
            "payload": ["BTC_USDT", "100ms", "100"],
        },
        "family": "gateio",
        "symbol": "BTC_USDT",
        "channel": "futures.order_book_update",
        "snapshot_required": False,
    },
    "gateio_inverse": {
        "url": "wss://fx-ws.gateio.ws/v4/ws/btc",
        "subscribe": {
            "channel": "futures.order_book_update",
            "event": "subscribe",
            "payload": ["BTC_USD", "100ms", "100"],
        },
        "family": "gateio",
        "symbol": "BTC_USD",
        "channel": "futures.order_book_update",
        "snapshot_required": False,
    },
    # ========================================================
    # OKX V5 — INCREMENTAL BOOKS
    # ========================================================
    "okx_spot": {
        "url": "wss://ws.okx.com:8443/ws/v5/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "channel": "books",
                    "instId": "BTC-USDT",
                }
            ],
        },
        "family": "okx",
        "symbol": "BTC-USDT",
        "snapshot_required": True,
    },
    "okx_linear": {
        "url": "wss://ws.okx.com:8443/ws/v5/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "channel": "books",
                    "instId": "BTC-USDT-SWAP",
                }
            ],
        },
        "family": "okx",
        "symbol": "BTC-USDT-SWAP",
        "snapshot_required": True,
    },
    "okx_inverse": {
        "url": "wss://ws.okx.com:8443/ws/v5/public",
        "subscribe": {
            "op": "subscribe",
            "args": [
                {
                    "channel": "books",
                    "instId": "BTC-USD-SWAP",
                }
            ],
        },
        "family": "okx",
        "symbol": "BTC-USD-SWAP",
        "snapshot_required": True,
    },
}


# ============================================================
# DEPTH MESSAGE CLASSIFICATION
# ============================================================


def classify_depth(profile, message):
    """
    Return:
        "snapshot" — genuine full-book message
        "delta"    — genuine incremental update
        None       — not a depth-data message

    Classification is intentionally conservative.

    Do not fabricate snapshot messages or infer message
    identity merely because a JSON object has bids/asks.
    """

    if not isinstance(message, dict):
        return None

    family = profile["family"]

    # ========================================================
    # BINANCE
    # ========================================================

    if family == "binance":
        if message.get("e") != "depthUpdate":
            return None

        if message.get("s") != profile["symbol"]:
            return None

        if not all(k in message for k in ("U", "u", "b", "a")):
            return None

        if not isinstance(message["b"], list):
            return None

        if not isinstance(message["a"], list):
            return None

        return "delta"

    # ========================================================
    # BYBIT
    # ========================================================

    if family == "bybit":
        if message.get("topic") != profile["topic"]:
            return None

        kind = message.get("type")

        if kind not in ("snapshot", "delta"):
            return None

        data = message.get("data")

        if not isinstance(data, dict):
            return None

        if data.get("s") != profile["symbol"]:
            return None

        if not all(k in data for k in ("u", "seq", "b", "a")):
            return None

        if not isinstance(data["b"], list):
            return None

        if not isinstance(data["a"], list):
            return None

        return kind

    # ========================================================
    # BITGET V3
    # ========================================================

    if family == "bitget":
        arg = message.get("arg")

        if not isinstance(arg, dict):
            return None

        if arg.get("topic") != "books":
            return None

        if arg.get("symbol") != profile["symbol"]:
            return None

        if arg.get("instType") != profile["inst_type"]:
            return None

        action = message.get("action")

        if action not in ("snapshot", "update"):
            return None

        records = message.get("data")

        if not isinstance(records, list) or not records:
            return None

        if not all(
            isinstance(record, dict)
            and isinstance(record.get("a"), list)
            and isinstance(record.get("b"), list)
            and "seq" in record
            for record in records
        ):
            return None

        return "snapshot" if action == "snapshot" else "delta"

    # ========================================================
    # GATE.IO
    # ========================================================

    if family == "gateio":
        if message.get("event") != "update":
            return None

        if message.get("channel") != profile["channel"]:
            return None

        result = message.get("result")

        if not isinstance(result, dict):
            return None

        if result.get("s") != profile["symbol"]:
            return None

        if not all(k in result for k in ("U", "u", "b", "a")):
            return None

        if not isinstance(result["b"], list):
            return None

        if not isinstance(result["a"], list):
            return None

        return "delta"

    # ========================================================
    # OKX V5
    # ========================================================

    if family == "okx":
        arg = message.get("arg")

        if not isinstance(arg, dict):
            return None

        if arg.get("channel") != "books":
            return None

        if arg.get("instId") != profile["symbol"]:
            return None

        action = message.get("action")

        if action not in ("snapshot", "update"):
            return None

        records = message.get("data")

        if not isinstance(records, list) or not records:
            return None

        if not all(
            isinstance(record, dict)
            and "seqId" in record
            and "prevSeqId" in record
            and isinstance(record.get("bids"), list)
            and isinstance(record.get("asks"), list)
            for record in records
        ):
            return None

        return "snapshot" if action == "snapshot" else "delta"

    return None


# ============================================================
# SERVER ERRORS AND PING HANDLING
# ============================================================


async def handle_control_message(ws, message):
    """
    Return True when the message was a control message.

    Control messages must never enter fixture files.
    """

    if not isinstance(message, dict):
        return False

    if message.get("event") == "error":
        raise RuntimeError(f"Exchange error: {message}")

    if message.get("op") == "subscribe":
        if message.get("success") is False:
            raise RuntimeError(f"Subscription rejected: {message}")

        if message.get("code") not in (None, "0", 0):
            raise RuntimeError(f"Subscription rejected: {message}")

        return True

    if message.get("event") in ("subscribe", "unsubscribe"):
        if message.get("error"):
            raise RuntimeError(f"Subscription error: {message}")

        if message.get("code") not in (None, "0", 0):
            raise RuntimeError(f"Subscription error: {message}")

        return True

    if message.get("event") == "notice":
        return True

    return False


# ============================================================
# CAPTURE ONE PROFILE
# ============================================================


async def capture(name, settings):
    print(f"\nCONNECTING {name}", flush=True)

    subscribe = settings["subscribe"]

    if subscribe is not None:
        subscribe = dict(subscribe)

        if settings["family"] == "gateio":
            subscribe["time"] = int(time.time())

    messages = []
    snapshot_count = 0
    delta_count = 0
    ignored_count = 0
    error = None

    try:
        async with asyncio.timeout(TIMEOUT_SECONDS):
            async with websockets.connect(
                settings["url"],
                open_timeout=12,
                ping_interval=20,
                max_size=16 * 1024 * 1024,
                max_queue=32,
            ) as ws:

                if subscribe is not None:
                    await ws.send(json.dumps(subscribe))

                while len(messages) < MAX_MESSAGES:
                    raw = await ws.recv()

                    if isinstance(raw, bytes):
                        ignored_count += 1
                        continue

                    # Some exchanges use textual application pings.
                    if raw == "ping":
                        await ws.send("pong")
                        continue

                    if raw == "pong":
                        continue

                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError:
                        ignored_count += 1
                        continue

                    if await handle_control_message(ws, message):
                        continue

                    kind = classify_depth(settings, message)

                    if kind is None:
                        ignored_count += 1
                        continue

                    # Preserve the complete native message.
                    messages.append(message)

                    if kind == "snapshot":
                        snapshot_count += 1
                    else:
                        delta_count += 1

                    enough_messages = len(messages) >= TARGET_MESSAGES

                    enough_kinds = delta_count > 0 and (
                        not settings["snapshot_required"] or snapshot_count > 0
                    )

                    if enough_messages and enough_kinds:
                        break

    except TimeoutError:
        error = "capture timeout"

    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    # ========================================================
    # EVALUATE CAPTURE
    # ========================================================

    enough_messages = len(messages) >= TARGET_MESSAGES

    enough_kinds = delta_count > 0 and (
        not settings["snapshot_required"] or snapshot_count > 0
    )

    if enough_messages and enough_kinds:
        status = "PASS"
    elif messages:
        status = "PARTIAL"
    else:
        status = "FAILED"

    # ========================================================
    # PERSIST COMPLETE NATIVE MESSAGES
    # ========================================================

    OUTPUT.mkdir(parents=True, exist_ok=True)

    if messages:
        destination = OUTPUT / f"{name}.json"
        temporary = OUTPUT / f"{name}.json.tmp"

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

    else:
        destination = None

    print(
        f"{status:<7} {name:<18} "
        f"messages={len(messages):<3} "
        f"snapshots={snapshot_count:<3} "
        f"deltas={delta_count:<3} "
        f"ignored={ignored_count:<3} "
        f"error={error or '-'}",
        flush=True,
    )

    return {
        "status": status,
        "family": settings["family"],
        "symbol": settings["symbol"],
        "messages": len(messages),
        "snapshots": snapshot_count,
        "deltas": delta_count,
        "ignored": ignored_count,
        "error": error,
        "path": str(destination) if destination else None,
    }


# ============================================================
# MAIN
# ============================================================


async def main():
    print("=" * 72)
    print("MARKETFORGE — WEBSOCKET DEPTH FIXTURE CAPTURE")
    print("=" * 72)

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def limited_capture(name, settings):
        async with semaphore:
            return await capture(name, settings)

    results = await asyncio.gather(
        *(limited_capture(name, settings) for name, settings in PROFILES.items())
    )

    report = dict(zip(PROFILES, results))

    OUTPUT.mkdir(parents=True, exist_ok=True)

    (OUTPUT / "capture-report.json").write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )

    passed = sum(result["status"] == "PASS" for result in results)

    partial = sum(result["status"] == "PARTIAL" for result in results)

    failed = sum(result["status"] == "FAILED" for result in results)

    print("\n" + "=" * 72)
    print("CAPTURE REPORT")
    print("=" * 72)

    for name, result in report.items():
        print(
            f"{name:<20} "
            f"{result['status']:<8} "
            f"snapshot={result['snapshots']:<3} "
            f"delta={result['deltas']:<3}"
        )

    print()
    print(f"Profiles: {len(results)}")
    print(f"Passed:   {passed}")
    print(f"Partial:  {partial}")
    print(f"Failed:   {failed}")
    print(f"Output:   {OUTPUT.resolve()}")

    if failed or partial:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
