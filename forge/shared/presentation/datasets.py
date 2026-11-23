"""Compact, Rich-free projections for processed dataset browsing."""

from typing import Any

from forge.datasets.query import utc

STAGES = {"normalize": "Normalized", "merge": "Merged", "convert": "Converted"}
KINDS = {"trade": "Trades", "l2": "Depth (L2)", "trade_l2": "Trades + Depth"}


def market(member: dict[str, Any]) -> str:
    kind = member.get("instrument_type")
    category = member.get("market_category")
    if not kind:
        return "Unknown"
    label = str(kind).replace("_", " ").title()
    return f"{label} · {str(category).title()}" if category and category != kind else label


def scope(members: list[dict[str, Any]]) -> tuple[str, str]:
    identities = sorted(
        {
            (
                str(m.get("exchange") or "?"),
                str(m.get("symbol") or m.get("instrument_id") or "?"),
                str(m.get("instrument_id") or ""),
                market(m),
            )
            for m in members
            if m.get("exchange") or m.get("symbol") or m.get("instrument_id")
        }
    )
    markets = sorted({market(m) for m in members})
    venue_labels = {"okx": "OKX", "xnas": "XNAS", "gateio": "Gate.io", "?": "Unknown exchange"}
    exchanges = {exchange for exchange, _, _, _ in identities if exchange != "?"}
    selected = identities[:1]
    if selected and len(identities) > 1:
        selected.append(
            next((item for item in identities if item[0] != selected[0][0]), identities[1])
        )
    labels: list[str] = []
    for exchange, symbol, _, market_label in selected:
        venue = venue_labels.get(exchange.lower(), exchange.title())
        short_symbol = symbol if len(symbol) <= 28 else f"{symbol[:27]}…"
        label = f"{venue} {short_symbol}"
        if len(selected) == 2 and selected[0][:2] == selected[1][:2]:
            label += f" ({market_label})"
        labels.append(label)
    preview = ", ".join(labels)
    if len(identities) > 2:
        preview += f" +{len(identities) - 2}"
    if len(exchanges) > 1:
        preview += f" · {len(exchanges)} exchanges"
    return preview or "Unknown", markets[0] if len(
        markets
    ) == 1 else "Mixed" if markets else "Unknown"


def period(row: dict[str, Any], *, precise: bool = False) -> str:
    def bound(value: int | None) -> str:
        if value is None:
            return "Unknown"
        timestamp = utc(value)
        return timestamp if precise else timestamp[:19].replace("T", " ")

    start = bound(row.get("start_timestamp_ns"))
    end = bound(row.get("end_timestamp_ns"))
    if not precise and len(start) == 19 and len(end) == 19 and start[:10] == end[:10]:
        end = end[11:]
    return f"{start} → {end}"


def integrity(value: Any) -> str:
    return {
        "pass": "Clean",
        "clean": "Clean",
        "verified": "Verified",
        "degraded": "Degraded",
        "fail": "Failed",
        "failed": "Failed",
        "mixed": "Mixed",
    }.get(str(value), "Unknown")


def file_summary(row: dict[str, Any]) -> dict[str, Any]:
    return row.get("summary") or {}


def count(value: int | None) -> str:
    return f"{value:,}" if value is not None else "Unknown"
