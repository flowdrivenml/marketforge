"""Human market/ranking views. Structured results and calculations live elsewhere."""

from collections import Counter, deque
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from time import monotonic
from types import TracebackType
from typing import Self

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.table import Table
from rich.text import Text

from forge.markets.models import array, obj
from forge.markets.progress import CollectionProgress, ScopeProgress


def numeric(value: object, *, compact: bool = True) -> str:
    if value is None:
        return "—"
    try:
        n = Decimal(str(value))
    except InvalidOperation:
        return "—"
    if not n.is_finite():
        return "—"
    if compact:
        if n != 0 and abs(n) < Decimal("0.000001"):
            return format(n.normalize(), ".8g")
        for suffix, divisor in (
            ("T", Decimal("1e12")),
            ("B", Decimal("1e9")),
            ("M", Decimal("1e6")),
            ("K", Decimal("1e3")),
        ):
            if abs(n) >= divisor:
                return format(n / divisor, ".3f").rstrip("0").rstrip(".") + suffix
    rendered = format(n, ",f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def amount(value: object, unit: object, *, estimated: bool = False, compact: bool = True) -> str:
    text = numeric(value, compact=compact)
    return (
        text
        if value is None
        else text + (" " + str(unit) if unit else "") + (" (est.)" if estimated else "")
    )


def timestamp(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        return datetime.fromtimestamp(value // 1_000_000_000, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    return str(value)


def freshness(row: dict[str, object]) -> str:
    if row.get("fetched_at") is None:
        return "Not collected"
    return "Stale" if row.get("stale") else "Fresh"


def state(value: object) -> Text:
    text = str(value)
    style = (
        "mf.success"
        if text in {"Fresh", "complete", "valid", "active"}
        else "mf.failure"
        if text in {"failed", "cancelled"}
        else "mf.secondary"
        if text in {"Not collected", "—"}
        else "mf.warning"
    )
    return Text(text, style=style)


def table(*columns: str) -> Table:
    t = Table(box=None, padding=(0, 1), header_style="mf.heading", expand=False)
    for column in columns:
        t.add_column(column, overflow="fold")
    return t


def render_market_result(
    console: Console,
    action: str,
    value: dict[str, object],
    *,
    filters: str = "",
    exchange_filtered: bool = False,
    market_filtered: bool = False,
) -> None:
    console.print(
        Text("Markets · " + action + (" · " + filters if filters else ""), style="mf.heading")
    )
    if action in {"sync", "refresh"}:
        sources = [obj(v) for v in array(value["sources"])]
        label = "Metadata records" if action == "sync" else "Observations"
        t = table("Exchange", "Market", "Outcome", label, "Dated futures")
        for r in sources:
            counts = obj(r.get("instrument_counts", {}))
            t.add_row(
                str(r["exchange"]),
                str(r["market"]),
                state(r["status"]),
                str(r.get("collected", r.get("observations", 0))),
                str(counts.get("future", 0)),
            )
        console.print(t)
        console.print(state(value["status"]))
        counts = Counter(str(r["status"]) for r in sources)
        console.print(
            f"Scopes {len(sources)} · successful {counts['complete']} · failed {counts['failed']} · cancelled {counts['cancelled']}"
        )
        if any(
            r["status"] == "complete" and r.get("collected", r.get("observations", 0)) == 0
            for r in sources
        ):
            console.print(
                Text(
                    "Empty successful scopes had no matching current instruments; retained historical records remain unchanged.",
                    style="mf.secondary",
                )
            )
        for r in sources:
            if r.get("error"):
                console.print(
                    Text(f"{r['exchange']}.{r['market']}: {r['error']}", style="mf.failure")
                )
        return
    rows = [obj(v) for v in array(value["instruments"])]
    if action == "show" and rows:
        r = rows[0]
        interpretation = obj(r.get("interpretation") or {})
        console.print(Text(str(r["reference"]), style="mf.identifier"))
        groups: list[tuple[str, list[tuple[str, object]]]] = [
            (
                "Identity and trading status",
                [
                    ("Exchange", r.get("exchange")),
                    (
                        "Instrument / market",
                        f"{r.get('instrument_type')} / {r.get('market_category')}",
                    ),
                    ("Status", r.get("status")),
                    (
                        "Base / quote / settlement",
                        f"{r.get('base_asset') or '—'} / {r.get('quote_asset') or '—'} / {r.get('settlement_asset') or '—'}",
                    ),
                ],
            ),
            (
                "Instrument / contract specifications",
                [
                    ("Listed", timestamp(r.get("launch_time_ns"))),
                    (
                        "Expiry",
                        timestamp(r.get("expiry_ns"))
                        if r.get("instrument_type") == "future"
                        else "Not a dated contract",
                    ),
                    ("Quantity denomination", r.get("quantity_type")),
                    (
                        "Contract value",
                        amount(
                            r.get("contract_value"), r.get("contract_value_asset"), compact=False
                        ),
                    ),
                    ("Tick size", numeric(r.get("tick_size"), compact=False)),
                    ("Quantity step", numeric(r.get("qty_step"), compact=False)),
                    (
                        "Min / max quantity",
                        f"{numeric(r.get('min_qty'), compact=False)} / {numeric(r.get('max_qty'), compact=False)}",
                    ),
                    (
                        "Minimum notional",
                        amount(r.get("min_notional"), r.get("quote_asset"), compact=False),
                    ),
                ],
            ),
            (
                "Current stored statistics",
                [
                    (
                        "Last price",
                        amount(r.get("last_price"), r.get("quote_asset"), compact=False),
                    ),
                    (
                        "24h native turnover",
                        amount(
                            r.get("turnover_24h"),
                            r.get("turnover_denomination"),
                            estimated=bool(r.get("turnover_estimated")),
                            compact=False,
                        ),
                    ),
                    (
                        "Base / quote volume",
                        f"{amount(r.get('volume_24h_base'), r.get('base_asset'), compact=False)} / {amount(r.get('volume_24h_quote'), r.get('quote_asset'), compact=False)}",
                    ),
                    (
                        "Contract volume",
                        amount(r.get("volume_24h_contracts"), "contracts", compact=False),
                    ),
                    (
                        "Open interest",
                        amount(r.get("open_interest"), r.get("open_interest_unit"), compact=False),
                    ),
                    ("Funding rate (fraction)", numeric(r.get("funding_rate"), compact=False)),
                ],
            ),
            (
                "Observation timestamps",
                [
                    ("Freshness", freshness(r)),
                    ("Source observation", timestamp(r.get("source_observed_at"))),
                    ("Fetched", timestamp(r.get("fetched_at"))),
                    ("24h window start", timestamp(r.get("window_start"))),
                    ("24h window end", timestamp(r.get("window_end"))),
                    (
                        "Interval basis",
                        str(
                            obj(r.get("interpretation") or {}).get(
                                "interval_basis", "legacy; not retained"
                            )
                        ),
                    ),
                    (
                        "Activity scope",
                        interpretation.get("activity_scope", "legacy; not retained"),
                    ),
                    (
                        "Selected turnover field",
                        interpretation.get("turnover_field", "legacy; not retained"),
                    ),
                    (
                        "Turnover calculation",
                        interpretation.get("turnover_kind", "legacy; not retained"),
                    ),
                    (
                        "Base volume scope",
                        interpretation.get("base_volume_scope", "legacy; not retained"),
                    ),
                    ("Missing turnover reason", interpretation.get("turnover_missing_reason", "—")),
                    (
                        "Price time basis",
                        interpretation.get("price_timestamp_basis", "legacy; not retained"),
                    ),
                    ("Mapping", str(r.get("mapping_version") or "—")),
                ],
            ),
        ]
        for heading, fields in groups:
            console.print(Text(heading, style="mf.heading"))
            t = table("Field", "Value")
            for label, item in fields:
                t.add_row(
                    label,
                    state(item)
                    if label == "Freshness"
                    else Text("—", style="mf.secondary")
                    if item is None or item == "—"
                    else str(item),
                )
            console.print(t)
        return
    if not rows:
        console.print(
            "No stored instruments match. Run marketforge markets sync --testing to collect metadata."
        )
    else:
        narrow = console.width < 90
        columns = (
            ["Instrument", "Stored statistics"]
            if narrow
            else [
                "Instrument",
                *([] if exchange_filtered else ["Exchange"]),
                "Type" if market_filtered else "Type / market",
                "Last price",
                "Native 24h turnover",
                "Statistics",
                "Status",
            ]
        )
        t = table(*columns)
        for r in rows:
            identity = Text(str(r.get("symbol", r["reference"])), style="mf.identifier")
            kind = f"{r.get('instrument_type', '—')}/{r.get('market_category', '—')}"
            price = amount(r.get("last_price"), r.get("quote_asset"))
            turnover = amount(
                r.get("turnover_24h"),
                r.get("turnover_denomination"),
                estimated=bool(r.get("turnover_estimated")),
            )
            if narrow:
                secondary = str(r.get("instrument_type", "—")) if market_filtered else kind
                if not exchange_filtered:
                    secondary = str(r.get("exchange", "")) + " " + secondary
                identity.append("\n" + secondary, style="mf.secondary")
                t.add_row(
                    identity,
                    Text.assemble(
                        "Price ",
                        Text(
                            price,
                            style="mf.secondary" if r.get("last_price") is None else "mf.body",
                        ),
                        "\n24h ",
                        Text(
                            turnover,
                            style="mf.secondary"
                            if r.get("turnover_24h") is None
                            else "mf.warning"
                            if r.get("turnover_estimated")
                            else "mf.body",
                        ),
                        "\n",
                        state(freshness(r)),
                        " · " + str(r.get("status") or "—"),
                    ),
                )
            else:
                t.add_row(
                    identity,
                    *([] if exchange_filtered else [str(r.get("exchange", "—"))]),
                    str(r.get("instrument_type", "—")) if market_filtered else kind,
                    Text(price, style="mf.secondary" if r.get("last_price") is None else "mf.body"),
                    Text(
                        turnover,
                        style="mf.secondary"
                        if r.get("turnover_24h") is None
                        else "mf.warning"
                        if r.get("turnover_estimated")
                        else "mf.body",
                    ),
                    state(freshness(r)),
                    str(r.get("status") or "—"),
                )
        console.print(t)
    console.print(
        f"Displayed {len(rows)} instruments"
        + (
            " · limit reached; more results exist. Increase --limit or narrow filters."
            if value.get("truncated")
            else "."
        )
    )
    console.print(
        Text(
            "Native denominations are separate comparison groups; est. denotes estimated turnover. Freshness threshold: 1 hour.",
            style="mf.secondary",
        )
    )


def render_ranking_result(
    console: Console,
    action: str,
    value: dict[str, object],
    *,
    limit: int | None = 50,
    excluded: bool = False,
    conversion: str | None = None,
) -> None:
    console.print(Text("Markets · ranking " + action, style="mf.heading"))
    if action == "new":
        console.print("Created policy: " + str(value["configuration"]))
        return
    if action == "validate":
        policy = obj(value["policy"])
        if policy["metric"] == "volume":
            console.print(
                Text(
                    "Valid policy · volume · base-asset quantities grouped by base asset",
                    style="mf.success",
                )
            )
            return
        console.print(
            Text(
                f"Valid policy · {policy['metric']} · comparison {policy['comparison_currency']}",
                style="mf.success",
            )
        )
        cfg = obj(policy.get("conversion", {}))
        console.print(
            "Conversion: "
            + (
                "automatic stored spot reference prices"
                if cfg.get("automatic")
                else "automatic disabled; manual rates / explicit equivalence only"
            )
        )
        if cfg.get("equivalent_currencies"):
            console.print(
                Text(
                    "Explicit 1:1 assumptions: "
                    + ", ".join(str(v) for v in array(cfg["equivalent_currencies"]))
                    + " == "
                    + str(policy["comparison_currency"]),
                    style="mf.warning",
                )
            )
        return
    if action == "build":
        console.print(Text("Created snapshot " + str(value["snapshot_id"]), style="mf.success"))
        console.print(
            "Inspect: marketforge markets rank show " + str(value["snapshot_id"]) + " --testing"
        )
        return
    if action == "list":
        rows = [obj(r) for r in array(value["rankings"])]
        if not rows:
            console.print(
                "No published rankings. Build one with: marketforge markets rank build NAME --testing"
            )
            return
        narrow = console.width < 110
        t = table(
            *(
                ["Policy", "Created UTC", "Ccy", "In / out", "Universe"]
                if narrow
                else [
                    "Policy",
                    "Created (UTC)",
                    "Currency",
                    "Included / excluded",
                    "Universe",
                    "Snapshot ID",
                ]
            )
        )
        for r in rows:
            included = int(str(r["included_count"]))
            total = int(str(r["universe_count"]))
            t.add_row(
                str(r["policy_name"]),
                timestamp(r["created_at"]),
                str(r["comparison_currency"]),
                f"{included} / {total - included}",
                state("partial" if r["partial_universe"] else "complete"),
                *([] if narrow else [Text(str(r["id"]), style="mf.identifier")]),
            )
        console.print(t)
        if narrow:
            console.print(Text("Snapshot IDs (table order):", style="mf.secondary"))
            for row in rows:
                console.print(Text(str(row["id"]), style="mf.identifier"), soft_wrap=True)
        console.print(
            f"Displayed {len(rows)} snapshots"
            + (" · more exist; increase --limit." if value.get("truncated") else ".")
        )
        return
    header = obj(value["snapshot"])
    rows = [obj(r) for r in array(value["entries"])]
    included_rows = [r for r in rows if r["position"] is not None]
    exclusions = [r for r in rows if r["position"] is None]
    policy = obj(header["policy_json"])
    sources = {
        str(source) for row in rows for source in obj(row.get("metadata_sources", {})).values()
    }
    if "current_catalogue" in sources:
        console.print(
            Text(
                "Some older entries use current catalogue metadata for symbol/assets; JSON metadata_sources identifies each field.",
                style="mf.warning",
            )
        )
    elif sources:
        console.print(
            Text(
                "Symbol/assets use publication or retained observation metadata.",
                style="mf.secondary",
            )
        )
    if policy.get("metric") == "volume":
        render_volume_snapshot(console, value, limit=limit, excluded=excluded, evidence=conversion)
        return
    currency = str(header["comparison_currency"])
    console.print(Text(f"{header['policy_name']} · {header['id']}", style="mf.identifier"))
    console.print(
        f"Created {timestamp(header['created_at'])} · {policy.get('metric', 'turnover_24h')} · comparison {currency}"
    )
    console.print(
        f"Included {header['included_count']} · excluded {int(str(header['universe_count'])) - int(str(header['included_count']))} · universe {header['universe_count']} · "
        + ("PARTIAL universe" if header["partial_universe"] else "complete universe")
    )
    for key in ("base_asset", "quote_asset", "match"):
        if value.get(key) is not None:
            console.print(Text(f"Filter {key}: {value[key]}", style="mf.secondary"))
    if value.get("instrument_type"):
        console.print(
            "Instrument type: "
            + str(value["instrument_type"])
            + f" · matching {len(included_rows)} ranked, {len(exclusions)} excluded · snapshot ranks preserved"
        )
    console.print(
        Text("Current priorities, not historical backtest observations.", style="mf.secondary")
    )
    console.print(
        f"Turnover: {sum(bool(r.get('turnover_estimated')) for r in included_rows)} volume-derived estimates · est. is separate from currency valuation."
    )
    summary = obj(value.get("conversion_summary", {}))
    known = ("identity", "direct", "bridged", "manual", "equivalence")
    if not summary or (summary.get("legacy") and not any(summary.get(method) for method in known)):
        console.print(
            Text(
                "Legacy snapshot: conversion evidence was not retained; no path or rate is inferred.",
                style="mf.warning",
            )
        )
    else:
        console.print(
            " · ".join(f"{method} {summary.get(method, 0)}" for method in (*known, "excluded"))
        )
        if summary.get("equivalence"):
            console.print(
                Text(
                    "Equivalence entries use explicit policy assumptions, not observed market rates.",
                    style="mf.warning",
                )
            )
        if summary.get("legacy"):
            console.print(
                Text(
                    f"Legacy {summary['legacy']}: no conversion provenance was retained.",
                    style="mf.warning",
                )
            )
    if header.get("algorithm_version") == "turnover24h-spotfx-rational80-v2":
        console.print(
            Text(
                "Reference-price valuation of aggregate turnover; not execution-time FX. Inspect --conversion REFERENCE or RANK.",
                style="mf.secondary",
            )
        )
    t = table(
        "Rank", "Exchange", "Type / market", "Instrument", f"Comparable turnover ({currency})"
    )
    for r in included_rows[:limit]:
        t.add_row(
            str(r["position"]),
            str(r.get("exchange", "—")),
            f"{r.get('instrument_type', '—')}/{r.get('market_category', '—')}",
            Text(str(r.get("symbol", r["instrument_reference"])), style="mf.identifier"),
            numeric(r["score"]) + (" (est.)" if r.get("turnover_estimated") else ""),
        )
    console.print(t)
    console.print(
        f"Displayed {len(included_rows[:limit])} of {len(included_rows)} ranked entries. Use --all for every row."
    )
    for reason, count in sorted(Counter(str(r["exclusion_reason"]) for r in exclusions).items()):
        console.print(Text(f"{count} excluded: {reason}", style="mf.warning"))
    if exclusions and not excluded:
        console.print("Inspect individual exclusions with --excluded [--all].")
    if excluded:
        t = table("Excluded instrument", "Reason")
        for r in exclusions[:limit]:
            t.add_row(str(r["instrument_reference"]), str(r["exclusion_reason"]))
        console.print(t)
        console.print(f"Displayed {len(exclusions[:limit])} of {len(exclusions)} exclusions.")

    if conversion is not None:
        selected = next(
            (
                r
                for r in rows
                if str(r["position"]) == conversion or r["instrument_reference"] == conversion
            ),
            None,
        )
        if selected is None:
            from forge.markets.models import MarketDataError

            raise MarketDataError("Entry not found in this pinned ranking snapshot.")
        console.print(
            Text(
                "Conversion evidence · " + str(selected["instrument_reference"]), style="mf.heading"
            )
        )
        console.print(
            "Native "
            + amount(selected.get("native_turnover"), selected.get("native_unit"), compact=False)
            + " · turnover observation "
            + str(selected.get("observation_id"))
        )
        console.print("Score " + amount(selected.get("score"), currency, compact=False))
        console.print(
            "Native turnover: "
            + (
                "volume-derived estimate"
                if selected.get("turnover_estimated")
                else "not flagged as volume-derived estimate"
            )
        )
        native_evidence = selected.get("turnover_evidence")
        if native_evidence is None:
            console.print("Legacy: scope/field provenance not retained.")
        else:
            native = obj(native_evidence)
            console.print(
                "Activity "
                + str(native.get("activity_scope", "unknown"))
                + " · field "
                + str(native.get("turnover_field", "unknown"))
            )
            console.print(
                "Interval basis "
                + str(native.get("interval_basis", "unknown"))
                + " · price time "
                + str(native.get("price_timestamp_basis", "unknown"))
            )
            if native.get("turnover_missing_reason"):
                console.print(Text(str(native["turnover_missing_reason"]), style="mf.warning"))
        console.print(
            "Source "
            + timestamp(selected.get("source_observed_at"))
            + " · fetched "
            + timestamp(selected.get("fetched_at"))
        )
        console.print(
            "Interval "
            + timestamp(selected.get("window_start"))
            + " -> "
            + timestamp(selected.get("window_end"))
        )
        evidence = selected.get("conversion_evidence")
        if evidence is None:
            console.print(
                Text(
                    str(
                        selected.get("exclusion_reason")
                        or "Legacy snapshot: no retained conversion evidence; no rate is inferred."
                    ),
                    style="mf.warning",
                )
            )
        else:
            fx = obj(evidence)
            console.print(
                "Method "
                + str(fx["method"])
                + " · path "
                + " -> ".join(str(v) for v in array(fx["path"]))
            )
            console.print(
                "Rate "
                + str(fx["rate"])
                + " · exact ratio "
                + str(fx["numerator"])
                + "/"
                + str(fx["denominator"])
            )
            console.print("Formula: " + str(fx["score_formula"]))
            if fx.get("source"):
                console.print(
                    "Source/assumption: "
                    + str(fx["source"])
                    + " · as of "
                    + timestamp(fx.get("as_of"))
                )
            for leg in array(fx["legs"]):
                item = obj(leg)
                console.print(
                    Text(
                        str(item["instrument_reference"])
                        + " · "
                        + str(item["exchange"])
                        + " · observation "
                        + str(item["observation_id"]),
                        style="mf.identifier",
                    )
                )
                console.print(
                    str(item["from_currency"])
                    + " -> "
                    + str(item["to_currency"])
                    + " · "
                    + ("inverse 1/price" if item["inverted"] else "forward price")
                    + " · original price "
                    + str(item["original_price"])
                )
                console.print(
                    "Observed "
                    + timestamp(item.get("effective_observed_at"))
                    + " · fetched "
                    + timestamp(item.get("fetched_at"))
                )


class _CurrentCollection:
    def __init__(self, display: "MarketCollectionDisplay") -> None:
        self.display = display

    def __rich__(self) -> RenderableType:
        return self.display.render()


class MarketCollectionDisplay:
    """One renderer; 4 Hz elapsed refresh, bounded scopes/results, failure isolation."""

    def __init__(self, console: Console, *, silent: bool = False) -> None:
        self.console = console
        self.silent = silent
        self.snapshot: CollectionProgress | None = None
        self.started = monotonic()
        self.live: Live | None = None
        self.disabled = False
        self.reported: set[tuple[str, str]] = set()
        self.recent: deque[ScopeProgress] = deque(maxlen=6)

    def __enter__(self) -> Self:
        if not self.silent and self.console.file.isatty():
            try:
                self.live = Live(
                    _CurrentCollection(self),
                    console=self.console,
                    refresh_per_second=4,
                    transient=True,
                )
                self.live.start()
            except Exception:
                self._stop()
                self.disabled = True
        return self

    def _stop(self) -> None:
        if self.live is not None:
            try:
                self.live.stop()
            except Exception:
                pass
            self.live = None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop()

    def update(self, snapshot: CollectionProgress) -> None:
        self.snapshot = snapshot
        newly_finished: list[ScopeProgress] = []
        for scope in snapshot.scopes:
            key = (scope.exchange, scope.market)
            if scope.status in {"complete", "failed", "cancelled"} and key not in self.reported:
                self.reported.add(key)
                self.recent.append(scope)
                newly_finished.append(scope)
        if self.silent or self.disabled:
            return
        try:
            if self.live is not None:
                self.live.update(_CurrentCollection(self))
            else:
                for s in newly_finished:
                    self.console.print(
                        f"{s.exchange}.{s.market}: {s.status} · {s.collected} "
                        + ("metadata records" if snapshot.metadata else "observations")
                    )
        except Exception:
            self._stop()
            self.disabled = True

    def render(self) -> RenderableType:
        s = self.snapshot
        if s is None:
            return Text("Markets · starting collection", style="mf.heading")
        counts = Counter(scope.status for scope in s.scopes)
        active = (
            ", ".join(
                f"{r.exchange}.{r.market} ({r.collected})"
                for r in s.scopes
                if r.status == "running"
            )
            or "—"
        )
        recent = list(self.recent)
        return Group(
            Text(
                "Markets · " + ("metadata sync" if s.metadata else "statistics refresh"),
                style="mf.heading",
            ),
            Text(
                f"Scopes {s.finished}/{len(s.scopes)} · successful {counts['complete']} · failed {counts['failed']} · cancelled {counts['cancelled']}"
            ),
            Text("Active: " + active, style="mf.identifier"),
            Text(
                "Recent: "
                + "; ".join(f"{r.exchange}.{r.market} {r.status} ({r.collected})" for r in recent)
            ),
            Text(
                f"Elapsed {monotonic() - self.started:.0f}s · Ctrl+C to cancel",
                style="mf.secondary",
            ),
        )


def render_workflow_result(console: Console, title: str, value: dict[str, object]) -> None:
    """Summarize independently committed collection stages and ranking publications."""
    console.print(Text("Markets · " + title + " · " + str(value["status"]), style="mf.heading"))
    stages = [obj(stage) for stage in array(value.get("stages", []))]
    if "refresh" in value:
        stages.append({"stage": "statistics", **obj(value["refresh"])})
    for stage in stages:
        console.print(
            Text(
                str(stage["stage"]) + ": " + str(stage["status"]),
                style="mf.success" if stage["status"] == "complete" else "mf.warning",
            )
        )
        if stage.get("error"):
            console.print(Text(str(stage["error"]), style="mf.failure"))
        for source in array(stage.get("sources", [])):
            scope = obj(source)
            if scope.get("status") != "complete":
                console.print(
                    Text(
                        f"  {scope['exchange']}.{scope['market']}: {scope['status']} · "
                        + str(scope.get("error", "collection did not complete")),
                        style="mf.failure",
                    )
                )
    for item in array(value.get("rankings", [])):
        outcome = obj(item)
        message = str(outcome["policy"]) + ": " + str(outcome["status"])
        if outcome["status"] == "published":
            message += " · " + str(outcome["snapshot_id"])
            message += " · " + (
                "PARTIAL coverage" if outcome["partial_universe"] else "complete coverage"
            )
        else:
            message += " · " + str(outcome.get("error", "unknown failure"))
        console.print(
            Text(message, style="mf.success" if outcome["status"] == "published" else "mf.failure")
        )


def render_volume_snapshot(
    console: Console,
    value: dict[str, object],
    *,
    limit: int | None,
    excluded: bool,
    evidence: str | None,
) -> None:
    header = obj(value["snapshot"])
    rows = [obj(row) for row in array(value["entries"])]
    included = [row for row in rows if row["position"] is not None]
    exclusions = [row for row in rows if row["position"] is None]
    console.print(Text(f"{header['policy_name']} · {header['id']}", style="mf.identifier"))
    console.print("Created " + timestamp(header["created_at"]) + " · 24h base-asset volume")
    console.print("PARTIAL coverage" if header["partial_universe"] else "Complete coverage")
    console.print(
        "Quantities compare within each base asset. Group ranks are preserved; storage positions do not compare assets."
    )
    filters = [
        str(value[key])
        for key in ("base_asset", "quote_asset", "match", "instrument_type")
        if value.get(key)
    ]
    if filters:
        console.print("Filters: " + ", ".join(filters))
    t = table(
        "Base asset", "Group rank", "Exchange", "Type / market", "Instrument", "24h base quantity"
    )
    for row in included[:limit]:
        t.add_row(
            str(row["base_asset"]),
            str(row["group_rank"]),
            str(row.get("exchange", "—")),
            f"{row.get('instrument_type', '—')}/{row.get('market_category', '—')}",
            Text(str(row.get("symbol", row["instrument_reference"])), style="mf.identifier"),
            amount(row["score"], row["volume_unit"], estimated=bool(row["volume_estimated"])),
        )
    console.print(t)
    console.print(
        f"Displayed {len(included[:limit])} of {len(included)} matching ranked entries. est. denotes reference-price derivation."
    )
    for reason, count in sorted(
        Counter(str(row["exclusion_reason"]) for row in exclusions).items()
    ):
        console.print(Text(f"{count} excluded: {reason}", style="mf.warning"))
    if excluded:
        t = table("Base asset", "Excluded instrument", "Reason")
        for row in exclusions[:limit]:
            t.add_row(
                str(row["base_asset"] or "—"),
                str(row["instrument_reference"]),
                str(row["exclusion_reason"]),
            )
        console.print(t)
    if evidence is not None:
        import json

        selected = next(
            (
                row
                for row in rows
                if str(row["position"]) == evidence or row["instrument_reference"] == evidence
            ),
            None,
        )
        if selected is not None:
            console.print("Retained volume derivation evidence:")
            console.print(
                json.dumps(selected["volume_evidence"], default=str, indent=2), markup=False
            )


def render_ranking_deletion(console: Console, value: dict[str, object]) -> None:
    console.print(f"Markets · Ranking deletion · {value['action']}", style="mf.heading")
    console.print(
        f"Matched: {value['matched_count']} · Eligible: {value['eligible_count']} · Deleted: {value['deleted_count']} · Blocked: {value['blocked_count']}"
    )
    for item in array(value["snapshots"]):
        row = obj(item)
        console.print(f"{row['policy_name']} · {row['id']}", style="mf.identifier", markup=False)
        output = table("Created", "Included / excluded", "Eligibility / result")
        output.add_row(
            str(row["created_at"]),
            f"{row['included_count']} / {row['excluded_count']}",
            str(row["result"]),
        )
        console.print(output)
        for blocker in array(row["blockers"]):
            console.print(f"Blocked: {blocker}", style="mf.warning", markup=False)
    console.print(str(value["message"]), style="mf.secondary")
