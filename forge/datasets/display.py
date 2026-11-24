"""Compact, escaped notebook views using cached metadata."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from html import escape
from typing import TYPE_CHECKING

from .metadata import utc

if TYPE_CHECKING:
    from .catalog import DatasetCatalog
    from .dataset import Dataset, Stream

LIMIT = 12
CSS = """<style>
.mf-research {color:var(--jp-ui-font-color1,#26344a);background:var(--jp-layout-color0,#fff);
 border:1px solid var(--jp-border-color2,#aebbd0);border-inline-start:4px solid #567bb8;
 border-radius:8px;padding:1rem;margin:.6rem 0;font:14px/1.5 system-ui,sans-serif;
 color-scheme:light dark;}
.mf-research h3,.mf-research h4 {margin:0 0 .4rem;color:inherit;}
.mf-research h4 {margin-top:.8rem;}
.mf-research p {margin:.4rem 0;overflow-wrap:anywhere;}
.mf-research .mf-scroll {overflow-x:auto;max-width:100%;margin:.45rem 0;}
.mf-research table {border-collapse:collapse;width:100%;margin:0;text-align:left;color:inherit;}
.mf-research th,.mf-research td {padding:.38rem .55rem;border-bottom:1px solid var(--jp-border-color2,#aebbd0);
 text-align:left;vertical-align:top;white-space:normal;color:inherit;overflow-wrap:anywhere;}
.mf-research th {font-weight:650;background:var(--jp-layout-color2,#eaf0fa);}
.mf-research .mf-num {text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;}
.mf-research details {margin:.3rem 0 .15rem;overflow-wrap:anywhere;}
.mf-research summary {cursor:pointer;font-weight:600;}
.mf-research .mf-note {font-size:.9em;}
@media(prefers-color-scheme:dark) {
.mf-research {background:var(--jp-layout-color0,#182132);color:var(--jp-ui-font-color1,#eff4ff);}
.mf-research th {background:var(--jp-layout-color2,#26344e);}
}
</style>"""


def text(value: object) -> str:
    return escape("Unknown" if value is None else str(value))


def size(value: int) -> str:
    divisor = next((i for i in (3, 2, 1) if value >= 1024**i), 0)
    return (
        f"{value / 1024**divisor:,.1f} {('B', 'KiB', 'MiB', 'GiB')[divisor]}"
        if divisor
        else f"{value:,} B"
    )


def panel(title: str, body: str) -> str:
    return CSS + f'<section class="mf-research"><h3>{text(title)}</h3>{body}</section>'


def table(
    headers: Sequence[str], rows: Iterable[Sequence[object]], numeric: Sequence[int] = ()
) -> str:
    def cells(values: Sequence[object], tag: str) -> str:
        return "".join(
            f"<{tag}{' scope=col' if tag == 'th' else ''}{' class=mf-num' if index in numeric else ''}>"
            f"{text(value)}</{tag}>"
            for index, value in enumerate(values)
        )

    body = "".join("<tr>" + cells(row, "td") + "</tr>" for row in rows)
    return (
        '<div class="mf-scroll" tabindex="0" role="region" aria-label="Scrollable table"><table><thead><tr>'
        + cells(headers, "th")
        + "</tr></thead><tbody>"
        + body
        + "</tbody></table></div>"
    )


def span(lo: int | None, hi: int | None) -> str:
    if lo is None or hi is None:
        return "Unknown"
    return f"{utc(lo)} → {utc(hi)}"


def markets(streams: Sequence[Stream], key: str) -> str:
    values = list(
        dict.fromkeys(str((stream.identity or {}).get(key) or "Unknown") for stream in streams)
    )
    if not values:
        return "Unknown"
    return ", ".join(values[:3]) + (f" +{len(values) - 3}" if len(values) > 3 else "")


def catalog(result: DatasetCatalog) -> str:
    overview = result.overview()
    rows: list[Sequence[object]] = []
    details: list[str] = []
    for item in result.entries[:LIMIT]:
        streams = (
            item.matching_streams if item.matching_streams is not None else item.streams_by_key
        )
        fields = item.discovery_fields()
        label = fields["reference"]
        rows.append(
            (
                label,
                fields["reference_kind"],
                fields["exchange"],
                fields["market"],
                fields["instrument"],
                item.data_type,
                span(*item.observed),
                fields["files"],
                item.operation,
            )
        )
        match = (
            f"{len(streams)} matching of {len(item.streams_by_key)} known memberships. "
            if item.matching_streams is not None
            else ""
        )
        details.append(f"<p><strong>{text(label)}</strong> {text(match)}{text(item.path)}</p>")
    body = (
        f"<p>{text(result.scope)} · {len(result):,} files · {overview['publications']:,} publications · "
        f"{overview['known_streams']:,} known streams · {size(overview['bytes'])}</p>"
        f"<p class=mf-note>Observed UTC envelope {text(span(overview['observed_start_ns'], overview['observed_end_ns']))}. "
        f"Showing {len(rows)} of {len(result)} files. Counts describe whole files.</p>"
    )
    if result.filters:
        body += f"<details><summary>Applied filters ({len(result.filters)})</summary><p>{text(result.filters)}</p></details>"
    if rows:
        body += table(
            (
                "Reference",
                "Identity kind",
                "Exchange",
                "Market",
                "Instrument",
                "Data",
                "Observed UTC",
                "Files",
                "Stage",
            ),
            rows,
            (7,),
        )
        body += (
            "<details><summary>Paths and matching membership counts</summary>"
            + "".join(details)
            + "</details>"
        )
    else:
        body += "<p>No matching files.</p>"
    if result.diagnostics:
        body += f"<details><summary>Discovery diagnostics ({len(result.diagnostics)})</summary>"
        body += table(
            ("Path", "Status", "Reason"),
            [(issue.path, issue.code, issue.message) for issue in result.diagnostics[:LIMIT]],
        )
        body += f"<p>Showing {min(LIMIT, len(result.diagnostics))} of {len(result.diagnostics)}. See .diagnostics.</p></details>"
    return panel("MarketForge · Dataset catalog", body)


def handle_details(value: Dataset | Stream) -> str:
    from .dataset import Dataset

    if isinstance(value, Dataset):
        info = value.info()
        streams = value.streams_by_key
        selected = value.matching_streams
    else:
        info = (
            {**(value.context or {}), **value.info()}
            if value.footers
            else {
                "path": value.dataset_path,
                "membership_evidence": value.evidence,
            }
        )
        streams, selected = (value,), (value,)
    byte_count = info.get("bytes")
    fields = (
        "dataset_id",
        "registry_context",
        "publication_id",
        "engine_publication_id",
        "instrument_namespace",
        "path",
        "primary_file",
        "operation",
        "status",
        "integrity",
        "integrity_scope",
        "metadata_scope",
        "membership_evidence",
        "requested_interval",
        "timestamp_evidence",
        "reason",
    )
    return (
        f"<p>Physical rows <strong>{text(info.get('rows'))}</strong> · "
        f"row groups {text(info.get('row_groups'))} · "
        f"size {text(size(byte_count) if isinstance(byte_count, int) else None)}"
        + (
            f" · matching rows {text(info.get('matching_rows'))}"
            if not isinstance(value, Dataset)
            else ""
        )
        + f"</p><p>Observed UTC {text(span(*value.observed))}</p>"
        + table(
            ("Exchange", "Symbol", "Instrument type", "Category", "Content", "Selection"),
            [
                (
                    markets((stream,), "exchange"),
                    markets((stream,), "symbol"),
                    markets((stream,), "instrument_type"),
                    markets((stream,), "market_category"),
                    stream.kind,
                    "Selected" if selected is not None and stream in selected else "Available",
                )
                for stream in streams[:LIMIT]
            ],
        )
        + f"<p class=mf-note>{min(len(streams), LIMIT)} of {len(streams)} known memberships shown.</p>"
        + "<details><summary>File, publication and evidence</summary>"
        + table(("Field", "Value"), [(key.replace("_", " "), info.get(key)) for key in fields])
        + "</details>"
    )


def handle(value: Dataset | Stream) -> str:
    from .dataset import Dataset

    title = (
        value.path.name
        if isinstance(value, Dataset)
        else str((value.identity or {}).get("stream_id", value.kind))
    )
    return panel(f"MarketForge · {title}", handle_details(value))
