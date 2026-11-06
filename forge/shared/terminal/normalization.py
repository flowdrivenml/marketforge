"""Compact preparation summaries using the shared terminal palette."""

from rich.console import Console

from forge.markets.models import obj


def render_batch(console: Console, result: dict[str, object]) -> None:
    console.print("Normalization · prepare", style="mf.heading")
    console.print(f"Recipe: {result.get('recipe', 'unknown')}", markup=False)
    status = str(result["status"])
    console.print(
        f"Status: {status}" + (" · dry run" if result.get("dry_run") else ""),
        style="mf.warning" if status == "partial" else "mf.success",
    )
    console.print(
        f"Jobs: {result['prepared_job_count']} · Tasks: {result['prepared_task_count']}"
        f" · Streams: {result['prepared_stream_count']}",
    )
    if status == "partial":
        console.print("Preparation is incomplete; see report for details.", style="mf.warning")
    ranking = obj(result.get("ranking_resolution", {}))
    if ranking.get("decision") == "would_create":
        console.print(
            f"Ranking: {ranking['policy']} missing; preparation would refresh and build it. Planned stream order is provisional.",
            style="mf.warning",
        )
    elif ranking.get("decision") in {"reused", "created"}:
        console.print(
            f"Ranking: {ranking['decision']} {ranking['policy']} · {ranking['snapshot']}",
            style="mf.secondary",
        )
    console.print("Direct Rust execution unsupported for Forge batch v3.", style="mf.warning")
    suffix = " (not written)" if result.get("dry_run") else ""
    for label, key in (("Batch", "batch_path"), ("Report", "report_path")):
        console.print(f"{label}: {result[key]}{suffix}", style="mf.identifier", markup=False)
