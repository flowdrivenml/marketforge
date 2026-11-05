"""Tokyo Night summaries for recipe batches; full diagnostics remain in the report."""

from rich.console import Console
from rich.table import Table

from forge.markets.models import array, obj


def render_batch(console: Console, result: dict[str, object]) -> None:
    console.print("MarketForge · Normalization preparation", style="mf.heading")
    status = str(result["status"])
    console.print(
        f"{result['prepared_job_count']} jobs · {status}"
        + (" · dry run" if result.get("dry_run") else ""),
        style="mf.warning" if status == "partial" else "mf.success",
    )
    console.print(str(result["run_directory"]), style="mf.identifier", markup=False)
    console.print(
        f"Ranking snapshot: {result['ranking_snapshot'] or 'none; all streams unranked'}",
        style="mf.secondary",
    )
    table = Table(
        "Job ID",
        "Content",
        "Tasks",
        "Streams",
        header_style="mf.heading",
        border_style="mf.secondary",
    )
    for value in array(result["job_summaries"]):
        job = obj(value)
        table.add_row(
            str(job["job_id"]),
            str(job["content_type"]),
            str(job["task_count"]),
            str(job["stream_count"]),
        )
    if table.row_count:
        console.print(table)
    for value in array(result["requests"]):
        request = obj(value)
        index = request["request_index"]
        coverage = obj(request["coverage"])
        if coverage.get("gap_count"):
            console.print(
                f"Request {index}: {coverage['gap_count']} nominal coverage gaps; see report.",
                style="mf.warning",
            )
        for blocker in array(request["blockers"]):
            item = obj(blocker)
            console.print(
                f"Request {index} · local:{item['local_id']}: {item['reason']}",
                style="mf.warning",
                markup=False,
            )
        for selected in array(request["selected_archives"]):
            item = obj(selected)
            if item["extends_requested_interval"]:
                console.print(
                    f"Request {index} · local:{item['local_id']}: whole archive [{item['start']}, {item['end_exclusive']}) extends recipe window",
                    style="mf.warning",
                    markup=False,
                )
    console.print(
        "Identifiers are unreserved testing IDs. Engine not executed.", style="mf.secondary"
    )
    console.print(
        f"Forge batch: {result['batch_path']}"
        + (" (not written)" if result.get("dry_run") else ""),
        markup=False,
    )
