"""Read-only run inspection treats missing/interrupted artifacts honestly."""

from pathlib import Path
from uuid import uuid4

from forge.orchestration.inspection import list_runs, read_logs, read_report


def test_missing_reports_and_incomplete_events(tmp_path: Path) -> None:
    identity = str(uuid4())
    root = tmp_path / identity
    root.mkdir()
    (root / "events.jsonl").write_text('{"status":"started"}\n{"broken":')
    assert read_report(tmp_path, identity)["status"] == "report_missing_or_interrupted"
    events = read_logs(tmp_path, identity)
    assert events["events"] == [{"status": "started"}, {"status": "incomplete_event_line"}]
    assert len(list_runs(tmp_path)) == 1
