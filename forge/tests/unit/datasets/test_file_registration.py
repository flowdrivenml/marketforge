"""Non-processing contract checks for physical file identity and evidence."""

import json
from pathlib import Path

import pytest

from forge.datasets.registration import file_summaries, merged_file_memberships
from forge.historical.engine.managed import checksum
from forge.historical.normalization.datasets import primary_reference, verify_inventory
from forge.shared.errors import MarketForgeError


def test_primary_ranges_exclude_siblings_and_auxiliary_files() -> None:
    root = "streams/trade/trades"
    manifest = {
        "files": [
            {
                "path": f"{root}/part-0.parquet",
                "rows": 2,
                "size_bytes": 7,
                "start_timestamp_ns": 4,
                "end_timestamp_ns": 5,
            },
            {
                "path": f"{root}/part-1.parquet",
                "rows": 3,
                "size_bytes": 9,
                "start_timestamp_ns": 7,
                "end_timestamp_ns": 8,
            },
        ],
        "auxiliary_files": [{"path": f"{root}/source.parquet"}],
        "streams": [
            {
                "directory": root,
                "stream_id": "s",
                "sources": [
                    {"task_id": 1, "start_row": 0, "end_row": 2},
                    {"task_id": 2, "start_row": 2, "end_row": 5},
                ],
            }
        ],
        "integrity_status": "clean",
    }
    jobs = {
        "tasks": [
            {
                "task_id": i,
                "symbol": "XYZ",
                "input_path": f"/{i}",
                "exchange": "fixture",
                "instrument_id": 4,
                "stream_id": "s",
            }
            for i in (1, 2)
        ]
    }
    result = file_summaries({"manifest": manifest, "operation": "normalize", "resolved_job": jobs})
    assert [file["source_paths"] for file in result] == [["/1"], ["/2"]]
    assert [file["rows"] for file in result] == [2, 3]
    assert [file["memberships"][0]["start_timestamp_ns"] for file in result] == [4, 7]
    assert [file["size_bytes"] for file in result] == [7, 9]


def test_checksum_inventory_requires_every_file_and_recognizes_provenance_pair(
    tmp_path: Path,
) -> None:
    names = ("processing-job.json", "processing-runtime.json")
    extras = ("processing-job.sha256", "processing-runtime.sha256")
    for name in (*names, *extras, "primary.parquet", "aux.parquet", "manifest.json"):
        (tmp_path / name).write_bytes(b"fixture")
    entries = [{"path": name, "size_bytes": 7} for name in ("primary.parquet", "aux.parquet")]
    meta = ("manifest.json", *names)
    expected = {
        name: checksum(tmp_path / name)
        for name in (*meta, *extras, "primary.parquet", "aux.parquet")
    }
    sidecar = tmp_path / "publication-checksums.json"
    sidecar.write_text(json.dumps(expected))
    assert verify_inventory(tmp_path, entries, meta, require_commit_checksums=True) == expected
    assert primary_reference(tmp_path / "primary.parquet") == (tmp_path, "primary.parquet")
    expected.pop("aux.parquet")
    sidecar.write_text(json.dumps(expected))
    with pytest.raises(MarketForgeError, match="checksums"):
        verify_inventory(tmp_path, entries, meta, require_commit_checksums=True)


def test_merged_membership_reads_only_file_local_streams(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pq.write_table(
        pa.table(
            {"dataset_id": [20, 21, 21], "stream_rank": [0, 1, 1], "event_timestamp_ns": [2, 3, 4]}
        ),
        tmp_path / "part.parquet",
    )
    pq.write_table(
        pa.table({"dataset_id": [21], "stream_rank": [1], "event_timestamp_ns": [8]}),
        tmp_path / "next.parquet",
    )
    streams = [
        {
            "stream_rank": rank,
            "dataset_id": rank + 20,
            "stream_id": str(rank),
            "exchange": "fixture",
            "instrument_id": rank,
            "symbol": "XYZ",
        }
        for rank in range(2)
    ]
    manifest = {
        "files": [{"path": name} for name in ("part.parquet", "next.parquet")],
        "streams": streams,
    }
    result = merged_file_memberships(tmp_path, manifest, [])
    assert [
        (m["stream_id"], m["start_timestamp_ns"], m["end_timestamp_ns"])
        for m in result["part.parquet"]
    ] == [("0", 2, 2), ("1", 3, 4)]
    assert [m["stream_id"] for m in result["next.parquet"]] == ["1"]
