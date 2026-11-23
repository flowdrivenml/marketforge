"""Exercise bounded source previews without registration or database access."""

import gzip
import io
import json
import tarfile
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest
from rich.console import Console

from forge.cli.main import main
from forge.historical.imports import formats
from forge.historical.imports.formats import SAMPLE_BYTES, sample
from forge.historical.profiles.documentation import PROFILE_REFERENCE
from forge.historical.profiles.source_sample import MAX_DEPTH, MAX_FIELDS, inspect_source
from forge.shared.terminal.source_sample import render_source_sample
from forge.shared.terminal.theme import build_theme

ROOT = Path(__file__).resolve().parents[4]
TRADES = ROOT / "docs/content/examples/catalogue/synthetic-trades.csv"


def test_csv_positions_quotes_and_irregular_width(tmp_path: Path) -> None:
    path = tmp_path / "records.csv"
    path.write_text(
        'Time,Price,Notes\n1730125800000000000,233.62,"two, tokens"\n42\n1,2,three,extra\n'
    )
    result = inspect_source(path, header=True)
    assert [f.name for f in result.fields] == ["Time", "Price", "Notes"]
    assert [f.position for f in result.fields] == [1, 2, 3]
    assert result.preview[0] == ("1730125800000000000", "233.62", "two, tokens")
    assert result.fields[1].missing == 1
    assert "irregular widths" in " ".join(result.warnings)


def test_duplicate_empty_headers_are_distinguished_by_position(tmp_path: Path) -> None:
    path = tmp_path / "records.csv"
    path.write_text("Same,Same,\n1,2,\n")
    result = inspect_source(path, header=True)
    assert [field.name for field in result.fields] == ["Same", "Same", ""]
    assert result.fields[-1].representation == "Empty"
    assert "duplicate or empty" in " ".join(result.warnings)


def test_json_shapes_and_exact_values(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    exact = "233.12345000000000000000000009"
    path.write_text(
        '{"data":{"ts":1730125800000000000,"price":'
        + exact
        + ',"size":"10","token":"2.50","ok":true,"book":[[1,"2"],[3,4]]}}\n'
        + '{"data":{"price":null,"ok":"false"}}\n'
    )
    result = inspect_source(path)
    fields = {field.name: field for field in result.fields}
    assert fields["data.ts"].examples == ("1730125800000000000",)
    assert fields["data.ts"].missing == 1
    assert fields["data.price"].examples == (exact, "null")
    assert fields["data.price"].representation == "Mixed"
    assert fields["data.price"].nulls == 1
    assert fields["data.token"].representation == "Decimal-like string"
    assert fields["data.size"].representation == "Numeric string"
    assert fields["data.ok"].representation == "Mixed"
    assert fields["data.book"].representation == "Array"
    serialized = json.loads(result.evidence_json(), parse_float=Decimal)
    assert serialized["representative_rows"][0]["data"]["price"] == Decimal(exact)


def test_json_path_conflicts_and_bounds(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    text = '{"a":{"b":1},"a.b":2}\n{"a":3}\n'
    text += '{"deep":' + '{"child":' * 50 + "0" + "}" * 51 + "\n"
    text += json.dumps({f"field_{i}": i for i in range(100)})
    path.write_text(text)
    result = inspect_source(path)
    assert len(result.fields) == MAX_FIELDS
    warnings = " ".join(result.warnings)
    assert "Literal dotted" in warnings
    assert "alternate between" in warnings
    assert "depth limit" in warnings
    assert all(field.name.count(".") < MAX_DEPTH for field in result.fields)


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("", "Empty"),
        ('"unterminated,12\n', "Malformed"),
        ('{"a":1,"a":2}\n', "duplicate key"),
        ('{"a":', "Malformed"),
        ("[1,2]\n", "Malformed"),
    ],
)
def test_unreadable_samples_report_reason(tmp_path: Path, text: str, reason: str) -> None:
    path = tmp_path / ("data.csv" if text.startswith('"') else "data.jsonl")
    path.write_text(text)
    result = inspect_source(path)
    assert result.reason is not None and reason in result.reason
    assert not result.preview


def test_bounded_prefix_excludes_incomplete_record(tmp_path: Path) -> None:
    path = tmp_path / "rows.csv"
    path.write_text("1,2\n" + "3" * (SAMPLE_BYTES * 2) + ",4\n")
    result = inspect_source(path, header=False, rows=20)
    assert result.preview == (("1", "2"),)
    assert "65,536-byte prefix" in " ".join(result.warnings)


def test_archive_members_and_wrappers(tmp_path: Path) -> None:
    content = b"1,2\n3,4\n"
    path = tmp_path / "one.zip"
    with zipfile.ZipFile(path, "w") as zipped:
        zipped.writestr("nested/rows.csv", content)
        zipped.writestr("other.csv", content)
        zipped.writestr("../unsafe.csv", content)
    assert inspect_source(path).layout == "requires_member"
    selected = inspect_source(path, "nested/rows.csv")
    assert selected.container == "zip" and selected.member == "nested/rows.csv"
    assert selected.preview[1] == ("3", "4")
    assert inspect_source(path, "../rows.csv").reason is not None
    assert inspect_source(path, "../unsafe.csv").reason == "Unsafe ZIP member name."
    path = tmp_path / "rows.csv.gz"
    path.write_bytes(gzip.compress(content))
    assert inspect_source(path).container == "gzip"
    path = tmp_path / "rows.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        member = tarfile.TarInfo("rows.csv")
        member.size = len(content)
        tar.addfile(member, io.BytesIO(content))
    assert inspect_source(path).container == "tar_gzip"
    assert inspect_source(path).layout == "unsupported"


def test_compressed_input_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "rows.csv.gz"
    path.write_bytes(gzip.compress(b"1,2\n"))
    monkeypatch.setattr(formats, "COMPRESSED_SAMPLE_BYTES", 8)
    result = inspect_source(path)
    assert result.layout == "unsupported"
    assert result.reason == "Compressed input exceeds the bounded sample read limit."


def test_default_and_explicit_row_count_and_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "rows.csv"
    original = b"Time,Price\n" + b"10,2.5\n" * 25
    path.write_bytes(original)
    args = ["normalize", "profiles", "sample", "--file", str(path), "--header"]
    assert main(args) == 0
    assert "5 rows" in capsys.readouterr().out
    assert main([*args, "--rows", "10", "--fields"]) == 0
    output = capsys.readouterr().out
    assert "10 rows" in output and "Data preview" not in output
    assert output.count(PROFILE_REFERENCE) == 1
    assert main(["--json", *args]) == 0
    assert json.loads(capsys.readouterr().out) == sample(path, header=True)
    assert main(["--json", *args, "--rows", "20"]) == 0
    observed = json.loads(capsys.readouterr().out)
    assert len(observed["representative_rows"]) == 21
    assert observed["representative_rows"][0] == ["Time", "Price"]
    with pytest.raises(SystemExit):
        main([*args, "--rows", "21"])
    assert path.read_bytes() == original
    assert inspect_source(TRADES).preview[0][0] == "1730125800000000000"


@pytest.mark.parametrize("width", [80, 120, 160])
def test_render_source_at_terminal_width(width: int) -> None:
    stream = io.StringIO()
    console = Console(file=stream, width=width, force_terminal=False, theme=build_theme())
    render_source_sample(console, inspect_source(TRADES, header=False), fields_only=False)
    result = stream.getvalue()
    assert all(len(line) <= width for line in result.splitlines())
    assert "1730125800000000000" in result
    assert "column_1" in result and "Data preview" in result
    assert "\x1b" not in result
