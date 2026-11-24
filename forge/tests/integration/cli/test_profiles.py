"""Exercise profile commands without mutating the installed catalogue."""

import json
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Iterator
from io import StringIO
from pathlib import Path
from typing import Any, cast

import pytest
from rich.console import Console

from forge.cli.commands import profiles
from forge.cli.main import main
from forge.historical.profiles.schema import ProfileSchema, load_schema
from forge.historical.profiles.service import catalogue, create_draft, discover, inspect
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.shared.terminal import editor
from forge.shared.terminal.profiles import render_inventory
from forge.shared.terminal.theme import build_theme

ROOT = Path(__file__).resolve().parents[4]
BYBIT = "bybit.spot.spot.trades"
PROFILE_IDS = ("okx.spot.spot.trades", BYBIT, "bybit.perpetual.linear.depth")


@pytest.fixture
def workspace(tmp_path: Path) -> ProjectPaths:
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    original = catalogue(ProjectPaths.resolve(root=ROOT))
    for item in PROFILE_IDS:
        relative = Path(item.rsplit(".", 1)[1]) / f"{item}.toml"
        dest = catalogue(paths) / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes((original / relative).read_bytes())
    return paths


def command(
    paths: ProjectPaths, *args: str, as_json: bool = False, group: str = "normalize"
) -> int:
    flags = ["--json"] if as_json else []
    return main(["--project-root", str(paths.root), *flags, group, "profiles", *args])


def forbidden(*_: Any, **__: Any) -> Any:
    pytest.fail("Profile command called an unrelated dependency.")


@pytest.mark.parametrize(
    ("filters", "selected"),
    [
        ((), 3),
        (("--exchange", "bybit"), 2),
        (("--market-category", "linear"), 1),
        (("--instrument-type", "perpetual"), 1),
        (("--type", "depth"), 1),
        (("--type", "trades", "--exchange", "bybit"), 1),
        (("--type", "depth", "--exchange", "okx"), 0),
    ],
)
def test_inventory_filters_and_compatibility(
    workspace: ProjectPaths,
    filters: tuple[str, ...],
    selected: int,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("forge.historical.profiles.service.validate_catalogue", forbidden)
    monkeypatch.setattr("forge.shared.database.connection.database_dsn", forbidden)
    snapshot = {path: path.read_bytes() for path in catalogue(workspace).rglob("*.toml")}
    assert command(workspace, "list", *filters) == 0
    output = capsys.readouterr().out
    if filters:
        assert f"{selected} of 3 installed profiles" in output
    else:
        assert "3 profiles installed  ·  2 Trades  ·  1 Depth  ·  2 Exchanges" in output
        assert output.index(PROFILE_IDS[2]) < output.index(BYBIT) < output.index(PROFILE_IDS[0])
    assert (
        "definition" not in output and "sha256" not in output and str(workspace.root) not in output
    )
    assert command(workspace, "list", *filters, as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)["profiles"]
    assert len(payload) == selected
    for item in payload:
        assert item == inspect(Path(item["location"]))
    assert {path: path.read_bytes() for path in catalogue(workspace).rglob("*.toml")} == snapshot
    assert not workspace.runs_dir.exists()


def test_empty_and_invalid_inventory(
    workspace: ProjectPaths, capsys: pytest.CaptureFixture[str]
) -> None:
    for source in catalogue(workspace).rglob("*.toml"):
        source.unlink()
    assert command(workspace, "list") == 0
    assert "0 profiles installed" in capsys.readouterr().out
    profile = catalogue(workspace) / "trades" / f"{BYBIT}.toml"
    profile.write_text("[not valid")
    assert command(workspace, "list") == 2
    assert "Invalid profile TOML" in capsys.readouterr().err


@pytest.mark.parametrize("width", [49, 80, 120, 160])
def test_widths_and_unknown_format(workspace: ProjectPaths, width: int) -> None:
    published = discover(workspace)
    published[0]["definition"]["source"]["format"] = "unknown"
    stream = StringIO()
    console = Console(file=stream, width=width, force_terminal=False, theme=build_theme())
    render_inventory(console, published, len(published), {})
    output = stream.getvalue()
    assert BYBIT in output
    assert max(map(len, output.splitlines())) <= width
    assert "\x1b" not in output
    assert "Format" in output if width >= 80 else "Format" not in output
    if width >= 80:
        assert "—" in output
    assert "\n\nOKX" in "\n".join(line.rstrip() for line in output.splitlines())


def test_show_is_structural_and_reuses_json(
    workspace: ProjectPaths, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(profiles, "edit_file", forbidden)
    monkeypatch.setattr(profiles, "validate", forbidden)
    assert command(workspace, "show", BYBIT) == 0
    output = capsys.readouterr().out
    for text in (
        BYBIT,
        "source.container",
        "fields.timestamp.encoding",
        "normalization.",
        "Omitted settings with schema defaults",
        "SHA-256",
        "Rust semantic validation has not run",
    ):
        assert text in output
    assert "source.archive_day_boundary" not in output
    assert command(workspace, "show", BYBIT, as_json=True) == 0
    show = json.loads(capsys.readouterr().out)
    assert command(workspace, "inspect", BYBIT, as_json=True) == 0
    assert show == json.loads(capsys.readouterr().out)
    assert set(show) == {"id", "location", "sha256", "validation", "definition"}


def test_readonly_editor(
    workspace: ProjectPaths, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target = catalogue(workspace) / "trades" / f"{BYBIT}.toml"
    before = inspect(target)
    monkeypatch.setattr(profiles, "interactive", lambda: False)
    assert command(workspace, "inspect", BYBIT) == 2
    assert "requires an interactive terminal" in capsys.readouterr().err
    monkeypatch.setattr(profiles, "interactive", lambda: True)

    def launch(args: list[str], *, check: bool) -> subprocess.CompletedProcess[str]:
        assert args == ["nvim", "-u", "NONE", "-i", "NONE", "-n", "-R", "-M", "--", str(target)]
        return subprocess.CompletedProcess(args, 7 if check else 0)

    monkeypatch.setattr(editor.subprocess, "run", launch)
    assert command(workspace, "inspect", BYBIT) == 0
    assert inspect(target) == before
    assert command(workspace, "inspect", "gateio.spot.spot.trades") == 2
    target.unlink()
    target.symlink_to(workspace.root)
    assert command(workspace, "inspect", BYBIT) == 2
    assert "Unsafe profile path" in capsys.readouterr().err


def test_editor_failure_and_draft_preservation(
    workspace: ProjectPaths, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target = catalogue(workspace) / "trades" / f"{BYBIT}.toml"
    content = b"# retain this comment\n" + target.read_bytes()
    target.write_bytes(content)
    monkeypatch.setattr(profiles, "interactive", lambda: True)

    def fail(args: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 7)

    monkeypatch.setattr(editor.subprocess, "run", fail)
    assert command(workspace, "inspect", BYBIT) == 2
    assert "code 7" in capsys.readouterr().err

    def edit(path: Path, **_: Any) -> Path:
        assert path.read_bytes() == content
        assert catalogue(workspace) not in path.parents
        path.write_text("[invalid")
        return path

    monkeypatch.setattr(profiles, "edit_file", edit)
    assert command(workspace, "edit", BYBIT) == 2
    draft = workspace.engine_config_dir / "normalize/profile-drafts" / target.name
    assert draft.read_text() == "[invalid"
    assert target.read_bytes() == content
    with pytest.raises(MarketForgeError, match="already exists"):
        create_draft(workspace, BYBIT)
    assert command(workspace, "edit", BYBIT, as_json=True) == 2
    draft.unlink()
    draft.symlink_to(target)
    with pytest.raises(MarketForgeError, match="Unsafe"):
        create_draft(workspace, BYBIT)
    assert target.read_bytes() == content


def test_cli_schema_selectors(workspace: ProjectPaths, capsys: pytest.CaptureFixture[str]) -> None:
    assert command(workspace, "schema", as_json=True) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["schema_version"] == 2 and len(report["fields"]) > 100
    assert command(workspace, "schema", "--field", "fields.timestamp.encoding", as_json=True) == 0
    report = json.loads(capsys.readouterr().out)
    assert [field["path"] for field in report["fields"]] == ["fields.<name>.encoding"]
    assert command(workspace, "schema", "--section", "normalization", as_json=True) == 0
    report = json.loads(capsys.readouterr().out)
    assert {field["section"] for field in report["fields"]} == {"normalization"}
    assert command(workspace, "schema", "--field", "source.formt") == 2
    assert "source.format" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        command(workspace, "schema", "--field", "source.format", "--section", "source")


def sample_paths(data: dict[str, object], schema: ProfileSchema, prefix: str = "") -> Iterator[str]:
    for key, value in data.items():
        token = json.dumps(key) if "." in key else key
        path = f"{prefix}.{token}" if prefix else token
        yield path
        if isinstance(value, dict) and schema.field(path).children:
            yield from sample_paths(cast(dict[str, object], value), schema, path)
        elif isinstance(value, list):
            for item in cast(list[object], value):
                if isinstance(item, dict):
                    yield from sample_paths(cast(dict[str, object], item), schema, f"{path}[]")


def test_metadata_and_installed_definitions_are_covered() -> None:
    schema = load_schema()
    assert schema.field("source.header").variants[0].required == "conditional"
    assert schema.field("feeds").variants[0].default == {}
    assert schema.field("normalization.bids.price_index").variants[0].default == 0
    assert schema.field("normalization.event_filter.in").variants[0].type == "array of string"
    assert schema.field("normalization.rules[0].target") == schema.field(
        "normalization.rules[].target"
    )
    assert schema.field("feeds.snapshot.source.format").section == "feeds"
    assert not schema.parameters(profile_type="trades", source_format="xlsx")
    for variant in schema.field("normalization.target").variants:
        values = {str(choice.value) for choice in variant.allowed_values}
        assert values == (
            {"trade"} if variant.profile_types == ("trades",) else {"l2_snapshot", "l2_update"}
        )
    for field in schema.fields:
        for variant in field.variants:
            for example in variant.examples:
                tomllib.loads(example)
    for profile in discover(ProjectPaths.resolve(root=ROOT)):
        for path in sample_paths(profile["definition"], schema):
            schema.field(path)


def test_reference_is_current() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "engine/scripts/generate_profile_schema.py"), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("alias", ["add", "register"])
def test_publication_aliases_still_use_conflict_safe_service(
    alias: str,
    workspace: ProjectPaths,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def register(paths: ProjectPaths, profile_id: str, source: Path) -> dict[str, str]:
        assert paths == workspace
        assert profile_id == BYBIT and source.name == "definition"
        return {"publication": "reused"}

    monkeypatch.setattr(profiles, "register", register)
    assert command(workspace, alias, BYBIT, "--from", "definition", as_json=True) == 0
    assert json.loads(capsys.readouterr().out) == {"publication": "reused"}


@pytest.mark.parametrize(
    ("path", "existing", "replacement", "message"),
    [
        (
            "src/historical/config/catalog/unified.rs",
            "    header: Option<bool>,",
            "    header: Option<bool>,\n    delimiter: Option<String>,",
            "Rust field inventory changed",
        ),
        (
            "src/historical/config/catalog/unified.rs",
            "    header: Option<bool>,\n",
            "",
            "Rust field inventory changed",
        ),
        (
            "src/shared/io/container.rs",
            "    Plain,",
            "    Plain,\n    NewContainer,",
            "must describe every allowed value",
        ),
        (
            "src/historical/config/catalog/unified.rs",
            "    #[serde(default)]\n    feeds:",
            "    feeds:",
            "Stale profile reference",
        ),
    ],
)
def test_generator_detects_rust_drift(
    tmp_path: Path, path: str, existing: str, replacement: str, message: str
) -> None:
    sources = [f"engine/{name}" for name in load_schema().rust_sources]
    sources.extend(
        (
            "engine/specs/normalization-profile.toml",
            "engine/scripts/generate_profile_schema.py",
            "forge/historical/profiles/schema.json",
            "docs/content/reference/configuration/profile-fields.md",
        )
    )
    for source in sources:
        target = tmp_path / source
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / source, target)
    changed = tmp_path / "engine" / path
    content = changed.read_text()
    assert existing in content
    changed.write_text(content.replace(existing, replacement, 1))
    result = subprocess.run(
        [sys.executable, str(tmp_path / "engine/scripts/generate_profile_schema.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0 and message in result.stderr


def test_acquisition_profile_commands_removed(
    workspace: ProjectPaths, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit, match="0"):
        main(["acquisition", "--help"])
    assert "profiles" not in capsys.readouterr().out
    with pytest.raises(SystemExit, match="2"):
        command(workspace, "list", group="acquisition")
    assert "invalid choice: 'profiles'" in capsys.readouterr().err
    assert command(workspace, "list", as_json=True) == 0
    assert "profiles" in json.loads(capsys.readouterr().out)


@pytest.mark.parametrize("kind", ["trades", "depth"])
def test_authoring(workspace: ProjectPaths, capsys: pytest.CaptureFixture[str], kind: str) -> None:
    profile_id = f"example_exchange.equity.equity.{kind}"
    assert command(workspace, "new", profile_id, as_json=True) == 0
    report = json.loads(capsys.readouterr().out)
    draft = Path(report["draft"])
    assert draft.parent == workspace.engine_config_dir / "normalize/profile-drafts"
    original = draft.read_bytes()
    assert b"REQUIRED" in original
    definition = tomllib.loads(original.decode())
    assert definition["id"] == profile_id and "format_code" not in definition
    template = ROOT / f"docs/content/examples/catalogue/{profile_id}.toml"
    example = tomllib.loads(template.read_text())
    assert definition == {key: value for key, value in example.items() if key != "format_code"}
    assert command(workspace, "new", profile_id) == 2
    assert draft.read_bytes() == original

    assert command(workspace, "guide", kind, as_json=True) == 0
    guide = json.loads(capsys.readouterr().out)
    import shlex

    from forge.cli.main import build_parser

    for step in guide["steps"]:
        if step["command"]:
            build_parser().parse_args(shlex.split(step["command"])[1:])


@pytest.mark.parametrize("width", [80, 120, 160])
@pytest.mark.parametrize("kind", [None, "trades", "depth"])
def test_compact_schema_rendering(width: int, kind: str | None) -> None:
    from forge.shared.terminal.profiles import render_schema

    stream = StringIO()
    console = Console(file=stream, width=width, theme=build_theme())
    render_schema(console, load_schema(), profile_type=kind)
    output = stream.getvalue()
    assert "\x1b" not in output
    assert max(map(len, output.splitlines())) <= width
    assert output.count("Explore further") == 1
    assert output.count("source.format") == int(kind is not None)
    assert "TOML has no null literal" not in output
    if kind:
        assert f"guide {kind}" in output


def test_contextual_schema(workspace: ProjectPaths, capsys: pytest.CaptureFixture[str]) -> None:
    schema = load_schema()
    assert schema.field("source.format", profile_type="trades").variants[0].type == "enum"
    assert schema.field("source.format").documentation.endswith("#sourceformat")
    shared = schema.field("normalization.quantity_encoding", profile_type="trades").variants[0]
    assert shared.profile_types == ("trades",)
    assert shared.source_formats == ("csv", "jsonl")
    for kind, expected in (("trades", {"csv", "jsonl"}), ("depth", {"csv", "jsonl", "xlsx"})):
        assert (
            command(workspace, "schema", "--type", kind, "--field", "source.format", as_json=True)
            == 0
        )
        fields = json.loads(capsys.readouterr().out)["fields"]
        assert len(fields) == 1
        variants = fields[0]["variants"]
        assert {choice["value"] for choice in variants[0]["allowed_values"]} == expected
        assert variants[0]["profile_types"] == [kind]
    assert command(workspace, "schema", "--type", "trades", "--field", "normalization.asks") == 2
    assert "Unknown profile schema field" in capsys.readouterr().err
    assert command(workspace, "schema", "--section", "feeds") == 0
    assert "Each feed supplies" in capsys.readouterr().out
    assert command(workspace, "schema", "--section", "invalid") == 2
    assert "Unknown profile schema section" in capsys.readouterr().err


@pytest.mark.parametrize(
    "args",
    [
        ("list",),
        ("list", "--exchange", "absent"),
        ("show", BYBIT),
        ("schema",),
        ("schema", "--section", "source"),
        ("schema", "--type", "trades", "--field", "source.format"),
        ("guide", "trades"),
        ("guide", "depth"),
        ("new", "example_exchange.equity.equity.depth"),
    ],
)
def test_canonical_documentation_footer(
    workspace: ProjectPaths, capsys: pytest.CaptureFixture[str], args: tuple[str, ...]
) -> None:
    from forge.historical.profiles.documentation import PROFILE_REFERENCE

    assert command(workspace, *args) == 0
    output = capsys.readouterr().out
    assert output.count(PROFILE_REFERENCE) == 1
    assert output.rstrip().endswith(PROFILE_REFERENCE)
    assert "\x1b" not in output


def test_profile_documentation_hyperlink() -> None:
    from forge.historical.profiles.documentation import PROFILE_REFERENCE
    from forge.shared.terminal.profiles import render_documentation

    stream = StringIO()
    render_documentation(Console(file=stream, force_terminal=True))
    output = stream.getvalue()
    assert "\x1b]8;" in output
    assert f";{PROFILE_REFERENCE}\x1b\\" in output


def test_validation_error_documentation(
    workspace: ProjectPaths, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from forge.historical.profiles.documentation import PROFILE_REFERENCE

    def invalid(*_args: object) -> None:
        raise MarketForgeError("Missing format_code")

    monkeypatch.setattr(profiles, "validate", invalid)
    assert command(workspace, "validate", BYBIT) == 2
    assert capsys.readouterr().err.count(PROFILE_REFERENCE) == 1
    assert command(workspace, "validate", BYBIT, as_json=True) == 2
    assert json.loads(capsys.readouterr().err)["error"]["message"] == "Missing format_code"
