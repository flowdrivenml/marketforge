"""Regression checks for parser-derived help and stream behavior."""

from __future__ import annotations

import argparse
import io
import re
import shlex
from collections.abc import Iterator
from pathlib import Path

import pytest
from rich.cells import cell_len

from forge.cli.help import HelpParser, children
from forge.cli.help_content import COMMAND_GROUPS, EXAMPLES, GROUPS, NOTES, RELATED, SUMMARIES
from forge.cli.main import build_parser, main


def hierarchy(
    parser: argparse.ArgumentParser, path: str = ""
) -> Iterator[tuple[str, argparse.ArgumentParser]]:
    yield path, parser
    for command in children(parser):
        yield from hierarchy(command.parser, f"{path} {command.name}".strip())


def test_root_groups_cover_each_registered_command_once() -> None:
    parser = build_parser()
    root = parser.format_help()
    current = ""
    found: dict[str, list[str]] = {}
    for line in root.splitlines():
        if line and not line.startswith(" "):
            current = line
        elif re.match(r"^  [a-z]", line) and current in dict(GROUPS):
            found.setdefault(current, []).append(line.split()[0])
    assert found == {title: list(names) for title, names in GROUPS}
    listed = [name for names in found.values() for name in names]
    assert set(listed) == {command.name for command in children(parser)}
    assert len(listed) == len(set(listed))
    for option in ("-h, --help", "--version", "--project-root", "--config", "--json"):
        assert option in root
    assert "Production is the default" in root
    assert "mf database status --testing" in root
    assert "{tui," not in root


def test_new_commands_are_visible_even_without_group_metadata() -> None:
    parser = HelpParser()
    commands = parser.add_subparsers()
    commands.add_parser("future", help="Inspect future resources.")
    commands.add_parser("private", help=argparse.SUPPRESS)
    output = parser.format_help()
    assert "OTHER COMMANDS" in output
    assert "future" in output
    assert "private" not in output


def test_every_parser_has_help_and_presentation_topics_exist() -> None:
    root = build_parser()
    inventory = dict(hierarchy(root))
    assert (SUMMARIES.keys() | EXAMPLES.keys() | NOTES.keys() | RELATED.keys()) <= inventory.keys()
    for path, parser in inventory.items():
        output = parser.format_help()
        assert output.startswith("MarketForge")
        assert "Usage" in output
        assert "-h, --help" in output
        if commands := list(children(parser)):
            headings = [title for title, _ in COMMAND_GROUPS.get(path, ())]
            assert all(
                title in output
                for title in (headings or ["COMMANDS" if path else "GETTING STARTED"])
            )
            for command in commands:
                assert re.search(rf"^  {re.escape(command.name)}(?:\s|$)", output, re.MULTILINE)
        else:
            assert output.splitlines()[1], f"{path} needs a summary"


def test_simple_leaf_help_snapshot() -> None:
    leaf = dict(hierarchy(build_parser()))["engine status"].format_help()
    assert [" ".join(line.split()) for line in leaf.splitlines() if line] == [
        "MarketForge · Engine · Status",
        "Show managed binaries and their installation status.",
        "Usage",
        "mf engine status [-h]",
        "OPTIONS",
        "-h, --help Show help for this command.",
    ]


def test_all_examples_and_related_topics_use_the_actual_parser(
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = build_parser()
    for examples in EXAMPLES.values():
        for example in examples:
            argv = shlex.split(example)[1:]
            if argv[-1] == "--help":
                with pytest.raises(SystemExit) as result:
                    root.parse_args(argv)
                assert result.value.code == 0
            else:
                root.parse_args(argv)
    capsys.readouterr()
    inventory = dict(hierarchy(root))
    for path, related in RELATED.items():
        for topic in related:
            reference = f"{path} {topic}" if list(children(inventory[path])) else topic
            assert reference in inventory


def test_argument_details_reflect_parser_metadata() -> None:
    inventory = dict(hierarchy(build_parser()))
    restore = inventory["database restore"].format_help()
    assert "--destination DESTINATION" in restore
    assert "Required." in restore
    assert "BACKUP_PATH" in restore
    migration = inventory["database migrate"].format_help()
    assert "(--testing | --production)" in migration
    assert "Select the production database (default)." in migration
    datasets = inventory["datasets list"].format_help()
    assert "--operation {normalize,merge,convert}" in datasets
    assert "--view {timelines,publications,files}" in datasets
    assert "legacy file page" in datasets
    assert "Repeat to select several" in inventory["datasets head"].format_help()
    assert "Default 10." in inventory["datasets head"].format_help()
    assert "--header, --no-header" in inventory["normalize profiles sample"].format_help()
    assert "--limit" not in inventory["markets sync"].format_help()
    assert "--sort" not in inventory["markets sync"].format_help()
    assert "repeatable exchange filter" in inventory["markets list"].format_help()
    parser = HelpParser()
    parser.add_argument("--item", action="append")
    assert "Repeatable." in parser.format_help()


def test_help_is_read_only_and_short_help_is_equivalent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MARKETFORGE_PROJECT_ROOT", raising=False)
    monkeypatch.delenv("TEST_DATABASE_KEY", raising=False)
    monkeypatch.delenv("PROD_DATABASE_KEY", raising=False)
    for path in (
        [],
        ["engine"],
        ["engine", "build"],
        ["acquisition", "import"],
        ["markets", "rank"],
    ):
        rendered: list[str] = []
        for flag in ("-h", "--help"):
            with pytest.raises(SystemExit) as result:
                main([*path, flag])
            assert result.value.code == 0
            output = capsys.readouterr()
            assert not output.err
            rendered.append(output.out)
        assert rendered[0] == rendered[1]
    assert main([]) == 0
    assert "GETTING STARTED" in capsys.readouterr().out
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "argv",
    [
        ["engine", "build", "--jobs", "0"],
        ["engine", "--unknown"],
        ["pipeline", "validate"],
        ["database", "status", "--testing", "--production"],
        ["datasets", "list", "--json"],
    ],
)
def test_parser_failures_still_use_stderr_and_exit_two(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as result:
        main(argv)
    assert result.value.code == 2
    output = capsys.readouterr()
    assert not output.out
    assert "usage:" in output.err
    assert "error:" in output.err
    assert "Traceback" not in output.err


@pytest.mark.parametrize("width", [50, 80, 120, 160])
def test_rendering_fits_terminal_width(width: int, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COLUMNS", str(width))
    for path, parser in hierarchy(build_parser()):
        assert isinstance(parser, HelpParser)
        stream = io.StringIO()
        parser.print_help(stream)
        output = stream.getvalue()
        assert all(cell_len(line) <= min(100, width) for line in output.splitlines()), path
        assert "\x1b" not in output
        assert "MarketForge" in output


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


@pytest.mark.parametrize(
    ("term", "color", "tty", "styled"),
    [
        ("xterm-256color", "auto", True, True),
        ("xterm-ghostty", "auto", True, True),
        ("dumb", "auto", True, False),
        ("xterm-256color", "auto", False, False),
        ("xterm-256color", "never", True, False),
        ("xterm-256color", "always", False, True),
    ],
)
def test_color_follows_terminal_and_explicit_configuration(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    term: str,
    color: str,
    tty: bool,
    styled: bool,
) -> None:
    monkeypatch.setenv("TERM", term)
    monkeypatch.setenv("COLORTERM", "truecolor" if term == "xterm-ghostty" else "")
    monkeypatch.delenv("NO_COLOR", raising=False)
    configuration = workspace / "help.toml"
    configuration.write_text(f'[presentation]\ncolor = "{color}"\n')
    stream = Terminal() if tty else io.StringIO()
    monkeypatch.setattr("sys.stdout", stream)
    with pytest.raises(SystemExit) as result:
        main(["--project-root", str(workspace), "--config", str(configuration), "engine", "-h"])
    assert result.value.code == 0
    output = stream.getvalue()
    assert ("\x1b[" in output) == styled
    plain = re.sub(r"\x1b\[[\d;]*m", "", output)
    assert "MarketForge · Engine" in plain
    assert "\x1b" not in plain
