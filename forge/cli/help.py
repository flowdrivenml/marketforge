"""Render the registered argparse hierarchy using the shared Forge console."""

from __future__ import annotations

# Argparse exposes formatter methods but keeps its action subclasses private.
# pyright: reportPrivateUsage=false
import argparse
import io
from collections.abc import Iterable
from dataclasses import dataclass
from difflib import get_close_matches
from typing import NoReturn, Protocol, TextIO, TypeVar, cast, overload

from rich.cells import cell_len
from rich.console import Console
from rich.text import Text

from forge.cli.help_content import (
    COMMAND_GROUPS,
    DOCUMENTATION,
    EXAMPLES,
    GROUPS,
    NOTES,
    RELATED,
    SUMMARIES,
)
from forge.shared.config.models import PresentationSettings
from forge.shared.errors import MarketForgeError
from forge.shared.terminal.console import create_console
from forge.shared.terminal.presentation import documentation_link
from forge.startup.context import build_context


class Writable(Protocol):
    def write(self, text: str, /) -> object: ...


class UsageFormatter(argparse.HelpFormatter):
    def _get_default_metavar_for_positional(self, action: argparse.Action) -> str:
        return action.dest.upper()


@dataclass(frozen=True)
class Command:
    name: str
    parser: argparse.ArgumentParser
    summary: str | None


def children(parser: argparse.ArgumentParser) -> Iterable[Command]:
    """Include parsers registered without a help string, in registration order."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            labels = {item.dest: item.help for item in action._choices_actions}
            parsers = cast(dict[str, argparse.ArgumentParser], action.choices)  # pyright: ignore[reportUnknownMemberType]
            for name, child in parsers.items():
                yield Command(name, child, labels.get(name))


_Namespace = TypeVar("_Namespace")


class HelpParser(argparse.ArgumentParser):
    """Keep parsing and errors in argparse and replace only help presentation."""

    help_path: tuple[str, ...] = ()
    help_summary: str | None = None
    _root: HelpParser | None = None
    _help_namespace: object | None = None

    def error(self, message: str) -> NoReturn:
        missing = message.removeprefix("the following arguments are required: ")
        if (
            self.help_path == ("convert", "new")
            and missing != message
            and "NAME" in missing.split(", ")
        ):
            message = (
                "The new conversion recipe needs a name. Example: "
                "mf convert new getting_started_usdt --from-merge getting_started "
                "--target-quote USDT --testing"
            )
        super().error(message)

    def bind_help(
        self,
        root: HelpParser | None = None,
        path: tuple[str, ...] = (),
        summary: str | None = None,
    ) -> None:
        """Associate each parser with its canonical displayed command path."""
        self._root = root
        self.help_path = path
        self.help_summary = summary
        seen: set[argparse.ArgumentParser] = set()
        for child in children(self):
            if isinstance(child.parser, HelpParser) and child.parser not in seen:
                child.parser.bind_help(root or self, (*path, child.name), child.summary)
                seen.add(child.parser)

    @overload
    def parse_known_args(
        self, args: Iterable[str] | None = None, namespace: None = None
    ) -> tuple[argparse.Namespace, list[str]]: ...

    @overload
    def parse_known_args(
        self, args: Iterable[str] | None, namespace: _Namespace
    ) -> tuple[_Namespace, list[str]]: ...

    @overload
    def parse_known_args(self, *, namespace: _Namespace) -> tuple[_Namespace, list[str]]: ...

    def parse_known_args(
        self, args: Iterable[str] | None = None, namespace: object | None = None
    ) -> tuple[object, list[str]]:
        selected = argparse.Namespace() if namespace is None else namespace
        if self._root is not None:
            if self.help_path == ("convert", "new") and args is not None:
                tokens = list(args)
                for token in tokens:
                    if token in {"--", "--help", "-h"}:
                        break
                    option = token.partition("=")[0]
                    if not option.startswith("--") or option in self._option_string_actions:
                        continue
                    matches = get_close_matches(
                        option, self._option_string_actions, n=1, cutoff=0.8
                    )
                    if matches:
                        self.error(f"unrecognized option {option}. Did you mean {matches[0]}?")
                args = tokens
            return super().parse_known_args(args, selected)
        previous = self._help_namespace
        self._help_namespace = selected
        try:
            return super().parse_known_args(args, selected)
        finally:
            self._help_namespace = previous

    def presentation(self) -> PresentationSettings:
        selected = (self._root or self)._help_namespace
        try:
            return build_context(
                project_root=getattr(selected, "project_root", None),
                config_file=getattr(selected, "config_file", None),
            ).settings.presentation
        except (MarketForgeError, OSError, ValueError, RuntimeError):
            # Help remains available to diagnose an invalid or missing workspace.
            return PresentationSettings()

    def print_root_help(self, namespace: argparse.Namespace) -> None:
        self._help_namespace = namespace
        try:
            self.print_help()
        finally:
            self._help_namespace = None

    def print_help(self, file: Writable | None = None) -> None:
        if file is not None and not hasattr(file, "isatty"):
            file.write(self.format_help())
            return
        render(self, create_console(self.presentation(), stream=cast(TextIO | None, file)))

    def format_help(self) -> str:
        output = io.StringIO()
        render(self, create_console(PresentationSettings(color="never"), stream=output))
        return output.getvalue()


def _heading(console: Console, title: str) -> None:
    console.print()
    text = Text(title, style="mf.identifier")
    text.stylize("bold")
    console.print(text)


def _rows(
    console: Console, rows: list[tuple[str, str]], *, style: str, label_width: int = 0
) -> None:
    limit = min(31, console.width // 2 - 3)
    width = max(
        label_width, max((cell_len(name) for name, _ in rows if cell_len(name) <= limit), default=0)
    )
    for name, description in rows:
        size = cell_len(name)
        row = Text("  ")
        row.append(name, style)
        if not description:
            console.print(row)
            continue
        if size > limit:
            console.print(row)
            row = Text()
            indent = 4
        else:
            indent = width + 4
        for index, line in enumerate(Text(description).wrap(console, console.width - indent)):
            line.rstrip()
            padding = indent - size - 2 if index == 0 and row else indent
            if index == 0:
                row.append(" " * padding)
                row.append_text(line)
                console.print(row)
            else:
                console.print(Text(" " * indent).append_text(line))


def _lines(console: Console, lines: Iterable[str], *, style: str = "mf.body") -> None:
    for line in lines:
        console.print(Text(line, style=style), overflow="fold")


def _summary(path: str, command: Command) -> str:
    return SUMMARIES.get(path, command.summary or command.parser.description or "")


def _commands(console: Console, path: str, commands: list[Command]) -> None:
    visible = {
        command.name: command for command in commands if command.summary != argparse.SUPPRESS
    }
    groups = COMMAND_GROUPS.get(path, () if path else GROUPS)
    assigned = {name for _, names in groups for name in names}
    groups = (
        *groups,
        (
            "OTHER COMMANDS" if groups else "COMMANDS",
            tuple(name for name in visible if name not in assigned),
        ),
    )
    for title, names in groups:
        rows = [
            (name, _summary(" ".join(filter(None, (path, name))), visible[name]))
            for name in names
            if name in visible
        ]
        if rows:
            _heading(console, title)
            _rows(console, rows, style="mf.success", label_width=11 if not path else 0)


def _usage(
    console: Console,
    parser: HelpParser,
    actions: list[argparse.Action],
    has_commands: bool,
) -> None:
    _heading(console, "Usage")
    command = " ".join(("mf", *parser.help_path))
    if not parser.help_path:
        _lines(console, ("  mf [global options] <command> [options]", "  mf <command> --help"))
        return
    if parser.usage is not None:
        _lines(console, (f"  {parser.usage % {'prog': command}}",))
        return
    if has_commands:
        _lines(console, (f"  {command} <command> [options]",))
        if not any(action.dest != "help" for action in actions):
            return
    if has_commands:
        _lines(console, (f"  {command} [options]",))
        return
    formatter = UsageFormatter(command, width=max(20, console.width - 2))
    formatter.add_usage(None, actions, parser._mutually_exclusive_groups, prefix="")
    _lines(console, ("  " + line for line in formatter.format_help().strip().splitlines()))


def _argument(
    formatter: argparse.HelpFormatter, action: argparse.Action, *, default: object
) -> tuple[str, str]:
    names = action.option_strings
    operand = formatter._format_args(action, action.dest.upper())
    name = ", ".join(names) if names else operand
    if names and action.nargs != 0:
        name += f" {operand}"
    label = (
        "Show help for this command."
        if isinstance(action, argparse._HelpAction)
        else formatter._expand_help(action)
        if action.help
        else ""
    )
    details: list[str] = []
    if names and action.required:
        details.append("Required.")
    if isinstance(
        action, (argparse._AppendAction, argparse._AppendConstAction, argparse._CountAction)
    ):
        if "repeat" not in label.lower():
            details.append("Repeatable.")
    if (
        isinstance(default, (int, float, str))
        and not isinstance(default, bool)
        and default not in ("", argparse.SUPPRESS)
        and "default" not in label.lower()
        and (names or action.nargs == "?")
    ):
        details.append(f"Default {default}.")
    return name, " ".join(filter(None, (label, *details)))


def render(parser: HelpParser, console: Console) -> None:
    console.width = min(console.width, 100)
    path = " ".join(parser.help_path)
    title = " · ".join(
        ("MarketForge", *(name.replace("-", " ").title() for name in parser.help_path))
    )
    console.print(Text(title, style="mf.heading"))
    summary = (
        SUMMARIES.get(path, parser.help_summary or parser.description or "")
        if path
        else "Financial Data Engineering · Quantitative Research · Algorithmic Trading"
    )
    if summary:
        console.print(Text(summary))
    commands = list(children(parser))
    actions = [
        action for action in parser._actions if not isinstance(action, argparse._SubParsersAction)
    ]
    _usage(console, parser, actions, bool(commands))
    _commands(console, path, commands)

    formatter = parser._get_formatter()
    sections: tuple[tuple[str, bool], ...] = (
        ("ARGUMENTS", False),
        ("OPTIONS" if path else "GLOBAL OPTIONS", True),
    )
    for title, optional in sections:
        rows = [
            _argument(formatter, action, default=parser.get_default(action.dest))
            for action in actions
            if bool(action.option_strings) == optional and action.help != argparse.SUPPRESS
        ]
        if rows:
            _heading(console, title)
            _rows(console, rows, style="mf.warning")

    behavior = [
        paragraph
        for paragraph in (
            parser.description if path else None,
            NOTES.get(path),
            parser.epilog,
        )
        if paragraph and paragraph != summary
    ]
    if behavior:
        _heading(console, "BEHAVIOR")
        _lines(console, behavior)

    if examples := EXAMPLES.get(path):
        _heading(console, "EXAMPLES" if path else "QUICK START")
        _lines(console, (f"  {example}" for example in examples), style="mf.success")
    if related := RELATED.get(path):
        _heading(console, "SEE ALSO")
        _lines(
            console,
            (f"  mf {path + ' ' if commands else ''}{topic} --help" for topic in related),
            style="mf.success",
        )
    if documentation := DOCUMENTATION.get(path):
        documentation_link(console, documentation)
    if not path:
        _heading(console, "DATABASE ENVIRONMENTS")
        _lines(
            console,
            (
                "Production is the default for database-backed commands. Where offered,",
                "use --testing or --production to select one environment explicitly.",
                "",
                "  mf database status",
                "  mf database status --testing",
            ),
        )
