"""CLI flags for shared collection sorting capabilities."""

import argparse
from collections.abc import Mapping

from forge.shared.sorting import SortSpec


def add_sorting(
    parser: argparse.ArgumentParser, spec: SortSpec | Mapping[str, SortSpec], *, notes: str = ""
) -> None:
    views = {"": spec} if isinstance(spec, SortSpec) else spec
    choices = tuple(
        dict.fromkeys(field for view in views.values() for field in (*view.fields, *view.aliases))
    )
    descriptions: list[str] = []
    for key, view in views.items():
        default = view.resolve()
        aliases = ", ".join(
            f"{alias} = {field}"
            for alias, field in view.aliases.items()
            if " " not in alias and alias.islower()
        )
        directions = "; ".join(
            f"{direction}: "
            + ", ".join(field for field, item in view.fields.items() if item.default == direction)
            for direction in ("asc", "desc")
            if any(item.default == direction for item in view.fields.values())
        )
        descriptions.append(
            (f"{key}: " if key else "")
            + directions
            + f" (default {default.field} {default.direction})"
            + (f"; aliases {aliases}" if aliases else "")
        )
    parser.add_argument(
        "--sort",
        metavar="FIELD",
        choices=choices,
        help=". ".join(descriptions) + ". " + notes,
    )
    parser.add_argument(
        "--order",
        choices=("asc", "desc"),
        help="Direction for the selected field, or the default field if --sort is omitted. "
        "Without --order, time, sizes and counts default to descending where available; identity fields default to ascending.",
    )
