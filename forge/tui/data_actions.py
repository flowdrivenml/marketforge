"""Data menus and recipe drafts shared by the workspace inventory tabs."""

from __future__ import annotations

import tomllib
from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING

from forge.historical.archives.storage.files import disk
from forge.shared.database.connection import database_environment, pinned_registry
from forge.tui import data_recipes
from forge.tui.actions import Action, Field
from forge.tui.clipboard import plain
from forge.tui.controller import document
from forge.tui.inspection import ActionMenu
from forge.tui.recipe_forms import model_for
from forge.tui.resources import Row
from forge.tui.screens import Confirm, Detail, Form

if TYPE_CHECKING:
    from forge.tui.app import ForgeApp


def row_actions(self: ForgeApp, row: Row | None, anchor: tuple[int, int] | None) -> None:
    view = self.view
    more: dict[str, Callable[[], object]] = {
        "Preview dataset deduplication…": self.dataset_deduplication,
    }
    if view == "Raw":
        more["Browse normalization profiles"] = partial(
            self.open_workspace_view, "Catalogue", "Profiles"
        )
    choices: dict[str, Callable[[], object]] = {}
    title = view
    if row is not None:
        selected = self.selected_rows(row)
        value = document(row.value)
        identity = row.reference or row.id
        if view == "Raw":
            identity = "\n".join(f"local:{ref}" for ref in data_recipes.archive_ids((row,)))
            title = (
                " · ".join(
                    str(value[key])
                    for key in ("exchange", "symbol", "market_category", "data_type")
                    if value.get(key)
                )
                or row.name
            )
        elif view == "Normalized":
            title = f"Normalized publication {identity} · {row.name}"
        elif view == "Merged":
            title = f"Merged publication {identity}"
        else:
            title = f"Converted · {identity}"
        if len(selected) > 1:
            title += f" · {len(selected)} selected rows"
        choices = {
            "Add to Recipe": partial(
                self.add_data_to_recipe, view, selected, self.database_id, self.context.testing
            ),
            "Inspect": partial(self.open_row, row),
            "Copy Selected Text": partial(
                self.copy_data_selection, self.resource_table().selected_text
            ),
            "Copy Row": partial(self.copy_dashboard_id, plain(row.value)),
            "Copy Identifier": partial(self.copy_dashboard_id, identity),
        }
        if view in {"Normalized", "Merged"}:
            more["Physical partitions"] = partial(
                self.inspect_partitions, {"publication_id": int(identity)}
            )
            more["View supersession evidence"] = partial(self.inspect_supersession, int(identity))
        if view == "Converted":
            for verb, label in (
                ("lineage", "Inspect lineage"),
                ("verify", "Verify output"),
                ("head", "Sample output"),
            ):
                more[label] = partial(self.open_indexed_output, identity, verb)
        if self.context.testing and view == "Converted":
            more["Preview output deletion…"] = partial(self.delete_publications, selected)
        if self.context.testing and view in {"Normalized", "Merged"}:
            more["Preview publication deletion…"] = partial(self.delete_publications, selected)
    if self.state.basket:
        more["Clear selection"] = self.clear_basket
    choices["More Actions"] = partial(self.show_data_actions, title, more)
    self.resource_table().clear_text_selection()
    self.show_data_actions(title, choices, primary="Add to Recipe", anchor=anchor)


def accepts_recipe(view: str, rows: tuple[Row, ...], *, testing: bool) -> bool:
    match view:
        case "Raw":
            return testing and bool(data_recipes.archive_ids(rows))
        case "Normalized":
            return all(
                document(row.value).get("stream") and document(row.value).get("primary_files")
                for row in rows
            )
        case "Merged":
            return len({row.reference or row.id for row in rows}) == 1
        case _:
            return False


async def add_data_to_recipe(
    self: ForgeApp, view: str, rows: tuple[Row, ...], registry: str | None, testing: bool
) -> None:
    if self.recipe_activation or self.editor_open:
        return
    if view == "Converted":
        output = rows[0]
        self.push_screen(
            Detail(
                "Add to Recipe",
                "No compatible downstream recipe currently consumes indexed Conversion outputs.",
                raw=True,
                actions={
                    "Inspect output": partial(self.open_row, output),
                    "Copy Output Reference": partial(self.copy_dashboard_id, output.reference),
                },
            )
        )
        return
    family = data_recipes.DESTINATIONS[view]
    paths = self.context.paths
    self.recipe_activation = True
    try:
        if registry is None:
            raise ValueError("Wait for the registry identity to be verified.")
        self.check_recipe_environment(registry, testing)
        if not accepts_recipe(view, rows, testing=testing):
            raise ValueError(
                {
                    "Raw": "Select registered archive IDs in the testing registry.",
                    "Normalized": "Each selected row requires an exact stream and registered primary-file IDs.",
                    "Merged": "Conversion accepts one committed source publication. Select one Merged row.",
                }[view]
            )
        with database_environment(testing=testing), pinned_registry(registry):
            if view == "Raw":
                ids = data_recipes.archive_ids(rows)
                if len(ids) > 1:
                    fields = tuple(
                        Field(
                            f"archive-{ref}",
                            f"Registered archive local:{ref}",
                            "check",
                            default=True,
                        )
                        for ref in ids
                    )
                    values = await self.push_screen_wait(
                        Form(
                            Action("Review selected archives", "Data", fields),
                            local=True,
                            submit_label="Continue",
                        )
                    )
                    if values is None:
                        return
                    ids = tuple(ref for ref in ids if values[f"archive-{ref}"])
                    if not ids:
                        raise ValueError("Select at least one registered archive.")

                async def bind_sources(
                    before: dict[str, object],
                ) -> tuple[dict[str, object], str, bool]:
                    return await data_recipes.normalization(paths, ids, registry, before)
            else:
                resolver = data_recipes.merge if view == "Normalized" else data_recipes.conversion
                bind_sources = partial(resolver, paths, rows, registry)

            seed, evidence, _ = await bind_sources({})
            workflow = {
                "normalize": "Normalization",
                "merge": "Merge",
                "convert": "Conversion",
            }[family]

            async def describe(original: bytes) -> tuple[bool, str]:
                before = tomllib.loads(original.decode())
                model = model_for(family, before)
                if model is not None:
                    model.model_validate(before)
                return data_recipes.destination_state(family, before, seed)

            new, existing = "Create New Recipe", "Add to Existing Recipe"
            title = f"Add {view} to {workflow} · {len(rows)} selected rows"
            while True:
                destination = await self.push_screen_wait(
                    ActionMenu(title, (new, existing, "Review Selected Sources"), primary=new)
                )
                if destination != "Review Selected Sources":
                    break
                await self.push_screen_wait(Detail("Exact selected sources", evidence, raw=True))
            if destination is None:
                return
            row = None
            original = None
            if destination == existing:
                path = await self.select_existing_recipe(family, workflow, title, describe)
                if path is None:
                    return
                self.check_recipe_environment(registry, testing)
                original = await disk(path.read_bytes)
                seed, evidence, duplicate = await bind_sources(tomllib.loads(original.decode()))
                if duplicate:
                    self.push_screen(
                        Detail("Already Selected", "Source already selected.", raw=True)
                    )
                    return
                if family == "convert" and not await self.push_screen_wait(
                    Confirm("Replace Source", evidence, confirm_label="Open replacement draft")
                ):
                    return
                row = Row(path.stem, path.stem, family, "", {}, path.stem, path)
            self.check_recipe_environment(registry, testing)
            await self.author_recipe(
                family,
                row,
                initial=seed,
                expected=original,
                source_review=evidence,
            )
    except Exception as error:
        self.push_screen(
            Detail("Add to Recipe · Incompatible Source", str(error), raw=True, error=True)
        )
    finally:
        self.recipe_activation = False
