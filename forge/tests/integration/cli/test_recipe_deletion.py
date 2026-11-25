"""Deletion uses disposable managed libraries and leaves all retained data intact."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from forge.cli.main import main
from forge.shared.config.deletion import RecipeDependencies, delete_recipe
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths


@pytest.mark.parametrize(
    ("group", "family"), [("acquisition", "acquisition"), ("normalize", "normalization")]
)
def test_preview_apply_and_isolation(
    workspace: Path,
    request_file: Path,
    capsys: pytest.CaptureFixture[str],
    group: str,
    family: str,
) -> None:
    paths = ProjectPaths(workspace, workspace / "data")
    target = paths.workflow_dir(family) / "request.toml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(request_file.read_text())
    preserved = (
        paths.workflow_dir("merge") / "request.toml",
        paths.engine_config_dir / "normalize/profiles/trades/request.toml",
        paths.runs_dir / "normalize-123/report.json",
        paths.normalization_jobs_dir / "request.json",
        paths.raw_dir / "fixture.csv",
        paths.datasets_dir / "fixture.parquet",
    )
    for entry in preserved:
        entry.parent.mkdir(parents=True, exist_ok=True)
        entry.write_text("retained")

    args = ["--project-root", str(workspace), "--json", group, "delete", "request.toml"]
    assert main(args) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["status"] == "ready_to_delete" and preview["operation"] == "recipe_deletion"
    assert preview["path"] == str(target) and target.exists()
    assert main([*args, "--apply"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "deleted"
    assert not target.exists()
    assert all(entry.read_text() == "retained" for entry in preserved)
    assert main(["--project-root", str(workspace), group, "list"]) == 0
    assert "request" not in capsys.readouterr().out
    assert main(args) == 2
    assert "was not found" in json.loads(capsys.readouterr().err)["error"]["message"]


def test_live_merge_and_conversion_references_allow_only_explicit_override(
    workspace: Path,
    request_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = ProjectPaths(workspace, workspace / "data")
    normalization = paths.normalization_recipe_dir / request_file.name
    normalization.parent.mkdir()
    normalization.write_bytes(request_file.read_bytes())
    merge = paths.workflow_dir("merge") / "live.toml"
    merge.parent.mkdir()
    merge.write_text("""version = 1
[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-03T00:00:00Z
[selection]
source = "request"
""")
    conversion = paths.workflow_dir("conversion") / "live.toml"
    conversion.parent.mkdir()
    conversion.write_text(f"""version = 1
[source]
kind = "acquisition"
reference = "request.toml"
path = "{request_file}"
sha256 = "{"0" * 64}"
[interval]
start_ns = 0
end_ns = 1
[conversion]
target_quote = "USD"
max_staleness_ns = 1
""")
    for group, target, dependency in (
        ("acquisition", request_file, conversion),
        ("normalize", normalization, merge),
    ):
        args = ["--project-root", str(workspace), "--json", group, "delete", "request"]
        assert main(args) == 0
        assert json.loads(capsys.readouterr().out)["dependencies"]["references"] == [
            str(dependency)
        ]
        assert main([*args, "--force"]) == 2
        capsys.readouterr()
        assert main([*args, "--apply"]) == 1
        assert json.loads(capsys.readouterr().out)["status"] == "blocked"
        assert target.exists()
        assert main([*args, "--apply", "--force"]) == 0
        assert json.loads(capsys.readouterr().out)["status"] == "deleted"
        assert not target.exists() and dependency.is_file()


def test_refuses_unsafe_targets_and_conflicting_owner(workspace: Path, request_file: Path) -> None:
    directory = request_file.parent
    (directory / "link.toml").symlink_to(request_file)
    (directory / "folder.toml").mkdir()
    os.mkfifo(directory / "pipe.toml")
    args = ["--project-root", str(workspace), "acquisition", "delete"]
    for name in ("../request", str(request_file), "link", "folder", "pipe"):
        assert main([*args, name, "--apply", "--force"]) == 2
    paths = ProjectPaths(workspace, workspace / "data")
    with WorkspaceOwnership(paths):
        assert main([*args, "request"]) == 0
        assert main([*args, "request", "--apply"]) == 2
    assert request_file.is_file()


def test_rejects_replaced_file_and_symlinked_library(workspace: Path, request_file: Path) -> None:
    paths = ProjectPaths(workspace, workspace / "data")

    def replace(path: Path) -> RecipeDependencies:
        path.rename(path.with_suffix(".original"))
        path.write_text("replacement")
        return RecipeDependencies()

    with pytest.raises(MarketForgeError, match="changed while"):
        delete_recipe(paths, "acquisition", "request", apply=True, dependencies=replace)
    assert request_file.read_text() == "replacement"
    assert request_file.with_suffix(".original").is_file()
    directory = request_file.parent
    displaced = directory.with_name("elsewhere")
    directory.rename(displaced)
    directory.symlink_to(displaced, target_is_directory=True)
    with pytest.raises(OSError):
        delete_recipe(paths, "acquisition", "request", apply=True, dependencies=replace)
    assert (displaced / "request.toml").read_text() == "replacement"


def test_recoverable_archive_and_restore_conflict(workspace: Path, request_file: Path) -> None:
    from forge.shared.config.deletion import archived_recipes, restore_recipe

    paths = ProjectPaths(workspace, workspace / "data")
    original = request_file.read_bytes()
    result = delete_recipe(
        paths, "acquisition", "request", apply=True, dependencies=lambda _: RecipeDependencies()
    )
    assert result.archived_path is not None
    assert archived_recipes(paths, "acquisition") == (result.archived_path,)
    assert result.archived_path.read_bytes() == original
    request_file.write_bytes(b"# newer recipe")
    with pytest.raises(MarketForgeError, match="already exists"):
        restore_recipe(paths, "acquisition", result.archived_path)
    assert request_file.read_bytes() == b"# newer recipe"
    request_file.unlink()
    assert restore_recipe(paths, "acquisition", result.archived_path) == request_file
    assert request_file.read_bytes() == original
    assert not archived_recipes(paths, "acquisition")


def test_pending_recipe_dependency_and_review_fingerprint(
    workspace: Path, request_file: Path
) -> None:
    from forge.historical.recipe_dependencies import inspect_references

    paths = ProjectPaths(workspace, workspace / "data")
    intent = paths.runs_dir / "acquisition-fixture" / "attempts" / "id" / "intent.json"
    intent.parent.mkdir(parents=True)
    intent.write_text(
        json.dumps(
            {
                "operation": "acquisition",
                "action": "download",
                "recipe_source": str(request_file),
            }
        )
    )

    def dependencies(path: Path) -> RecipeDependencies:
        return inspect_references(paths, "acquisition", path)

    blocked = delete_recipe(paths, "acquisition", "request", apply=True, dependencies=dependencies)
    assert blocked.status == "blocked"
    assert blocked.dependencies.executions == (intent,)
    original = request_file.read_bytes()
    intent.with_name("recipe.toml").write_bytes(original)
    intent.with_name("recipe-source.json").write_text(
        json.dumps({"path": str(request_file), "sha256": hashlib.sha256(original).hexdigest()})
    )
    preview = delete_recipe(paths, "acquisition", "request", dependencies=dependencies)
    assert preview.status == "ready_to_delete"
    request_file.write_bytes(request_file.read_bytes() + b"\n# External edit\n")
    with pytest.raises(MarketForgeError, match="changed since review"):
        delete_recipe(
            paths,
            "acquisition",
            "request",
            apply=True,
            expected_sha256=preview.sha256,
            dependencies=dependencies,
        )
