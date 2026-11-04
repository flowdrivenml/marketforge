"""Managed configuration name boundaries."""

from pathlib import Path

import pytest

from forge.shared.config.library import ConfigurationNameError, resolve_config_file


def test_optional_extension_selects_the_same_file(tmp_path: Path) -> None:
    assert resolve_config_file(tmp_path, "my-request") == resolve_config_file(
        tmp_path,
        "my-request.toml",
    )
    assert not (tmp_path / "my-request.toml").exists()


@pytest.mark.parametrize(
    "name",
    ["", "../escape", "/tmp/escape", "nested/file", ".hidden", "file.toml.toml"],
)
def test_rejects_paths_and_invalid_names(tmp_path: Path, name: str) -> None:
    with pytest.raises(ConfigurationNameError):
        resolve_config_file(tmp_path, name)


def test_rejects_symlinks_outside_the_library(tmp_path: Path) -> None:
    library = tmp_path / "library"
    library.mkdir()
    outside = tmp_path / "outside.toml"
    outside.write_text("version = 1\n", encoding="utf-8")
    (library / "escape.toml").symlink_to(outside)

    with pytest.raises(ConfigurationNameError, match="outside"):
        resolve_config_file(library, "escape")
