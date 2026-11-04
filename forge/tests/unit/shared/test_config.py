"""Application configuration behavior."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from forge.shared.config.loader import ConfigurationLoadError, load_settings
from forge.shared.paths import ProjectPaths


def test_defaults() -> None:
    settings = load_settings()

    assert settings.version == 1
    assert settings.storage.data_root == "data"
    assert settings.presentation.mode == "progress"
    assert settings.presentation.color == "auto"


def test_loads_toml_settings(tmp_path: Path) -> None:
    source = tmp_path / "settings.toml"
    source.write_text(
        "version = 1\n"
        "\n"
        "[storage]\n"
        'data_root = "archive-storage"\n'
        "\n"
        "[presentation]\n"
        'mode = "silent"\n'
        'color = "never"\n',
        encoding="utf-8",
    )

    settings = load_settings(source)

    assert settings.storage.data_root == "archive-storage"
    assert settings.presentation.mode == "silent"
    assert settings.presentation.color == "never"


def test_empty_document_uses_defaults(tmp_path: Path) -> None:
    source = tmp_path / "settings.toml"
    source.write_text("", encoding="utf-8")

    assert load_settings(source) == load_settings()


@pytest.mark.parametrize(
    "document",
    [
        "version = 2\n",
        "version = true\n",
        "unknown_setting = true\n",
        '[storage]\ndata_rooot = "somewhere"\n',
        '[storage]\ndata_root = "   "\n',
        '[presentation]\nmode = "background"\n',
        '[presentation]\ncolor = "purple"\n',
        "storage = []\n",
    ],
)
def test_rejects_invalid_settings(
    tmp_path: Path,
    document: str,
) -> None:
    source = tmp_path / "settings.toml"
    source.write_text(document, encoding="utf-8")

    with pytest.raises(ValidationError):
        load_settings(source)


def test_missing_explicit_file_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationLoadError, match="Cannot read"):
        load_settings(tmp_path / "missing.toml")


def test_invalid_toml_is_an_error(tmp_path: Path) -> None:
    source = tmp_path / "settings.toml"
    source.write_text("[storage", encoding="utf-8")

    with pytest.raises(ConfigurationLoadError, match="Invalid TOML"):
        load_settings(source)


def test_custom_storage_keeps_configuration_at_workspace(
    tmp_path: Path,
) -> None:
    root = tmp_path / "marketforge"
    (root / "forge").mkdir(parents=True)
    (root / "engine").mkdir()
    (root / "forge" / "pyproject.toml").write_text(
        '[project]\nname = "marketforge"\n',
        encoding="utf-8",
    )

    source = tmp_path / "settings.toml"
    source.write_text(
        '[storage]\ndata_root = "custom-data"\n',
        encoding="utf-8",
    )

    settings = load_settings(source)
    paths = ProjectPaths.resolve(
        root=root,
        data_root=Path(settings.storage.data_root),
    )

    assert paths.data_root == root / "custom-data"
    assert paths.config_dir == root / "data" / ".config"
    assert paths.settings_file == paths.config_dir / "marketforge.toml"
    assert paths.pipeline_config_dir == paths.config_dir / "pipeline"
    assert not paths.data_root.exists()
