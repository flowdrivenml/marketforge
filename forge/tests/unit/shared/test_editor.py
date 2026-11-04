"""External editor invocation behavior."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from forge.shared.terminal.editor import EditorError, edit_file


def test_editor_arguments_preserve_quoted_values_and_file_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "request with spaces.toml"
    source.write_text("version = 1\n", encoding="utf-8")
    invocations: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        assert check is False
        invocations.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=b"",
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setenv("VISUAL", "another-editor")

    result = edit_file(
        source,
        editor='my-editor --wait "two words"',
    )

    assert result == source
    assert invocations == [
        ["my-editor", "--wait", "two words", str(source)],
    ]


def test_failed_editor_returns_an_application_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "request.toml"
    source.write_text("version = 1\n", encoding="utf-8")

    def fake_run(
        command: list[str],
        *,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        assert check is False
        return subprocess.CompletedProcess(
            command,
            1,
            stdout=b"",
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(EditorError, match="code 1"):
        edit_file(source, editor="my-editor")
