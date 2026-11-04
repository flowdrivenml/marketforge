"""Basic content and compression validation without network or database access."""

import gzip
import io
import zipfile
from pathlib import Path

import pytest

from forge.historical.archives.storage.files import ArchiveFileError, validate_file


@pytest.mark.parametrize(
    "content",
    [
        b"<html>error</html>",
        gzip.compress(b"<html>error</html>"),
        b"random document",
        b"\x1f\x8bbroken",
        b"",
    ],
)
def test_rejects_non_archives_and_error_documents(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "archive"
    path.write_bytes(content)
    with pytest.raises(ArchiveFileError):
        validate_file(path)


def test_zip_is_validated_without_extracting_members(tmp_path: Path) -> None:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("../../outside.csv", b"a,b\n1,2\n")
    path = tmp_path / "archive.zip"
    path.write_bytes(output.getvalue())
    validate_file(path)
    assert list(tmp_path.iterdir()) == [path]
