"""Private editable drafts and their pending review state."""

import json
import shutil
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import mkdtemp

type ValidateDraft = Callable[[Path], Awaitable[object]]
type ApplyDraft = Callable[[bytes, bytes], Awaitable[None]]
type QueueEditor = Callable[[Path, ValidateDraft, ApplyDraft, bytes | None], object]


@dataclass
class EditorSession:
    target: Path
    validate: ValidateDraft
    apply: ApplyDraft
    original: bytes | None = None
    draft: Path | None = None
    updated: bytes | None = None
    state: str = "Opening"
    error: str = ""
    busy: bool = True

    def stage(self) -> Path:
        if self.draft is not None:
            return self.draft
        original = self.target.read_bytes() if self.original is None else self.original
        directory = Path(mkdtemp(prefix="marketforge-edit-"))
        draft = directory / self.target.name
        try:
            (directory / ".session.json").write_text(
                json.dumps(
                    {"target": str(self.target), "original_sha256": sha256(original).hexdigest()}
                ),
                encoding="utf-8",
            )
            draft.write_bytes(original)
        except OSError:
            shutil.rmtree(directory)
            raise
        self.original, self.draft = original, draft
        return draft

    def discard(self) -> None:
        if self.draft is not None:
            shutil.rmtree(self.draft.parent)
