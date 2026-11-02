from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from marketforge.acquisition.http import HttpClient, RequestPolicy
from marketforge.models import (
    AcquisitionPlan,
    DownloadResult,
    DownloadStatus,
    PlannedDownload,
    RawFileRecord,
)
from marketforge.storage.manifest import verify_raw_file, write_raw_record

DEFAULT_CHUNK_SIZE = 1024 * 1024

ProgressCallback = Callable[
    [PlannedDownload, int, int | None],
    None,
]


class Downloader:
    """Download remote archives safely to raw storage."""

    def __init__(
        self,
        http: HttpClient | None = None,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than zero")

        self.http = http or HttpClient()
        self.chunk_size = chunk_size

    def download(
        self,
        planned: PlannedDownload,
        *,
        policy: RequestPolicy | None = None,
        progress: ProgressCallback | None = None,
    ) -> DownloadResult:
        """Download one remote archive."""

        destination = planned.destination

        existing = verify_raw_file(destination)

        if existing is not None:
            return DownloadResult(
                remote_file=planned.remote_file,
                path=destination,
                bytes_written=existing.bytes_written,
                sha256=existing.sha256,
                status=DownloadStatus.REUSED,
            )

        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        partial = self._partial_path(destination)

        # Never append to an old incomplete transfer.
        partial.unlink(missing_ok=True)

        hasher = hashlib.sha256()
        bytes_written = 0

        try:
            response = self.http.get(
                planned.remote_file.url,
                policy=policy,
                stream=True,
            )

            try:
                with partial.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if not chunk:
                            continue

                        output.write(chunk)

                        hasher.update(chunk)

                        bytes_written += len(chunk)
                        if progress is not None:
                            progress(
                                planned,
                                bytes_written,
                                planned.remote_file.size_bytes,
                            )

                    output.flush()

                    os.fsync(output.fileno())

            finally:
                response.close()

            # self._verify_size(
            #     planned=planned,
            #     bytes_written=bytes_written,
            # )

            # os.replace() is atomic when source and destination
            # are on the same filesystem.
            os.replace(
                partial,
                destination,
            )

            record = RawFileRecord(
                remote_file=planned.remote_file,
                path=destination,
                bytes_written=bytes_written,
                sha256=hasher.hexdigest(),
                downloaded_at=datetime.now(timezone.utc),
            )

            write_raw_record(record)

        except Exception:
            partial.unlink(missing_ok=True)
            raise

        return DownloadResult(
            remote_file=planned.remote_file,
            path=destination,
            bytes_written=bytes_written,
            sha256=hasher.hexdigest(),
            status=DownloadStatus.DOWNLOADED,
        )

    @staticmethod
    def _partial_path(
        destination: Path,
    ) -> Path:
        """Return the temporary path used during a download."""

        return destination.with_name(destination.name + ".part")

    @staticmethod
    def _verify_size(
        *,
        planned: PlannedDownload,
        bytes_written: int,
    ) -> None:
        """Verify exact source size metadata when available."""

        expected = planned.remote_file.size_bytes

        if expected is None:
            return

        if bytes_written != expected:
            raise ValueError(
                "Downloaded size does not match expected size: "
                f"expected={expected}, "
                f"actual={bytes_written}, "
                f"file={planned.remote_file.filename}"
            )

    def download_many(
        self,
        plan: AcquisitionPlan,
        *,
        policy: RequestPolicy | None = None,
        progress: ProgressCallback | None = None,
    ) -> list[DownloadResult]:
        """Execute all downloads in an acquisition plan sequentially."""

        results: list[DownloadResult] = []

        for planned in plan.downloads:
            result = self.download(
                planned,
                policy=policy,
                progress=progress,
            )
            results.append(result)

        return results
