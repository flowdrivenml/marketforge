"""Origin-independent evidence for one verified managed normalization input."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RegisteredInput:
    local_copy_id: int
    path: Path
    sha256: str
    source_object_id: int | None
    provenance: dict[str, Any]
    instrument_or_scope: dict[str, Any]
    specifications: dict[str, Any] | None
    profile_id: str | None
    profile_fingerprint: str | None
    declared_coverage: tuple[str | None, str | None]
    observed_evidence: dict[str, Any] | None
    readiness_blockers: tuple[str, ...]

    @classmethod
    def from_row(cls, row: dict[str, Any], path: Path) -> "RegisteredInput":
        provenance: dict[str, Any] = (
            row.get("import_provenance") or row.get("source_metadata") or {}
        )
        binding: dict[str, Any] = provenance.get("binding") or {}
        provenance = {
            **provenance,
            "source": {
                key: row.get(key)
                for key in (
                    "raw_source_id",
                    "raw_source_key",
                    "provider",
                    "product",
                    "acquisition_method",
                    "source_provenance",
                )
            },
        }
        readiness: dict[str, Any] = binding.get("readiness") or {}
        return cls(
            local_copy_id=int(row["id"]),
            path=path,
            sha256=row["sha256"],
            source_object_id=row.get("raw_object_id"),
            provenance=provenance,
            instrument_or_scope={
                key: row.get(key)
                for key in (
                    "exchange",
                    "instrument_type",
                    "market_category",
                    "scope_kind",
                    "scope_identifier",
                    "feed",
                    "feed_source",
                )
            },
            specifications=row.get("specifications_json"),
            profile_id=row.get("format_profile"),
            profile_fingerprint=binding.get("profile_fingerprint"),
            declared_coverage=(
                str(row["start_at"]) if row.get("start_at") is not None else None,
                str(row["end_at"]) if row.get("end_at") is not None else None,
            ),
            observed_evidence=None,
            readiness_blockers=tuple(readiness.get("blockers", [])),
        )
