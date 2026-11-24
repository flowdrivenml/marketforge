"""Legacy converter evidence and recorded byte formatting."""

from dataclasses import dataclass

from forge.tui.controller import document


@dataclass(frozen=True)
class Converted:
    records: tuple[dict[str, object], ...]
    checked_reports: int

    @property
    def rows(self) -> int | None:
        counts = [document(item.get("publication")).get("events") for item in self.records]
        if any(not isinstance(count, int) for count in counts):
            return None
        return sum(count for count in counts if isinstance(count, int))


def converted_from_reports(reports: list[dict[str, object]]) -> Converted:
    outputs: dict[str, dict[str, object]] = {}
    for item in reports:
        manifest = document(item.get("publication")).get("manifest")
        if item.get("status") == "complete" and isinstance(manifest, str) and manifest:
            outputs.setdefault(manifest, item)
    return Converted(tuple(outputs.values()), len(reports))


def size_label(size: int | None, missing: int = 0) -> str:
    if size is None:
        return "Unknown"
    value = float(size)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    return (
        (f"{value:,.1f}" if index else f"{size:,}")
        + f" {units[index]}"
        + (" partial" if missing else "")
    )
