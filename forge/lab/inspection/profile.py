"""Lightweight inspection reports using Parquet metadata."""

from __future__ import annotations

from dataclasses import dataclass

from forge.datasets.dataset import Stream


@dataclass(frozen=True)
class Inspection:
    stream: Stream

    def schema(self):
        return self.stream.schema()

    def summary(self) -> dict[str, object]:
        import pyarrow.parquet as pq

        details = self.stream.info()

        null_counts: dict[str, int | None] = {}

        for name in self.stream.schema().names:
            count = 0
            known = True

            for path in self.stream.files:
                file = pq.ParquetFile(path)

                for index in range(file.metadata.num_row_groups):
                    row_group = file.metadata.row_group(index)

                    column = next(
                        (
                            row_group.column(i)
                            for i in range(row_group.num_columns)
                            if row_group.column(i).path_in_schema == name
                        ),
                        None,
                    )

                    if (
                        column is None
                        or column.statistics is None
                        or column.statistics.null_count is None
                    ):
                        known = False
                        break

                    count += column.statistics.null_count

                if not known:
                    break

            null_counts[name] = count if known else None

        return {
            **details,
            "null_counts": null_counts,
            "caveat": (
                "Footer statistics only; " "not a full integrity certification."
            ),
        }


def inspect(stream: Stream) -> Inspection:
    return Inspection(stream)
