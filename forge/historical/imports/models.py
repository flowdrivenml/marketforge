"""Strict external-origin imports; filenames do not supply market semantics."""

import fnmatch
import os
import tomllib
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from forge.historical.archives.config import AcquisitionInterval
from forge.shared.config.models import SettingsModel
from forge.shared.errors import MarketForgeError


class ImportError(MarketForgeError):
    """An import cannot safely preserve the declared source identity/bytes."""


class ImportIdentity(SettingsModel, frozen=True):
    exchange: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    market_category: Literal["spot", "linear", "inverse"]
    instrument_type: Literal["spot", "perpetual"]
    symbol: str = Field(min_length=1, pattern=r"\S")

    @model_validator(mode="after")
    def validate_market(self) -> Self:
        if (self.instrument_type == "spot") != (self.market_category == "spot"):
            raise ValueError("Incompatible market and instrument type.")
        if self.symbol == "UNSPECIFIED":
            raise ValueError("Supply the actual instrument identity explicitly.")
        return self


class HistoricalSpecs(SettingsModel, frozen=True):
    as_of: AwareDatetime
    base_asset: str = Field(min_length=1)
    quote_asset: str = Field(min_length=1)
    quantity_type: Literal["base", "quote", "contracts"]
    contract_value: str | None = None
    contract_value_asset: str | None = None

    @model_validator(mode="after")
    def explicit_contract(self) -> Self:
        if self.quantity_type == "contracts":
            if self.contract_value is None or not self.contract_value_asset:
                raise ValueError("Contracts require explicit historical value and currency.")
            try:
                value = Decimal(self.contract_value)
            except InvalidOperation:
                raise ValueError("Contract value must be a decimal string.") from None
            if not value.is_finite() or value <= 0:
                raise ValueError("Contract value must be finite and positive.")
        return self


class ImportConfig(SettingsModel, frozen=True):
    version: Literal[1] = 1
    source_key: str = Field(min_length=1)
    source_kind: Literal["recording", "purchased", "other"] = "recording"
    root: str = Field(min_length=1)
    files: list[str] = Field(default_factory=lambda: list[str]())
    patterns: list[str] = Field(default_factory=lambda: list[str]())
    max_files: int = Field(default=1000, ge=1, le=10000)
    identity: ImportIdentity
    data_type: Literal["trades", "order_book_l2"]
    interval: AcquisitionInterval | None = None
    specifications: HistoricalSpecs | None = None
    format_profile: str | None = None

    @model_validator(mode="after")
    def selected(self) -> Self:
        if not self.files and not self.patterns:
            raise ValueError("Select explicit files or bounded patterns.")
        for pattern in self.patterns + self.files:
            if Path(pattern).is_absolute() or ".." in Path(pattern).parts:
                raise ValueError("Inputs must be paths beneath root, without parent traversal.")
        return self


def load_import(path: Path) -> ImportConfig:
    with path.open("rb") as source:
        return ImportConfig.model_validate(tomllib.load(source))


@dataclass(frozen=True)
class FrozenInput:
    reference: str
    path: str
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int

    def matches(self, stat: os.stat_result) -> bool:
        return (self.size, self.mtime_ns, self.ctime_ns, self.device, self.inode) == (
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
            stat.st_dev,
            stat.st_ino,
        )


def _matches(relative: str, pattern: str) -> bool:
    parts = relative.split("/")
    tokens = pattern.split("/")

    @lru_cache(maxsize=4096)
    def match(i: int, j: int) -> bool:
        if j == len(tokens):
            return i == len(parts)
        if tokens[j] == "**":
            return match(i, j + 1) or i < len(parts) and match(i + 1, j)
        return i < len(parts) and fnmatch.fnmatchcase(parts[i], tokens[j]) and match(i + 1, j + 1)

    return match(0, 0)


def freeze_inputs(config: ImportConfig) -> list[FrozenInput]:
    root = Path(config.root).expanduser().resolve()
    if not root.is_dir():
        raise ImportError("Import root is not a directory.")
    selected = set(config.files)
    entries = 0
    if config.patterns:
        for parent, dirs, files in os.walk(root, followlinks=False):
            entries += len(dirs) + len(files)
            if entries > 100000:
                raise ImportError(
                    "Pattern expansion exceeds 100000 inspected entries; narrow the root."
                )
            for file in files:
                relative = (Path(parent) / file).relative_to(root).as_posix()
                if any(_matches(relative, p) for p in config.patterns):
                    selected.add(relative)
                if len(selected) > config.max_files:
                    raise ImportError("Selected files exceed max_files.")
    if not selected:
        raise ImportError("No files matched the import selection.")
    if len(selected) > config.max_files:
        raise ImportError("Selected files exceed max_files.")
    result: list[FrozenInput] = []
    for relative in sorted(selected):
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ImportError("Source input is missing or escapes its root.")
        stat = path.stat()
        result.append(
            FrozenInput(
                relative,
                str(path),
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
                stat.st_dev,
                stat.st_ino,
            )
        )
    return result


def plan(config: ImportConfig) -> dict[str, object]:
    return {
        "version": 1,
        "configuration": config.model_dump(mode="json"),
        "inputs": [asdict(item) for item in freeze_inputs(config)],
        "normalization": "Requires an explicit historical specification and a compatible validated Rust profile; raw import alone is not normalization.",
    }
