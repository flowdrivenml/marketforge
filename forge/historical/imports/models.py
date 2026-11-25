"""Strict external-origin imports; filenames do not supply market semantics."""

import fnmatch
import os
import tomllib
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Literal, Self

from pydantic import AwareDatetime, Field, field_validator, model_validator

from forge.historical.archives.config import AcquisitionInterval
from forge.shared.config.loader import configuration_errors
from forge.shared.config.models import SettingsModel
from forge.shared.errors import MarketForgeError


class ImportError(MarketForgeError):
    """An import cannot safely preserve the declared source identity/bytes."""


class ImportIdentity(SettingsModel, frozen=True):
    exchange: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    market_category: Literal["spot", "linear", "inverse", "equity", "option"]
    instrument_type: Literal["spot", "perpetual", "future", "equity", "option"]
    symbol: str = Field(min_length=1, pattern=r"^\S+$")

    @model_validator(mode="after")
    def validate_market(self) -> Self:
        if self.exchange == "external":
            raise ValueError(
                "Supply the actual trading venue; acquisition origin is separate provenance."
            )
        if any(
            (self.instrument_type == kind) != (self.market_category == kind)
            for kind in ("spot", "equity", "option")
        ):
            raise ValueError("Incompatible market and instrument type.")
        if self.symbol == "UNSPECIFIED":
            raise ValueError("Supply the actual instrument identity explicitly.")
        return self


class HistoricalSpecs(SettingsModel, frozen=True):
    as_of: AwareDatetime | None = None
    fetched_at: AwareDatetime | None = None
    base_asset: str | None = Field(default=None, min_length=1)
    quote_asset: str | None = Field(default=None, min_length=1)
    settlement_asset: str | None = None
    quantity_type: Literal["base", "quote", "contracts"]
    contract_value: str | None = None
    contract_value_asset: str | None = None
    tick_size: Decimal | None = None
    qty_step: Decimal | None = None
    min_qty: Decimal | None = None
    max_qty: Decimal | None = None
    min_notional: Decimal | None = None
    min_notional_asset: str | None = None
    max_notional: Decimal | None = None
    max_notional_asset: str | None = None
    valid_from: AwareDatetime | None = None
    valid_to: AwareDatetime | None = None
    provenance: str | None = None

    @field_validator(
        "tick_size", "qty_step", "min_qty", "max_qty", "min_notional", "max_notional", mode="before"
    )
    @classmethod
    def exact_decimal(cls, value: object) -> object:
        if type(value) is int:
            return Decimal(value)
        if isinstance(value, str):
            try:
                return Decimal(value)
            except InvalidOperation:
                raise ValueError("Expected an exact decimal string.") from None
        if isinstance(value, float):
            raise ValueError("Use a quoted exact decimal or a TOML integer.")
        return value

    @field_validator("as_of", "fetched_at", "valid_from", "valid_to", mode="before")
    @classmethod
    def timestamp(cls, value: object) -> object:
        parsed = (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            if isinstance(value, str)
            else value
        )
        return (
            parsed.astimezone(UTC)
            if isinstance(parsed, datetime) and parsed.tzinfo is not None
            else parsed
        )

    @model_validator(mode="after")
    def explicit_contract(self) -> Self:
        if (self.valid_from is None) != (self.valid_to is None):
            raise ValueError("Supply both historical validity bounds.")
        if self.as_of is None and self.fetched_at is None and self.valid_from is None:
            raise ValueError("Current specifications require retrieval as_of or fetched_at.")
        if self.as_of is not None and self.fetched_at is not None and self.as_of != self.fetched_at:
            raise ValueError("as_of and fetched_at disagree about retrieval time.")
        if self.fetched_at is None:
            object.__setattr__(self, "fetched_at", self.as_of)
        if self.as_of is None:
            object.__setattr__(self, "as_of", self.fetched_at)
        for name in ("tick_size", "qty_step"):
            value = getattr(self, name)
            if value is not None and (not value.is_finite() or value <= 0):
                raise ValueError(f"{name} must be finite and positive when supplied.")
        for name in ("min_qty", "max_qty", "min_notional", "max_notional"):
            value = getattr(self, name)
            if value is not None and (not value.is_finite() or value < 0):
                raise ValueError(f"{name} must be finite and nonnegative when supplied.")
        if self.min_qty is not None and self.max_qty is not None and self.min_qty > self.max_qty:
            raise ValueError("min_qty exceeds max_qty.")
        if (
            self.min_notional is not None
            and self.max_notional is not None
            and self.min_notional_asset == self.max_notional_asset
            and self.min_notional > self.max_notional
        ):
            raise ValueError("min_notional exceeds max_notional.")
        if self.valid_from and self.valid_to and self.valid_from >= self.valid_to:
            raise ValueError("Historical validity interval is empty.")
        if self.contract_value is not None:
            try:
                contract = Decimal(self.contract_value)
            except InvalidOperation:
                raise ValueError("Contract value must be a decimal string.") from None
            if not contract.is_finite() or contract <= 0 or not self.contract_value_asset:
                raise ValueError("Contract value requires a positive exact value and denomination.")
        if self.contract_value_asset is not None and self.contract_value is None:
            raise ValueError("Contract denomination requires an explicit value.")
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
    binding: dict[str, object] | None = None

    @model_validator(mode="after")
    def selected(self) -> Self:
        if self.specifications and not (
            self.specifications.base_asset and self.specifications.quote_asset
        ):
            raise ValueError("Import specifications require base_asset and quote_asset.")
        if not self.files and not self.patterns:
            raise ValueError("Select explicit files or bounded patterns.")
        for pattern in self.patterns + self.files:
            if Path(pattern).is_absolute() or ".." in Path(pattern).parts:
                raise ValueError("Inputs must be paths beneath root, without parent traversal.")
        return self


def load_import(path: Path) -> ImportConfig:
    return read_import(path)[0]


def read_import(path: Path) -> tuple[ImportConfig, bytes]:
    with configuration_errors(path):
        submitted = path.read_bytes()
        config = ImportConfig.model_validate(tomllib.loads(submitted.decode()))
        if config.binding is not None:
            raise ImportError(
                "Legacy import is raw-only. Use acquisition register for a verified binding."
            )
        return config, submitted


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
    if ".agents" in root.parts:
        raise ImportError("Forbidden source root.")
    if any(".agents" in Path(value).parts for value in config.files + config.patterns):
        raise ImportError("Forbidden input path.")
    selected = set(config.files)
    entries = 0
    if config.patterns:
        for parent, dirs, files in os.walk(root, followlinks=False):
            dirs[:] = [
                name
                for name in dirs
                if name != ".agents" and not (Path(parent) / name).is_symlink()
            ]
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
        if (root / relative).is_symlink():
            raise ImportError("Source symlinks are not managed-copy inputs.")
        path = (root / relative).resolve()
        if ".agents" in path.parts or not path.is_relative_to(root) or not path.is_file():
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
