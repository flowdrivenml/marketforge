"""Inspect and change the managed normalization runtime through the Rust contract."""

import hashlib
import json
import math
import os
import re
import stat
import tomllib
from collections.abc import Generator, Mapping
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast
from uuid import uuid4

import tomlkit
from tomlkit.container import Container, OutOfOrderTableProxy
from tomlkit.exceptions import ParseError
from tomlkit.items import InlineTable, String, Table

from forge.historical.engine.managed import resolve, supervised
from forge.historical.engine.runtime import inspect_runtime
from forge.shared.config.toml import configuration_item
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership, acquire_file_lock
from forge.shared.paths import ProjectPaths

MISSING = object()
UNITS = {"": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}
BYTE_VALUE = re.compile(r"([0-9]+)(?:\.([0-9]+))?[ \t]*(KiB|MiB|GiB|TiB)?")
RATE_VALUE = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
LEGACY_ENABLED = "integrity_policy.profile"


def value_at(document: Mapping[str, Any], path: str) -> Any:
    value: Any = document
    for part in path.split("."):
        if not isinstance(value, dict):
            return MISSING
        value = cast(dict[str, Any], value).get(part, MISSING)
    return value


def configured_at(document: Mapping[str, Any], path: str) -> tuple[Any, str | None]:
    value = value_at(document, path)
    if path == "integrity_policy.enabled" and value is MISSING:
        legacy = value_at(document, LEGACY_ENABLED)
        if legacy is not MISSING:
            return legacy, LEGACY_ENABLED
    return value, path if value is not MISSING else None


def native(value: Any) -> Any:
    # Absent optional Rust fields serialize as null. Keep presence separate.
    if value is MISSING:
        return None
    if value is None or type(value) in {str, bool, int}:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    # An invalid original can be repaired with a valid candidate. Keep its
    # diagnostic value representable without nonstandard JSON or implicit coercion.
    return repr(value)


def typed_value(field: Mapping[str, Any], raw: str) -> str | bool | int | float:
    kind = field["kind"]
    if kind in {"string", "path"}:
        return raw
    if kind == "enum":
        if raw in field["choices"]:
            return raw
        raise MarketForgeError(f"{field['path']}: choose {', '.join(field['choices'])}.")
    if kind == "boolean":
        if raw in {"true", "false"}:
            return raw == "true"
        raise MarketForgeError(f"{field['path']}: use true or false.")
    if kind == "rate":
        if RATE_VALUE.fullmatch(raw) and math.isfinite(number := float(raw)):
            return number
        raise MarketForgeError(f"{field['path']}: expected a finite decimal number.")
    if kind in {"bytes", "mib"}:
        match = BYTE_VALUE.fullmatch(raw)
        if match is None:
            raise MarketForgeError(f"{field['path']}: use bytes, KiB, MiB, GiB, or TiB.")
        whole, fraction, unit = match.groups()
        if fraction and not unit:
            raise MarketForgeError(f"{field['path']}: bare bytes must be an integer.")
        scale = 10 ** len(fraction or "")
        amount, remainder = divmod(int(whole + (fraction or "")) * UNITS[unit or ""], scale)
        if kind == "mib" and unit:
            amount, residual_bytes = divmod(amount, 1 << 20)
            if residual_bytes:
                raise MarketForgeError(f"{field['path']}: use an exact whole number of MiB.")
        if remainder:
            raise MarketForgeError(f"{field['path']}: value must be an exact number of bytes.")
    elif kind in {"integer", "milliseconds"} and re.fullmatch(r"[0-9]+", raw):
        amount = int(raw)
    else:
        raise MarketForgeError(f"{field['path']}: expected an unsigned integer.")
    if amount >= 1 << field["integer_bits"]:
        raise MarketForgeError(f"{field['path']}: value exceeds the Engine integer range.")
    return amount


@contextmanager
def directory_fd(directory: Path, *, create: bool = False) -> Generator[int]:
    """Pin each component without following symlinks."""
    with ExitStack() as stack:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        fd = os.open(directory.anchor, flags)
        stack.callback(os.close, fd)
        for part in directory.parts[1:]:
            if part == "..":
                raise MarketForgeError("Unsafe runtime configuration directory.")
            if create:
                with suppress(FileExistsError):
                    os.mkdir(part, dir_fd=fd)
            fd = os.open(part, flags, dir_fd=fd)
            stack.callback(os.close, fd)
        yield fd


def identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_uid,
        info.st_gid,
        info.st_mtime_ns,
        info.st_ctime_ns,
        info.st_size,
    )


@dataclass(frozen=True)
class Snapshot:
    data: bytes
    info: os.stat_result
    directory: os.stat_result


def read_entry(fd: int, name: str, *, limit: int | None = None) -> Snapshot:
    entry = os.stat(name, dir_fd=fd, follow_symlinks=False)
    if not stat.S_ISREG(entry.st_mode) or entry.st_nlink != 1:
        raise MarketForgeError(f"Expected a regular runtime file with one link: {name}")
    if limit is not None and entry.st_size > limit:
        raise MarketForgeError(f"Configuration exceeds inspection limit: {name}")
    handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    with os.fdopen(handle, "rb") as stream:
        if identity(os.fstat(stream.fileno())) != identity(entry):
            raise MarketForgeError("Runtime configuration changed while it was being read.")
        data = stream.read() if limit is None else stream.read(limit + 1)
        if identity(os.fstat(stream.fileno())) != identity(entry):
            raise MarketForgeError("Runtime configuration changed while it was being read.")
    return Snapshot(data, entry, os.fstat(fd))


def read_snapshot(path: Path, *, limit: int | None = None) -> Snapshot:
    try:
        with directory_fd(path.parent) as fd:
            return read_entry(fd, path.name, limit=limit)
    except OSError as error:
        raise MarketForgeError(
            f"Cannot safely read runtime configuration {path}: {error}"
        ) from error


def parse_document(data: bytes) -> dict[str, Any]:
    try:
        return tomllib.loads(data.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as error:
        raise MarketForgeError(f"Invalid runtime TOML: {error}") from error


def replace_entry(path: Path, original: Snapshot, data: bytes) -> None:
    """Replace the pinned entry after checking its inode, contents, and parent."""
    temporary = f".{path.name}.{uuid4().hex}.tmp"
    with directory_fd(path.parent) as fd:
        if not os.path.samestat(os.fstat(fd), original.directory):
            raise MarketForgeError("Runtime configuration directory changed. Preview again.")
        handle = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd)
        try:
            with os.fdopen(handle, "wb") as output:
                output.write(data)
                output.flush()
                current = os.fstat(output.fileno())
                if (current.st_uid, current.st_gid) != (original.info.st_uid, original.info.st_gid):
                    os.fchown(output.fileno(), original.info.st_uid, original.info.st_gid)
                os.fchmod(output.fileno(), stat.S_IMODE(original.info.st_mode))
                os.fsync(output.fileno())
            with directory_fd(path.parent) as current_fd:
                current = read_entry(current_fd, path.name)
                if (
                    not os.path.samestat(current.directory, original.directory)
                    or identity(current.info) != identity(original.info)
                    or current.data != original.data
                ):
                    raise MarketForgeError("Runtime configuration changed. Preview again.")
                os.replace(temporary, path.name, src_dir_fd=fd, dst_dir_fd=current_fd)
                try:
                    os.fsync(current_fd)
                except OSError as error:
                    raise MarketForgeError(
                        f"Runtime configuration was replaced, but directory sync failed: {error}"
                    ) from error
        finally:
            try:
                os.unlink(temporary, dir_fd=fd)
            except FileNotFoundError:
                pass


def create_entry(path: Path, data: bytes) -> None:
    """Publish bytes atomically without replacing an externally created file."""
    temporary = f".{path.name}.{uuid4().hex}.tmp"
    with directory_fd(path.parent, create=True) as fd:
        handle = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd)
        try:
            with os.fdopen(handle, "wb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            with directory_fd(path.parent) as current_fd:
                if not os.path.samestat(os.fstat(current_fd), os.fstat(fd)):
                    raise MarketForgeError("Configuration directory changed. Reload the file.")
                try:
                    os.link(temporary, path.name, src_dir_fd=fd, dst_dir_fd=current_fd)
                except FileExistsError:
                    raise MarketForgeError(
                        "Configuration was created externally. Reload the file."
                    ) from None
            os.unlink(temporary, dir_fd=fd)
            os.fsync(fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=fd)


@dataclass(frozen=True)
class Change:
    original: Snapshot
    candidate: bytes
    fields: tuple[dict[str, Any], ...]
    schema_version: int
    requires_acknowledgement: bool
    engine_identity: tuple[int, ...] | None = None

    def report(self, *, status: str) -> dict[str, Any]:
        return {
            "status": status,
            "runtime_schema_version": self.schema_version,
            "validation": {"toml": True, "rust": True},
            "original_sha256": hashlib.sha256(self.original.data).hexdigest(),
            "candidate_sha256": hashlib.sha256(self.candidate).hexdigest(),
            "requires_risk_acknowledgement": self.requires_acknowledgement,
            "changes": list(self.fields),
        }


RuntimeOperation = Literal["normalize", "merge", "convert"]


class RuntimeConfiguration:
    """Rust-backed service for the authoritative normalization TOML."""

    def __init__(self, paths: ProjectPaths, operation: RuntimeOperation = "normalize") -> None:
        self.paths = paths
        self.operation = operation
        self.label = "normalization" if operation == "normalize" else operation
        self.path = paths.engine_config_dir / f"{operation}.toml"
        self._schema: dict[str, Any] | None = None
        self._schema_engine: tuple[int, ...] | None = None

    def schema(self) -> dict[str, Any]:
        binary = resolve(self.paths, self.operation)
        installed = identity(binary.stat())
        if self._schema is None or self._schema_engine != installed:
            result = supervised([str(binary), "--runtime-schema"], cwd=self.paths.root)
            if result.returncode:
                raise MarketForgeError(
                    f"Managed {self.label} Engine cannot report the runtime schema. "
                    f"Run mf engine build {self.operation}."
                )
            try:
                schema = json.loads(result.stdout)
                if (
                    schema["kind"] != f"{self.label}_runtime_schema"
                    or schema["schema_version"] != 1
                ):
                    raise ValueError("unsupported protocol")
                if identity(binary.stat()) != installed:
                    raise MarketForgeError(f"Managed {self.label} Engine changed. Preview again.")
                self._schema = cast(dict[str, Any], schema)
                self._schema_engine = installed
            except (ValueError, KeyError, TypeError) as error:
                raise MarketForgeError(
                    f"Unsupported {self.label} runtime schema response. Run mf engine build {self.operation}."
                ) from error
        return self._schema

    def field(self, path: str) -> dict[str, Any]:
        for field in self.schema()["fields"]:
            if field["path"] == path:
                return cast(dict[str, Any], field)
        raise MarketForgeError(f"Unsupported {self.label} runtime field: {path}")

    def snapshot(self) -> Snapshot:
        path = self.paths.runtime_config(self.operation)
        return read_snapshot(path)

    def validate(self, data: bytes, binary: Path | None = None) -> dict[str, Any]:
        if self.operation == "convert" and binary is None:
            self.schema()
        # Use the same file name and project root as an ordinary Engine invocation.
        with TemporaryDirectory(prefix=f"marketforge-{self.operation}-config-") as temporary:
            candidate = Path(temporary) / self.path.name
            candidate.write_bytes(data)
            try:
                return inspect_runtime(self.paths, self.operation, candidate, binary)
            except MarketForgeError as error:
                message = (
                    str(error)
                    .removeprefix(f"{self.operation} runtime {candidate}: ")
                    .removeprefix("Validation failed: ")
                    .removeprefix("Merge argument error: ")
                    .removeprefix(f"invalid configuration: historical configuration {candidate}: ")
                    .removeprefix("invalid configuration: ")
                )
                raise MarketForgeError(message.replace(str(candidate), str(self.path))) from error

    def describe(
        self,
        field: dict[str, Any],
        configured: Mapping[str, Any],
        effective: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        path = field["path"]
        raw, source = configured_at(configured, path)
        if effective is None:
            return {
                **field,
                "configured": native(raw),
                "configured_path": source,
                "effective": None,
                "effective_known": False,
                "active": None,
                "inactive_reasons": [],
            }
        inactive: list[str] = []
        for dependency in field["dependencies"]:
            current = value_at(effective, dependency["path"])
            values = dependency.get("values")
            if values is not None:
                comparable = str(current).lower() if type(current) is bool else current
                if comparable not in values:
                    inactive.append(f"{dependency['path']} must be {' or '.join(values)}")
            elif current is MISSING or current is None:
                inactive.append(f"{dependency['path']} is absent")
        value = (
            configured.get("schema_version")
            if path == "schema_version"
            else native(value_at(effective, path))
        )
        return {
            **field,
            "configured": native(raw),
            "configured_path": source,
            "effective": value,
            "effective_known": True,
            "active": value is not None and not inactive,
            "inactive_reasons": inactive,
        }

    def change_field(
        self,
        original: Snapshot,
        path: str,
        raw: str | None,
    ) -> Change:
        return self.propose(original, self.patch(original.data, path, raw))

    def patch(self, data: bytes, path: str, raw: str | None, *, stage: str | None = None) -> bytes:
        """Update one draft field without validating an intermediate configuration."""
        field = self.field(path)
        if not field["mutable"]:
            raise MarketForgeError(f"{path}: runtime schema identity is immutable.")
        if raw is None and field["required"] and stage is None:
            raise MarketForgeError(f"{path}: a required field cannot be unset.")
        configured = parse_document(data)
        current, source = configured_at(configured.get(stage, {}) if stage else configured, path)
        if raw is None and current is MISSING:
            return data
        try:
            document = tomlkit.parse(data.decode("utf-8"))
        except ParseError as error:
            raise MarketForgeError(f"Cannot round-trip runtime TOML: {error}") from error
        node: Container | Table | InlineTable | OutOfOrderTableProxy = document
        parts = path.split(".")
        if stage:
            parts.insert(0, stage)
        parents: list[tuple[Container | Table | InlineTable | OutOfOrderTableProxy, str]] = []
        for part in parts[:-1]:
            if part not in node:
                node[part] = configuration_item({}, inline=isinstance(node, InlineTable))
            member = node[part]
            if not isinstance(member, (Table, InlineTable, OutOfOrderTableProxy)):
                raise MarketForgeError(f"{path}: expected a TOML table at {part}.")
            parents.append((node, part))
            node = member
        key = parts[-1]
        old_key = source.rsplit(".", 1)[-1] if source else key
        if raw is None:
            del node[old_key]
            if stage:
                if not node:
                    for parent, part in reversed(parents):
                        child = parent[part]
                        if (
                            not isinstance(child, (Table, InlineTable))
                            or child
                            or "#" in child.as_string()
                        ):
                            break
                        del parent[part]
            elif field.get("omit_empty_parent") and not node:
                del document[parts[0]]
        else:
            value = typed_value(field, raw)
            if old_key == key and type(value) is type(current) and value == current:
                return data
            previous: object = node[old_key] if old_key in node else None
            if old_key != key:
                # A legacy string profile spelling must not coexist with enabled.
                del node[old_key]
            if isinstance(value, str) and isinstance(previous, String):
                quote = previous.as_string()[:3]
                literal = (
                    quote.startswith("'")
                    and "'" not in value
                    and "\n" not in value
                    and "\r" not in value
                )
                node[key] = tomlkit.string(
                    value, literal=literal, multiline=quote in {"'''", '"""'}
                )
            else:
                node[key] = configuration_item(
                    value, tuple(parts), inline=isinstance(node, InlineTable)
                )
        return document.as_string().encode("utf-8")

    def propose(self, original: Snapshot, candidate: bytes) -> Change:
        schema = self.schema()
        schema_engine = self._schema_engine
        before = parse_document(original.data)
        effective = self.validate(candidate)
        after = parse_document(candidate)
        if type(before.get("schema_version")) is not int or (
            type(after.get("schema_version")) is not int
            or before["schema_version"] != after["schema_version"]
        ):
            raise MarketForgeError("Runtime schema identity is immutable.")
        try:
            current = effective if candidate == original.data else self.validate(original.data)
        except MarketForgeError:
            current = None  # A candidate can repair settings rejected by the Engine.
        changes: list[dict[str, Any]] = []
        for field in schema["fields"]:
            # Include indirect changes so switching a policy shows what becomes effective.
            old = self.describe(field, before, current)
            new = self.describe(field, after, effective)
            configured_changed = any(
                old[key] != new[key] for key in ("configured", "configured_path")
            )
            effective_changed = current is not None and any(
                old[key] != new[key] for key in ("effective", "active")
            )
            if configured_changed or effective_changed:
                changes.append(
                    {
                        "path": field["path"],
                        "risk": field["risk"],
                        "purpose": field["purpose"],
                        "kind": field["kind"],
                        "default": field["default"],
                        "configured_changed": configured_changed,
                        "before": {
                            key: old[key]
                            for key in (
                                "configured",
                                "configured_path",
                                "effective",
                                "effective_known",
                                "active",
                            )
                        },
                        "after": {
                            key: new[key]
                            for key in (
                                "configured",
                                "configured_path",
                                "effective",
                                "effective_known",
                                "active",
                            )
                        },
                    }
                )
        if identity(resolve(self.paths, self.operation).stat()) != schema_engine:
            raise MarketForgeError(f"Managed {self.label} Engine changed. Preview again.")
        return Change(
            original,
            candidate,
            tuple(changes),
            schema["runtime_schema_version"],
            any(field["risk"] == "integrity-critical" for field in changes),
            schema_engine,
        )

    def apply(self, change: Change, *, acknowledge_risk: bool = False) -> None:
        if change.requires_acknowledgement and not acknowledge_risk:
            raise MarketForgeError(
                "Integrity-critical changes require --acknowledge-risk with --apply."
            )
        if change.candidate == change.original.data:
            return
        with (
            WorkspaceOwnership(self.paths),
            acquire_file_lock(self.paths.root / ".marketforge-engine.lock"),
        ):
            binary = resolve(self.paths, self.operation)
            if identity(binary.stat()) != change.engine_identity:
                raise MarketForgeError(f"Managed {self.label} Engine changed. Preview again.")
            # Verify the exact candidate again against the currently installed Engine.
            self.validate(change.candidate, binary)
            replace_entry(self.path, change.original, change.candidate)
