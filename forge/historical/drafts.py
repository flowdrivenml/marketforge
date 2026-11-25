"""Create editable declarations without inferring source identity."""

from pathlib import Path

from forge.shared.config.toml import dumps
from forge.shared.errors import MarketForgeError


def write_new(path: Path, data: str | bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(data if isinstance(data, bytes) else data.encode())
    return path


def create_import(path: Path, name: str, source: Path) -> Path:
    if not source.exists():
        raise MarketForgeError("External input does not exist.")
    config = {
        "version": 1,
        "source_key": name,
        "source_kind": "recording",
        "root": str(source if source.is_dir() else source.parent),
        "files": [source.name] if source.is_file() else [],
        "patterns": ["*"] if source.is_dir() else [],
        "max_files": 1000,
        "data_type": "trades",
        "identity": {"market_category": "spot", "instrument_type": "spot", "symbol": "UNSPECIFIED"},
    }
    return write_new(
        path,
        "# Supply explicit identity/specifications before validating. No venue or units were inferred.\n"
        + dumps(config),
    )


def create_registration(path: Path, name: str, source: Path) -> Path:
    if source.suffix == ".toml":
        return write_new(path, source.read_bytes())
    root = source if source.is_dir() else source.parent
    return write_new(
        path,
        dumps(
            {
                "registration": {"version": 1, "name": name},
                "source": {"key": name, "root": str(root)},
            }
        )
        + "# REQUIRED: provider, product, provenance, coverage, and inputs referencing registered catalogue metadata.\n",
    )
