"""Explicit Forge actions and form fields shared by workspace screens."""

from dataclasses import dataclass
from shlex import join
from typing import Literal

from forge.shared.errors import MarketForgeError

Value = str | int | float | bool
Values = dict[str, Value]


@dataclass(frozen=True, slots=True)
class Field:
    key: str
    label: str
    kind: Literal["text", "integer", "number", "choice", "check"] = "text"
    choices: tuple[str, ...] = ()
    default: Value = ""
    required: bool = False
    option: str = ""

    def parse(self, value: Value) -> Value:
        if value == "":
            if self.required:
                raise MarketForgeError(f"{self.label} is required.")
            return value
        if self.kind in {"integer", "number"}:
            try:
                return int(str(value)) if self.kind == "integer" else float(str(value))
            except ValueError:
                raise MarketForgeError(f"{self.label} requires a number.") from None
        if self.kind == "choice" and value not in self.choices:
            raise MarketForgeError(f"Choose a supported {self.label.lower()}.")
        return value


@dataclass(frozen=True, slots=True)
class Action:
    key: str
    area: str
    fields: tuple[Field, ...] = ()
    effect: Literal["read", "write", "network", "engine", "editor", "guarded"] = "read"
    testing: bool = False

    def command(self, values: Values, *, testing: bool = False) -> str:
        cmd = ["mf", *self.key.split()]
        for field in self.fields:
            value = values.get(field.key, field.default)
            if value == "" or value is False:
                continue
            if field.option:
                if field.key == "header":
                    cmd.append("--header" if value == "present" else "--no-header")
                    continue
                if field.key in {"exchanges", "markets"}:
                    for item in str(value).split(","):
                        cmd.extend([field.option, item.strip()])
                    continue
                if field.kind == "check":
                    cmd.append(field.option)
                    continue
                if field.key == "source" and self.key == "convert new":
                    cmd.extend([f"--from-{values.get('route', 'merge')}", str(value)])
                    continue
                if field.key == "column":
                    for column in str(value).split(","):
                        cmd.extend([field.option, column.strip()])
                    continue
                cmd.append(field.option)
            if field.key != "route":
                cmd.append(str(value))
        if self.testing or self.key in {"database backup", "database restore"}:
            cmd.append("--testing" if testing else "--production")
        if self.key == "acquisition plan":
            cmd.append("--cache-only")
        if self.key == "datasets migrate-registration":
            cmd.append("--all")
        if self.key == "database restore":
            cmd.append("--disposable-testing")
        return join(cmd)


def flag(key: str, label: str, *, option: str = "") -> Field:
    return Field(key, label, "check", default=False, option=option or "--" + key.replace("_", "-"))


def opt(key: str, label: str, *, choices: tuple[str, ...] = (), option: str = "") -> Field:
    return Field(
        key,
        label,
        "choice" if choices else "text",
        choices,
        option=option or "--" + key.replace("_", "-"),
    )


def integer(key: str, label: str, default: int | str = "", *, position: bool = False) -> Field:
    return Field(
        key,
        label,
        "integer",
        default=default,
        required=position,
        option="" if position else "--" + key,
    )


REF = Field("reference", "Recipe name or supported path", required=True)
NAME = Field("reference", "Recipe name", required=True)
RUN = Field("reference", "Run ID", required=True)
ID = integer("id", "Registry primary-file dataset ID", position=True)
ARCHIVE = Field("archive", "Local archive ID", required=True)
ARCHIVE_OPT = opt("archive", "Local archive ID")
SRC = Field("source", "Source path", required=True, option="--from")
LIMIT = integer("limit", "Result limit", 50)
OFFSET = integer("offset", "Result offset", 0)
DRY = flag("dry_run", "Preview without writing")
WINDOW = (opt("start", "Start UTC"), opt("end", "Exclusive end UTC"))
DATA = (
    opt("operation", "Operation", choices=("normalize", "merge")),
    opt("data_type", "Data type", choices=("trade", "l2", "trade_l2")),
    opt("publication", "Engine publication ID"),
    opt("exchange", "Exchange"),
    opt("instrument", "Symbol or instrument ID"),
    opt("instrument_type", "Instrument type"),
    opt("market_category", "Market category"),
    opt("status", "Status"),
    *WINDOW,
    LIMIT,
    OFFSET,
)
RAW = (
    opt("origin", "Origin", choices=("remote", "external")),
    opt("exchange", "Exchange"),
    opt("market", "Market category"),
    opt("instrument_type", "Instrument type"),
    opt("instrument", "Source instrument"),
    opt("data_type", "Data type"),
    opt("scope_kind", "Scope", choices=("instrument", "base_coin", "family")),
    opt("feed", "Feed", choices=("incremental", "snapshot")),
    opt("raw_source_key", "Registered raw source key"),
    opt("base_asset", "Base asset"),
    opt("quote_asset", "Quote asset"),
    opt("match", "Symbol pattern"),
    *WINDOW,
    LIMIT,
    OFFSET,
)
MARKET = (
    opt("exchanges", "Exchanges, comma separated", option="--exchange"),
    opt("markets", "Market categories, comma separated", option="--market"),
    opt("instrument", "Symbol"),
)
ASSETS = (
    opt("base_asset", "Base asset"),
    opt("quote_asset", "Quote asset"),
    opt("match", "Symbol pattern"),
)
RANK = (opt("instrument_type", "Instrument type", choices=("spot", "perpetual", "future")), *ASSETS)
ALL = flag("all", "All snapshots")
APPLY = flag("apply", "Apply reviewed selection")
ACT = Action

ACTIONS = (
    ACT("pipeline new", "Workflows", (NAME,), "write"),
    ACT("pipeline validate", "Workflows", (NAME,)),
    ACT("pipeline plan", "Workflows", (NAME,), testing=True),
    ACT(
        "pipeline run",
        "Workflows",
        (NAME, opt("stop_after", "Stop after", choices=("normalize.prepare",))),
        "engine",
        True,
    ),
    ACT("pipeline resume", "Runs", (RUN,), "engine", True),
    ACT("pipeline status", "Runs", (RUN,)),
    ACT("acquisition list", "Recipes & Profiles"),
    ACT("acquisition new", "Workflows", (NAME,), "editor"),
    ACT("acquisition edit", "Recipes & Profiles", (NAME,), "editor"),
    ACT("acquisition validate", "Workflows", (NAME,)),
    ACT(
        "acquisition plan",
        "Workflows",
        (NAME, flag("verify_local", "Verify sample on disk"), LIMIT),
        testing=True,
    ),
    *(
        ACT(
            f"acquisition {x}",
            "Workflows",
            (
                NAME,
                flag("refresh", "Refresh discovery"),
                Field("rate", "Requests per second", "number", option="--requests-per-second"),
            ),
            "network",
            True,
        )
        for x in ("map", "download")
    ),
    *(
        ACT(
            f"acquisition import {x}",
            "Workflows",
            (NAME, SRC) if x == "new" else (NAME,),
            "write" if x in {"run", "new"} else "read",
            x == "run",
        )
        for x in ("new", "inspect", "validate", "plan", "run")
    ),
    *(
        ACT(
            f"acquisition register {x}",
            "Workflows",
            (NAME, SRC) if x == "new" else (NAME,),
            "editor" if x == "edit" else "write" if x in {"new", "run", "plan"} else "read",
            x in {"validate", "plan", "run"},
        )
        for x in ("new", "edit", "inspect", "validate", "plan", "run")
    ),
    ACT(
        "normalize profiles list",
        "Recipes & Profiles",
        tuple(
            opt(k, k.replace("_", " ").title())
            for k in ("type", "exchange", "instrument_type", "market_category")
        ),
    ),
    ACT(
        "normalize profiles sample",
        "Recipes & Profiles",
        (
            Field("file", "Raw file", required=True, option="--file"),
            opt("member", "Archive member"),
            opt("header", "CSV header", choices=("present", "absent")),
        ),
    ),
    ACT(
        "normalize profiles inspect",
        "Recipes & Profiles",
        (Field("reference", "Profile ID", required=True),),
    ),
    ACT(
        "normalize profiles validate",
        "Recipes & Profiles",
        (Field("reference", "Installed profile ID"), opt("file", "Profile TOML")),
    ),
    *(
        ACT(
            f"normalize profiles {x}",
            "Recipes & Profiles",
            (Field("reference", "Profile ID", required=True), SRC),
            "write",
            True,
        )
        for x in ("add", "register")
    ),
    ACT(
        "normalize new",
        "Workflows",
        (NAME, opt("from_acquisition", "Acquisition recipe")),
        "editor",
    ),
    ACT("normalize validate", "Workflows", (REF,)),
    ACT(
        "normalize prepare",
        "Workflows",
        (REF, opt("from_acquisition", "Acquisition recipe"), DRY),
        "write",
        True,
    ),
    *(
        ACT(
            f"normalize {x}",
            "Workflows",
            (
                REF,
                flag("allow_partial", "Allow partial registration"),
                *((DRY,) if x == "execute" else ()),
            ),
            "engine",
            True,
        )
        for x in ("execute", "run")
    ),
    ACT(
        "normalize register",
        "Runs",
        (Field("reference", "Publication or execution intent path", required=True),),
        "write",
        True,
    ),
    ACT("merge new", "Workflows", (NAME, opt("from_normalize", "Normalization recipe")), "write"),
    ACT("merge validate", "Workflows", (REF,)),
    ACT("merge prepare", "Workflows", (REF,), "write", True),
    ACT("merge run", "Workflows", (REF,), "engine", True),
    ACT(
        "merge register",
        "Runs",
        (Field("reference", "Publication or execution intent path", required=True),),
        "write",
        True,
    ),
    ACT(
        "convert new",
        "Workflows",
        (
            NAME,
            Field(
                "route", "Source route", "choice", ("merge", "normalize", "acquisition"), "merge"
            ),
            SRC,
            Field("quote", "Target quote", required=True, option="--target-quote"),
        ),
        "write",
    ),
    ACT("convert list", "Workflows"),
    ACT("convert show", "Workflows", (REF,)),
    ACT("convert config show", "Workflows"),
    ACT("convert config get", "Workflows", (Field("field", "Runtime field", required=True),)),
    ACT("convert config schema", "Workflows", (opt("field", "Runtime field"),)),
    ACT("convert config validate", "Workflows"),
    ACT("convert validate", "Workflows", (REF,)),
    ACT("convert prepare", "Workflows", (REF, DRY), "write", True),
    ACT("convert run", "Workflows", (REF,), "engine", True),
    *(ACT(f"datasets {x}", "Data", DATA, testing=True) for x in ("list", "find", "coverage")),
    *(
        ACT(f"datasets {x}", "Data", (ID,), testing=True)
        for x in ("show", "streams", "verify", "schema")
    ),
    ACT(
        "datasets lineage",
        "Data",
        (
            ID,
            flag("publication", "Publication selection"),
            flag("checksums", "Include checksums"),
            LIMIT,
        ),
        testing=True,
    ),
    ACT(
        "datasets head",
        "Data",
        (
            ID,
            LIMIT,
            opt("column", "Columns, comma separated"),
            flag("wide", "All columns"),
            flag("precise", "Exact timestamps"),
        ),
        testing=True,
    ),
    ACT("datasets refresh-summary", "Data", (ID,), "write", True),
    ACT("datasets migrate-registration", "Data", (DRY,), "write", True),
    ACT("storage list", "Data", (*RAW, opt("status", "Archive status"), ARCHIVE_OPT), testing=True),
    ACT("storage coverage", "Data", (*RAW, flag("gaps", "Include gaps")), testing=True),
    ACT("storage show", "Data", (ARCHIVE,), testing=True),
    *(
        ACT(
            f"storage {x}",
            "Data",
            (ARCHIVE_OPT, LIMIT, opt("status", "Archive status"), RAW[0]),
            "write",
            True,
        )
        for x in ("verify", "reconcile")
    ),
    *(
        ACT(
            f"storage {x}",
            "Data",
            (
                *RAW,
                opt("status", "Archive status"),
                ARCHIVE_OPT,
                flag("overlap", "Match overlapping dates"),
            ),
            "guarded",
            True,
        )
        for x in ("prune", "delete")
    ),
    *(ACT(f"markets {x}", "Markets", MARKET, "network", True) for x in ("sync", "refresh")),
    ACT(
        "markets list",
        "Markets",
        (
            *MARKET,
            *ASSETS,
            opt("sort", "Order", choices=("identity", "turnover", "freshness")),
            LIMIT,
        ),
        testing=True,
    ),
    ACT(
        "markets show",
        "Markets",
        (Field("reference", "Full instrument reference", required=True), *MARKET),
        testing=True,
    ),
    *(
        ACT(
            f"markets rank {x}",
            "Markets",
            (NAME,),
            "write" if x != "validate" else "read",
            x == "build",
        )
        for x in ("new", "validate", "build")
    ),
    ACT("markets rank list", "Markets", (LIMIT,), testing=True),
    ACT(
        "markets rank show",
        "Markets",
        (
            Field("reference", "Policy or snapshot ID", required=True),
            *RANK,
            opt("conversion", "Conversion evidence entry"),
            flag("excluded", "Review exclusions"),
            flag("all", "All entries"),
            LIMIT,
        ),
        testing=True,
    ),
    ACT(
        "markets rank update",
        "Markets",
        (Field("reference", "Policy", "choice", ("turnover", "volume")),),
        "network",
        True,
    ),
    ACT(
        "markets rank delete",
        "Markets",
        (Field("reference", "Policy or snapshot ID"), ALL),
        "guarded",
        True,
    ),
    *(
        ACT(
            f"catalog {model} {x}",
            "Catalog",
            (
                Field(
                    "source",
                    "Declaration TOML",
                    required=True,
                    option="--file" if x == "validate" else "--from",
                ),
            )
            if x in {"add", "validate"}
            else (Field("reference", "Catalogue reference", required=True),)
            if x == "show"
            else (),
            "write" if x == "add" else "read",
            x != "validate",
        )
        for model in ("exchange", "instrument", "specs")
        for x in ("add", "list", "show", "validate")
    ),
    *(
        ACT(
            f"runs {x}",
            "Runs",
            (LIMIT,) if x == "list" else (RUN, LIMIT) if x == "logs" else (RUN,),
        )
        for x in ("list", "show", "logs", "report")
    ),
    ACT("engine status", "System"),
    ACT(
        "engine runtime",
        "System",
        (
            Field(
                "reference",
                "Runtime operation",
                "choice",
                ("normalize", "merge", "convert", "pipeline"),
            ),
        ),
    ),
    ACT(
        "engine build",
        "System",
        (
            Field("reference", "Engine target", "choice", ("normalize", "merge", "convert")),
            DRY,
            flag("offline", "Use local build cache"),
            integer("jobs", "Cargo jobs"),
        ),
        "engine",
    ),
    ACT("database status", "System", testing=True),
    ACT("database migrate", "System", effect="write", testing=True),
    ACT(
        "database backup",
        "System",
        (Field("reference", "New backup directory", required=True),),
        "write",
        True,
    ),
    ACT(
        "database restore",
        "System",
        (
            Field("reference", "Verified backup directory", required=True),
            Field("destination", "Disposable database name", required=True, option="--destination"),
        ),
        "write",
        True,
    ),
    ACT("init", "System", effect="network", testing=True),
    ACT("doctor", "System", testing=True),
    ACT("config show", "System"),
)
BY_KEY = {action.key: action for action in ACTIONS}


def read_only(action: Action, values: Values) -> bool:
    return action.effect == "read" or (
        bool(values.get("dry_run")) and not values.get("from_acquisition")
    )
