"""Installer tests with mocked native tools and opt-in isolated PostgreSQL."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import psycopg
import pytest

from forge.shared.database.credentials import PRODUCTION_DATABASE_KEY, TEST_DATABASE_KEY
from forge.shared.database.migrations import migrate_database
from forge.shared.database.status import inspect_database
from forge.shared.errors import MarketForgeError
from scripts.install import provision, setup

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "scripts/install"


def bash(source: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", "set -Eeuo pipefail\n" + source, "fixture", *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )


def executable(path: Path, body: str) -> None:
    path.write_text("#!/bin/bash\n" + body + "\n")
    path.chmod(0o755)


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "checkout with spaces"
    (root / "forge").mkdir(parents=True)
    (root / "forge/pyproject.toml").touch()
    (root / "engine").mkdir()
    for key in (PRODUCTION_DATABASE_KEY, TEST_DATABASE_KEY):
        monkeypatch.delenv(key, raising=False)
    return root


@pytest.mark.parametrize(
    ("arguments", "expected", "code"),
    [
        (["--dry-run"], "Registry: production", 0),
        (
            ["--dev", "--dry-run", "--skip-postgres", "--skip-market-init"],
            "Registry: testing",
            0,
        ),
        (["--help"], "Usage:", 0),
        (["--unknown"], "Unknown option", 1),
    ],
)
def test_arguments_and_nonmutating_commands(
    workspace: Path, arguments: list[str], expected: str, code: int
) -> None:
    shutil.copy(ROOT / "install.sh", workspace)
    shutil.copytree(SCRIPTS, workspace / "scripts/install")
    before = sorted(workspace.rglob("*"))
    result = subprocess.run(
        [str(workspace / "install.sh"), *arguments],
        cwd=workspace.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == code
    assert expected in result.stdout + result.stderr
    assert before == sorted(workspace.rglob("*"))
    if "--skip-postgres" in arguments:
        assert "skip PostgreSQL packages and provisioning" in result.stdout
        assert "skip market initialization" in result.stdout


@pytest.mark.parametrize(
    ("os_name", "architecture", "message", "code"),
    [
        ("Linux", "aarch64", "APT", 0),
        ("Darwin", "arm64", "Homebrew", 0),
        ("Darwin", "x86_64", "Homebrew", 0),
        ("FreeBSD", "x86_64", "Unsupported platform", 1),
        ("Darwin", "powerpc", "Unsupported CPU", 1),
    ],
)
def test_platform_selection(
    tmp_path: Path, os_name: str, architecture: str, message: str, code: int
) -> None:
    executable(
        tmp_path / "uname",
        f'if [[ "$1" == -s ]]; then echo {os_name}; else echo {architecture}; fi',
    )
    result = bash(
        'PATH="$1:$PATH" "$2" --dry-run', str(tmp_path), str(ROOT / "install.sh")
    )
    assert result.returncode == code
    assert message in result.stdout + result.stderr


def test_linux_refuses_unsupported_distribution_or_missing_apt(tmp_path: Path) -> None:
    release = tmp_path / "os-release"
    release.write_text("ID=arch\n")
    source = 'source "$1/common.sh"; source "$1/linux.sh"; have() { return 1; }; detect_linux "$2"'
    result = bash(source, str(SCRIPTS), str(release))
    assert "unsupported" in result.stderr
    release.write_text('ID="debian"\n')
    result = bash(source, str(SCRIPTS), str(release))
    assert "Cannot find apt-get" in result.stderr


@pytest.mark.parametrize(
    ("platform", "manager"), [("linux", "apt-get"), ("macos", "brew")]
)
@pytest.mark.parametrize("installed", [True, False])
def test_native_dependencies_preserve_neovim(
    platform: str, manager: str, installed: bool
) -> None:
    result = bash(
        """
        source "$1/common.sh"; source "$1/$2.sh"
        mode=dev
        installed=$3
        have() { [[ "$1" != nvim || "$installed" == true ]]; }
        xcode-select() { :; }
        sudo() { printf '%s\\n' "$*"; }
        brew() { printf 'brew %s\\n' "$*"; }
        native_dependencies
        """,
        str(SCRIPTS),
        platform,
        str(installed).lower(),
    )
    assert result.returncode == 0
    assert ("install" in result.stdout) is not installed
    if not installed:
        assert manager in result.stdout and "neovim" in result.stdout


@pytest.mark.parametrize("installed", [False, True])
def test_tool_bootstrap_only_when_missing(tmp_path: Path, installed: bool) -> None:
    for tool, version in (
        ("cargo", "cargo 1.94.0"),
        ("rustc", "rustc 1.94.0"),
        ("uv", "uv 0.13.0"),
    ):
        executable(tmp_path / tool, f'echo "{version} (fixture)"')
    result = bash(
        """
        source "$1/common.sh"
        export PATH="$2:$PATH"
        mode=runtime
        installed=$3
        have() {
            [[ "$installed" == true || "$1" == "$available_uv" || "$available_rust" == true ]]
        }
        available_uv=
        available_rust=false
        bootstrap() {
            printf 'bootstrapped %s\\n' "$1"
            if [[ "$1" == *uv* ]]; then available_uv=uv; else available_rust=true; fi
        }
        ensure_uv
        ensure_rust
        """,
        str(SCRIPTS),
        str(tmp_path),
        str(installed).lower(),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.count("bootstrapped") == (0 if installed else 2)


@pytest.mark.parametrize("python_found", [True, False])
def test_python_resolution_and_locked_sync(tmp_path: Path, python_found: bool) -> None:
    executable(tmp_path / "python", "exit 0")
    executable(tmp_path / "mf", 'echo "marketforge 0.1.0"')
    result = bash(
        """
        source "$1/common.sh"
        fixture=$2; root="$fixture/workspace with spaces"; mode=runtime
        found=$3
        uv() {
            case "$*" in
                "python find >=3.12") if [[ $found == true || -f "$fixture/installed" ]]; then echo "$fixture/python"; else return 1; fi ;;
                "python install 3.12") touch "$fixture/installed" ;;
                sync*) printf '%s\\n' "$*"; mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"; cp "$fixture/mf" "$UV_PROJECT_ENVIRONMENT/bin/mf" ;;
            esac
        }
        install_forge
        """,
        str(SCRIPTS),
        str(tmp_path),
        str(python_found).lower(),
    )
    assert result.returncode == 0, result.stderr
    assert "--locked" in result.stdout
    assert "--no-default-groups" in result.stdout
    assert (tmp_path / "installed").exists() is not python_found


@pytest.mark.parametrize(
    ("code", "skip", "expected"),
    [(0, True, 0), (4, True, 1), (4, False, 0), (1, False, 1)],
)
def test_postgres_stage_stops_before_dependent_actions(
    code: int, skip: bool, expected: int
) -> None:
    result = bash(
        """
        root=$1; source "$root/common.sh"; source "$root/database.sh"
        environment=testing; install_python=database_helper; mf_path=mf; skip_postgres=$3
        state=$2
        database_helper() {
            case "$2" in
                check) return "$state" ;;
                provision) state=0; echo provision ;;
                migrate) echo "database migrate --testing" >&2; echo fixture ;;
            esac
        }
        local_postgres() { echo local; pg_admin=(psql); pg_socket=/tmp; pg_port=5432; }
        mf() { echo "$*"; }
        ensure_database
        """,
        str(SCRIPTS),
        str(code),
        str(skip).lower(),
    )
    assert result.returncode == expected
    assert ("database migrate --testing" in result.stderr) == (expected == 0)
    assert ("provision\n" in result.stdout) == (code == 4 and not skip)


def test_workspace_and_launcher_reuse(
    workspace: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shutil, "which", lambda _: "/fixture/editor")
    setup.workspace(workspace)
    config = workspace / "data/.config/forge/marketforge.toml"
    assert 'editor = "nvim"' in config.read_text()
    config.write_text('[presentation]\neditor = "custom-editor"\n')
    raw = workspace / "data/raw/user-record"
    raw.write_bytes(b"preserve")
    setup.workspace(workspace)
    assert 'editor = "custom-editor"' in config.read_text()
    assert raw.read_bytes() == b"preserve"
    profile = tmp_path / "profile"
    monkeypatch.setattr(Path, "home", classmethod(lambda _: profile))
    setup.launchers(workspace, "runtime")
    setup.launchers(workspace, "dev")
    launcher = profile / ".local/bin/mf"
    assert launcher.readlink() == workspace / ".install/runtime/bin/mf"
    launcher.unlink()
    launcher.write_text("unrelated")
    with pytest.raises(MarketForgeError):
        setup.launchers(workspace, "dev")
    assert launcher.read_text() == "unrelated"


def test_bad_names_and_missing_credentials(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(provision.MissingDatabaseCredentials):
        provision.preflight(workspace, True)
    monkeypatch.setenv(TEST_DATABASE_KEY, "dbname=marketforge")
    with pytest.raises(
        MarketForgeError, match="must explicitly select marketforge_test"
    ):
        provision.preflight(workspace, True)


def test_existing_unconfigured_local_conflict_preserves_data(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def admin(_: list[str], socket: str, port: str, statement: str) -> str:
        if statement.startswith("SHOW"):
            return "180000"
        assert "SELECT pg_get_userbyid" in statement
        return "existing_owner"

    monkeypatch.setattr(provision, "admin_query", admin)
    with pytest.raises(MarketForgeError, match="already exists"):
        provision.provision(workspace, True, "/tmp", "5432", ["psql"])
    assert not (workspace / ".secrets/postgres.env").exists()


def test_real_postgres_isolated(
    workspace: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    binaries = os.getenv("MARKETFORGE_INSTALL_TEST_PG_BIN")
    if not binaries or os.getuid() == 0:
        pytest.skip("Set MARKETFORGE_INSTALL_TEST_PG_BIN and run as a non-root user.")
    bindir = Path(binaries)
    # Keep socket paths short and let PostgreSQL enforce authentication over TCP.
    from socket import socket
    from tempfile import TemporaryDirectory

    with socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = str(sock.getsockname()[1])
    owner = subprocess.check_output(["id", "-un"], text=True).strip()
    with TemporaryDirectory(prefix="mf-pg-") as short:
        data = tmp_path / "pg"

        def pg(tool: str) -> str:
            return str(bindir / tool)

        env = {
            key: value for key, value in os.environ.items() if not key.startswith("PG")
        }
        subprocess.run(
            [
                pg("initdb"),
                "-D",
                str(data),
                "--auth-local=trust",
                "--auth-host=scram-sha-256",
                "-U",
                owner,
            ],
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                pg("pg_ctl"),
                "-D",
                str(data),
                "-l",
                str(tmp_path / "pg.log"),
                "-o",
                f"-k {short} -p {port} -c listen_addresses=127.0.0.1",
                "start",
            ],
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        try:
            dotfile = workspace / ".secrets/postgres.env"
            dotfile.parent.mkdir()
            dotfile.write_text(
                "UNRELATED='literal extension setting'"
            )  # No trailing newline.
            admin = [pg("psql"), "-U", owner]
            for testing in (True, False):
                provision.provision(workspace, testing, short, port, admin)
                provision.preflight(workspace, testing)
                assert migrate_database(testing=testing, project_root=workspace).applied
                first = inspect_database(testing=testing, project_root=workspace)
                assert first.purpose == ("testing" if testing else "production")
                assert first.database_id
                assert not migrate_database(
                    testing=testing, project_root=workspace
                ).applied
                provision.preflight(workspace, testing, ready=True)
                original = dotfile.read_bytes()
                provision.provision(workspace, testing, short, port, admin)
                assert original == dotfile.read_bytes()
                assert (
                    inspect_database(
                        testing=testing, project_root=workspace
                    ).database_id
                    == first.database_id
                )
            assert dotfile.read_text().startswith(
                "UNRELATED='literal extension setting'\n"
            )
            assert dotfile.stat().st_mode & 0o777 == 0o600
            assert "postgresql://" not in capsys.readouterr().out
            dsn = provision.resolve_database_dsn(project_root=workspace, testing=True)
            with psycopg.connect(dsn) as connection:
                connection.execute(
                    "UPDATE forge_meta.database_identity SET purpose = 'production'"
                )
            with pytest.raises(MarketForgeError, match="purpose testing"):
                provision.preflight(workspace, True)

            # This dedicated database has an existing object but no Forge identity.
            provision.admin_query(
                admin, short, port, "CREATE DATABASE installer_conflict;"
            )
            provision.admin_query(
                admin,
                short,
                port,
                r"\connect installer_conflict" + "\nCREATE SCHEMA catalog;",
            )
            key = PRODUCTION_DATABASE_KEY
            previous = os.environ.get(key)
            try:
                os.environ[key] = psycopg.conninfo.make_conninfo(
                    host=short, port=port, user=owner, dbname="installer_conflict"
                )
                with pytest.raises(MarketForgeError, match="existing objects"):
                    provision.preflight(workspace, False)
            finally:
                if previous is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = previous
        finally:
            subprocess.run(
                [pg("pg_ctl"), "-D", str(data), "-m", "fast", "stop"],
                env=env,
                check=True,
                stdout=subprocess.DEVNULL,
            )


@pytest.mark.parametrize("fresh", [True, False])
def test_apt_postgres_cluster_handling(tmp_path: Path, fresh: bool) -> None:
    executable(tmp_path / "psql", "exit 0")
    result = bash(
        """
        source "$1/common.sh"; source "$1/linux.sh"
        PATH="$2:$PATH"; state=$3
        pg_lsclusters() {
            case "$state" in
                true) return 0 ;;
                new) echo "18 main 5432 down postgres ignored" ;;
                *) echo "18 main 5432 online postgres ignored" ;;
            esac
        }
        apt_install() { echo "install $*"; state=new; }
        pg_ctlcluster() { :; }
        pg_isready() { return 1; }
        sudo() { echo "start $*"; state=online; }
        local_postgres
        echo "port $pg_port"
        """,
        str(SCRIPTS),
        str(tmp_path),
        str(fresh).lower(),
    )
    assert result.returncode == 0, result.stderr
    assert ("install postgresql" in result.stdout) == fresh
    assert ("start pg_ctlcluster" in result.stdout) == fresh
    assert "port 5432" in result.stdout


@pytest.mark.parametrize("ready", [True, False])
def test_macos_existing_postgres_preserved(tmp_path: Path, ready: bool) -> None:
    (tmp_path / "bin").mkdir()
    executable(tmp_path / "bin/pg_isready", "exit 0" if ready else "exit 1")
    result = bash(
        """
        source "$1/common.sh"; source "$1/macos.sh"
        fixture=$2
        brew() {
            case "$*" in
                "list --formula") echo postgresql@17 ;;
                "--prefix postgresql@17") echo "$fixture" ;;
                *) echo "unexpected brew operation"; return 1 ;;
            esac
        }
        local_postgres
        echo "port $pg_port"
        """,
        str(SCRIPTS),
        str(tmp_path),
    )
    if ready:
        assert result.returncode == 0
        assert "port 5432" in result.stdout
    else:
        assert result.returncode == 1
        assert "Existing Homebrew PostgreSQL is unavailable" in result.stderr
    assert "unexpected brew" not in result.stdout


@pytest.mark.parametrize(
    ("state", "cli_status", "exit_status", "recorded"),
    [("complete", 0, 0, True), ("partial", 1, 3, False), ("failed", 1, 3, False)],
)
def test_market_initialization_results(
    state: str, cli_status: int, exit_status: int, recorded: bool
) -> None:
    result = bash(
        """
        source "$1/common.sh"
        root=$1; mode=runtime; environment=production
        install_python=fixture; mf_path=mf; registry_identity=fixture
        state=$2; cli_status=$3
        fixture() {
            case "$2" in
                already-initialized) return 1 ;;
                initialize) echo "$state"; return "$cli_status" ;;
                initialization) cat ;;
                record-initialization) echo "recorded" ;;
            esac
        }
        market_init
        """,
        str(SCRIPTS),
        state,
        str(cli_status),
    )
    assert result.returncode == exit_status
    assert ("recorded" in result.stdout) == recorded
    if exit_status:
        assert "Local installation and migrations completed" in result.stderr


def test_market_initialization_record_is_scoped(workspace: Path) -> None:
    (workspace / ".install").mkdir()
    record = setup.initialization_record
    assert not record(workspace, "production", "registry:16", write=False)
    assert record(workspace, "testing", "registry:16", write=True)
    assert record(workspace, "testing", "registry:16", write=False)
    assert not record(workspace, "testing", "registry:17", write=False)
    assert not record(workspace, "production", "registry:16", write=False)
