# Shared helpers. Keep compatibility with macOS Bash 3.2.

stage_name="arguments"
stage() { stage_name=$1; printf '\n%s\n' "$1"; }
fail() { printf 'MarketForge installer [%s]: %s\n' "$stage_name" "$*" >&2; exit 1; }
unexpected_failure() {
    printf 'MarketForge installer [%s]: command failed (exit %s). Earlier completed stages are retained. Resolve the error above and rerun.\n' "$stage_name" "$1" >&2
    exit 1
}
have() { command -v "$1" >/dev/null 2>&1; }
need() { have "$1" || fail "Cannot find $1 on PATH. Install it or make it available and retry."; }

usage() {
    cat <<'HELP'
Usage: ./install.sh [--dev] [--dry-run] [--skip-postgres] [--skip-market-init]
  --dev               Install contributor tools and select the testing registry.
  --dry-run           Inspect executable availability and print the plan.
  --skip-postgres     Use an existing configured connection; still migrate it.
  --skip-market-init  Finish local setup without provider collection or rankings.
  --help              Show this help.
HELP
}

print_plan() {
    for executable in python3 uv rustc cargo nvim git psql; do
        if have "$executable"; then
            printf '  Found %s; will check compatibility.\n' "$executable"
        else
            printf '  %s is absent; will install it if needed.\n' "$executable"
        fi
    done
    printf '  Plan: native dependencies via %s, locked Forge %s environment, managed normalize/merge/convert.\n' "$native_manager" "$mode"
    printf '  Plan: initialize missing Live general configuration and preserve any existing file.\n'
    if [[ $mode == dev ]]; then
        printf '  Plan: dev and lab groups, Rust formatting/lint components, isolated Zensical environment.\n'
    fi
    if "$skip_postgres"; then
        printf '  Plan: verify the configured %s connection and migrate; skip PostgreSQL packages and provisioning.\n' "$environment"
    else
        printf '  Plan: reuse the configured %s connection, or provision a dedicated local role and database; migrate and verify.\n' "$environment"
    fi
    if "$skip_market_init"; then
        printf '  Plan: skip market initialization.\n'
    else
        printf '  Plan: initialize %s market metadata and rankings.\n' "$environment"
    fi
    printf 'Dry run complete. No changes made.\n'
}

bootstrap() {
    local script
    script=$(mktemp "${TMPDIR:-/tmp}/marketforge-install.XXXXXXXX")
    # Download separately so a failed transfer cannot pass a partial script to sh.
    if ! curl --proto '=https' --tlsv1.2 -fLsS "$1" -o "$script"; then
        unlink "$script"
        fail "Could not download the tool bootstrap. Check HTTPS access and retry."
    fi
    shift
    if ! "$@" "$script"; then
        unlink "$script"
        fail "Tool bootstrap failed. Resolve its diagnostics and retry."
    fi
    unlink "$script"
    hash -r
}

ensure_uv() {
    if ! have uv; then
        bootstrap https://astral.sh/uv/install.sh env UV_NO_MODIFY_PATH=1 UV_INSTALL_DIR="$HOME/.local/bin" sh
    else
        printf '  Reusing uv.\n'
    fi
    need uv
    uv --version
}

rust_bootstrap() {
    sh "$1" -y --profile minimal --default-toolchain stable --no-modify-path
}

ensure_rust() {
    if ! have rustc && ! have cargo && ! have rustup; then
        bootstrap https://sh.rustup.rs rust_bootstrap
    elif have rustup && ! rustup which rustc >/dev/null 2>&1; then
        local installed
        installed=$(rustup toolchain list --quiet)
        if [[ -z $installed || $installed == "no installed toolchains" ]]; then
            rustup toolchain install stable --profile minimal
            rustup default stable
        else
            fail "Rustup has installed toolchains but none selected here. Select a compatible toolchain and retry."
        fi
    else
        printf '  Reusing Rust toolchain.\n'
    fi
    need rustc
    need cargo
    local version major minor
    version=$(rustc --version) || fail "Rust is not usable. Select a compatible toolchain with rustup and retry."
    [[ $version =~ ^rustc\ ([0-9]+)\.([0-9]+)\. ]] || fail "Cannot determine the Rust version."
    major=${BASH_REMATCH[1]}
    minor=${BASH_REMATCH[2]}
    (( major > 1 || (major == 1 && minor >= 89) )) ||
        fail "Rust 1.89 or later is required by the locked Engine dependencies. Update the selected toolchain and retry."
    command cargo --version
    if [[ $mode == dev ]]; then
        if have rustup && rustup which rustc >/dev/null 2>&1; then
            cargo fmt --version >/dev/null 2>&1 || rustup component add rustfmt
            cargo clippy --version >/dev/null 2>&1 || rustup component add clippy
        fi
        command cargo fmt --version || fail "Install rustfmt for the active toolchain."
        command cargo clippy --version || fail "Install Clippy for the active toolchain."
    fi
}

install_forge() {
    if [[ -L "$root/.install" || ( -e "$root/.install" && ! -d "$root/.install" ) ]]; then
        fail "The checkout's .install path must be a real directory."
    fi
    install_env="$root/.install/$mode"
    mf_path="$install_env/bin/mf"
    install_python="$install_env/bin/python"
    local python
    if ! python=$(uv python find '>=3.12'); then
        uv python install 3.12
        python=$(uv python find '>=3.12')
    fi
    "$python" -c 'import sys; assert sys.version_info >= (3, 12)'
    local -a groups
    groups=(--no-default-groups)
    if [[ $mode == dev ]]; then
        groups+=(--group dev --group lab)
    fi
    if [[ -L "$install_env" || ( -e "$install_env" && ! -f "$install_env/pyvenv.cfg" ) ]]; then
        fail "The selected installer environment is not a virtual environment owned by this layout."
    fi
    UV_PROJECT_ENVIRONMENT="$install_env" uv sync --project "$root/forge" --locked --python "$python" "${groups[@]}"
    mf --version
    if [[ $mode == dev ]]; then
        local docs_env="$root/.install/docs"
        if [[ -L "$docs_env" || ( -e "$docs_env" && ! -f "$docs_env/pyvenv.cfg" ) ]]; then
            fail "The documentation environment path is occupied."
        fi
        if [[ ! -x "$docs_env/bin/python" ]]; then
            uv venv --python "$python" "$docs_env"
        fi
        uv pip install --python "$docs_env/bin/python" -r "$root/docs/requirements.txt"
        "$docs_env/bin/zensical" --version
        "$install_env/bin/jupyter" lab --version
        "$install_python" -c 'import ipykernel, polars, pyarrow, pytest, pytest_asyncio'
        "$install_env/bin/ruff" --version
        "$install_env/bin/pyright" --version
    fi
}

mf() { "$mf_path" --project-root "$root" "$@"; }

configure_forge() {
    "$install_python" "$root/scripts/install/setup.py" workspace "$root"
}

check_launchers() {
    "$install_python" "$root/scripts/install/setup.py" check-launchers "$root"
}

install_launchers() {
    "$install_python" "$root/scripts/install/setup.py" launchers "$root" "$mode"
    "$HOME/.local/bin/mf" --help >/dev/null
    "$HOME/.local/bin/marketforge" --help >/dev/null
}

install_engines() {
    local target
    for target in normalize merge convert; do
        if mf --json engine status | "$install_python" "$root/scripts/install/setup.py" engine "$target"; then
            printf '  %s already installed and verified.\n' "$target"
        else
            mf engine build "$target" >/dev/null
            mf --json engine status | "$install_python" "$root/scripts/install/setup.py" engine "$target" ||
                fail "The installed $target failed verification. Inspect mf engine status and retry."
            printf '  Built and verified %s.\n' "$target"
        fi
    done
}

market_init() {
    local result rc=0 status
    if "$install_python" "$root/scripts/install/setup.py" already-initialized "$root" "$environment" "$registry_identity"; then
        printf '  Already complete for this registry.\n'
        return
    fi
    result=$("$install_python" "$root/scripts/install/provision.py" initialize "$root" "$environment" "$mf_path" "$registry_identity") || rc=$?
    status=$(printf '%s' "$result" | "$install_python" "$root/scripts/install/setup.py" initialization)
    case "$rc:$status" in
        0:complete)
            "$install_python" "$root/scripts/install/setup.py" record-initialization "$root" "$environment" "$registry_identity"
            printf '  Complete.\n'
            ;;
        *)
            printf '  %s (init exit %s). Local installation and migrations completed.\n' "$status" "$rc" >&2
            print_next_steps >&2
            if [[ $rc == 130 ]]; then exit 130; fi
            exit 3 ;;
    esac
}

print_next_steps() {
    printf 'From the checkout:\n  mf --help\n  mf tui --%s\n  mf init --%s  (retry or run market initialization)\n' "$environment" "$environment"
    printf 'CLI directory: %s/.local/bin\n' "$HOME"
    printf 'If it is missing from your PATH, add that directory or invoke the executable there directly.\n'
    if [[ $mode == dev ]]; then
        printf 'Development tools: %s/bin\nDocumentation tools: %s/.install/docs/bin\n' "$install_env" "$root"
    fi
}
