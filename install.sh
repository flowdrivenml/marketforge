#!/usr/bin/env bash
# Install from this checkout without modifying the login shell.
set +x
set -Eeuo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
cd -- "$root"
source "$root/scripts/install/common.sh"

mode=runtime
environment=production
dry_run=false
skip_postgres=false
skip_market_init=false
for option in "$@"; do
  case "$option" in
  --dev)
    mode=dev
    environment=testing
    ;;
  --dry-run) dry_run=true ;;
  --skip-postgres) skip_postgres=true ;;
  --skip-market-init) skip_market_init=true ;;
  --help | -h)
    usage
    exit 0
    ;;
  *)
    usage >&2
    fail "Unknown option. See ./install.sh --help."
    ;;
  esac
done

stage "Platform and tools"
case "$(uname -s)" in
Linux)
  source "$root/scripts/install/linux.sh"
  detect_linux
  ;;
Darwin) source "$root/scripts/install/macos.sh" ;;
*) fail "Unsupported platform. Use Ubuntu, Debian, or macOS." ;;
esac
case "$(uname -m)" in
x86_64 | aarch64 | arm64) ;;
*) fail "Unsupported CPU architecture. Use x86-64 or ARM64." ;;
esac
printf '  %s / %s. Mode: %s. Registry: %s.\n' "$(uname -s)" "$(uname -m)" "$mode" "$environment"

export PATH="$HOME/.local/bin:${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
if "$dry_run"; then
  print_plan
  exit 0
fi

((EUID != 0)) || fail "Run as the account that will use MarketForge. APT and PostgreSQL administration request elevation separately."
trap 'unexpected_failure "$?"' ERR
trap 'fail "Interrupted. Completed stages are retained; rerun to continue."' INT TERM

native_dependencies
ensure_uv
ensure_rust
need nvim
neovim_version=$(nvim --version | sed -n '1p') || fail "Neovim could not report its version. Check the existing installation."
[[ $neovim_version == NVIM* ]] || fail "The nvim command did not report a Neovim version."
printf '  Verified %s\n' "$neovim_version"

stage "Forge and CLI"
install_forge
configure_forge
check_launchers

stage "Managed Engine"
install_engines

stage "PostgreSQL"
source "$root/scripts/install/database.sh"
ensure_database

stage "Installation verification"
mf config show >/dev/null
mf --help >/dev/null
mf --json engine status | "$install_python" "$root/scripts/install/setup.py" engines
install_launchers

stage "Market initialization"
if "$skip_market_init"; then
  printf '  Skipped. Run the initialization command below when ready.\n'
else
  market_init
fi

printf '\nInstallation complete (%s).\n' "$mode"
print_next_steps
