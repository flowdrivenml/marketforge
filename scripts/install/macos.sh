# Homebrew dependencies and local PostgreSQL on Intel and Apple Silicon.

native_manager=Homebrew

find_brew() {
    local candidate
    if have brew; then return; fi
    for candidate in /opt/homebrew/bin/brew /usr/local/bin/brew; do
        if [[ -x "$candidate" ]]; then
            export PATH="${candidate%/*}:$PATH"
            return
        fi
    done
    return 1
}

brew_bootstrap() { /bin/bash "$1"; }

native_dependencies() {
    if ! xcode-select -p >/dev/null 2>&1; then
        xcode-select --install
        fail "Finish installing the Apple command-line tools in the system dialog, then rerun."
    fi
    if ! find_brew; then
        local choice
        printf 'Homebrew is required. Run the official Homebrew installer now? [y/N] ' > /dev/tty 2>/dev/null ||
            fail "Install Homebrew from https://brew.sh/ and rerun."
        read -r choice < /dev/tty || fail "Homebrew installation needs confirmation."
        case "$choice" in
            y|Y|yes|YES) bootstrap https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh brew_bootstrap ;;
            *) fail "Install Homebrew from https://brew.sh/ and rerun." ;;
        esac
        find_brew || fail "Homebrew was not found after bootstrap. Follow its setup instructions and retry."
    fi
    local -a packages
    packages=()
    have nvim || packages+=(neovim)
    have pkg-config || packages+=(pkgconf)
    have git || packages+=(git)
    have curl || packages+=(curl)
    if [[ $mode == dev ]] && ! have node; then packages+=(node); fi
    if [[ -n ${packages[*]-} ]]; then brew install "${packages[@]}"; fi
}

local_postgres() {
    local formula prefix candidates
    candidates=$(brew list --formula | awk '/^postgresql(@[0-9]+)?$/')
    pg_socket=/tmp
    pg_port=5432
    if [[ $candidates == *$'\n'* ]]; then
        fail "Multiple Homebrew PostgreSQL servers are installed. Configure the intended connection and rerun with --skip-postgres."
    elif [[ -z $candidates ]]; then
        if have postgres || { have pg_isready && pg_isready -q -h 127.0.0.1 -p "$pg_port"; } ||
            [[ -S "$pg_socket/.s.PGSQL.$pg_port" ]]; then
            fail "Found a non-Homebrew PostgreSQL server. Configure its Forge connection and rerun with --skip-postgres."
        fi
        formula=postgresql@17
        brew install "$formula"
        brew services start "$formula"
    else
        formula=$candidates
    fi
    prefix=$(brew --prefix "$formula")
    pg_admin=("$prefix/bin/psql" -U "$(id -un)")
    local retries=0
    until "$prefix/bin/pg_isready" -q -h "$pg_socket" -p "$pg_port"; do
        if [[ -n $candidates ]]; then
            fail "Existing Homebrew PostgreSQL is unavailable on the default socket and port. Start it or configure its connection and rerun with --skip-postgres."
        fi
        (( retries < 15 )) || fail "Homebrew PostgreSQL did not start. Inspect brew services list."
        retries=$((retries + 1))
        sleep 1
    done
}
