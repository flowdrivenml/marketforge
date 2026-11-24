# Debian and Ubuntu package and PostgreSQL cluster management.

native_manager=APT
apt_refreshed=false

detect_linux() {
    local release="${1:-/etc/os-release}" id
    [[ -r "$release" ]] || fail "Cannot identify the Linux distribution."
    id=$(awk -F= '$1 == "ID" {gsub(/"/, "", $2); print $2; exit}' "$release")
    case "$id" in
        ubuntu|debian) printf '  Linux distribution: %s\n' "$id" ;;
        *) fail "This Linux distribution is unsupported. Use Ubuntu or Debian." ;;
    esac
    need apt-get
}

apt_install() {
    (( $# > 0 )) || return 0
    need sudo
    if ! "$apt_refreshed"; then
        sudo apt-get update
        apt_refreshed=true
    fi
    sudo apt-get install -y "$@"
}

native_dependencies() {
    local -a packages
    packages=()
    have git || packages+=(git)
    have curl || packages+=(curl ca-certificates)
    have nvim || packages+=(neovim)
    have cc && have make || packages+=(build-essential)
    have pkg-config || packages+=(pkg-config)
    if [[ $mode == dev ]] && ! have node; then packages+=(nodejs); fi
    if [[ -n ${packages[*]-} ]]; then apt_install "${packages[@]}"; fi
}

local_postgres() {
    local installed=false clusters online version cluster status owner unused
    clusters=
    if have pg_lsclusters; then clusters=$(pg_lsclusters --no-header); fi
    if [[ -z $clusters ]]; then
        if have postgres || { have pg_isready && pg_isready -q -h 127.0.0.1 -p 5432; }; then
            fail "Found a non-APT PostgreSQL installation. Configure its Forge connection and rerun with --skip-postgres."
        fi
        apt_install postgresql postgresql-client
        installed=true
    fi
    need pg_lsclusters
    clusters=$(pg_lsclusters --no-header)
    online=$(printf '%s\n' "$clusters" | awk '$4 == "online" {print}')
    if [[ -z $online && $installed == true && -n $clusters && $clusters != *$'\n'* ]]; then
        read -r version cluster unused status owner unused <<<"$clusters"
        need pg_ctlcluster
        sudo pg_ctlcluster "$version" "$cluster" start
        online=$(pg_lsclusters --no-header | awk '$4 == "online" {print}')
    fi
    if [[ -z $online || $online == *$'\n'* ]]; then
        fail "Local provisioning needs exactly one running APT PostgreSQL cluster. Inspect pg_lsclusters, or configure a connection and use --skip-postgres."
    fi
    read -r version cluster pg_port status owner unused <<<"$online"
    [[ $version =~ ^[0-9]+$ ]] && (( version >= 14 )) ||
        fail "The selected local PostgreSQL cluster must be version 14 or later. Existing clusters are preserved."
    pg_socket=/var/run/postgresql
    need psql
    need sudo
    pg_admin=(sudo -u "$owner" -- "$(command -v psql)" -U "$owner")
}
