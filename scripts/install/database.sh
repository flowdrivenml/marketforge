# Delegate database inspection and credential storage to Python and Forge.

ensure_database() {
    local code=0
    "$install_python" "$root/scripts/install/provision.py" check "$root" "$environment" || code=$?
    if [[ $code == 4 ]]; then
        if "$skip_postgres"; then
            fail "No $environment credentials. Configure the matching key in the environment or .secrets/postgres.env and retry."
        fi
        local_postgres
        "$install_python" "$root/scripts/install/provision.py" provision "$root" "$environment" "$pg_socket" "$pg_port" "${pg_admin[@]}"
        "$install_python" "$root/scripts/install/provision.py" check "$root" "$environment"
    elif [[ $code != 0 ]]; then
        fail "The configured database did not pass preflight. Resolve the reported issue before retrying."
    fi
    registry_identity=$("$install_python" "$root/scripts/install/provision.py" migrate "$root" "$environment" "$mf_path")
    printf '  Verified %s database identity and migration history.\n' "$environment"
}
