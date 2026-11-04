export UV_PROJECT_ENVIRONMENT="$PWD/.venv"

uv lock --project forge
uv sync --project forge --locked


curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
export UV_PROJECT_ENVIRONMENT="$PWD/.venv"
uv lock --project forge
uv sync --project forge --locked


export UV_PROJECT_ENVIRONMENT="$PWD/.venv"

uv run --project forge --locked marketforge --help
uv run --project forge --locked marketforge --version
uv run --project forge --locked ruff check forge
uv run --project forge --locked ruff format --check forge
uv run --project forge --locked pyright --project forge/pyproject.toml


hwo to run neocim editor without pytho errors

export UV_PROJECT_ENVIRONMENT="$PWD/.venv"

cd forge
uv run --locked nvim tests/unit/shared/test_paths.py 


How to create test database 

(
    set -eu
    : "${MARKETFORGE_DATABASE_URL:?MARKETFORGE_DATABASE_URL is not set}"

    marketforge_db_owner="$(
        psql --dbname="$MARKETFORGE_DATABASE_URL" \
            -XAt --set=ON_ERROR_STOP=1 \
            -c 'SELECT current_user'
    )"

    marketforge_db_port="$(
        psql --dbname="$MARKETFORGE_DATABASE_URL" \
            -XAt --set=ON_ERROR_STOP=1 \
            -c "SELECT current_setting('port')"
    )"

    sudo -u postgres createdb \
        --port="$marketforge_db_port" \
        --template=template0 \
        --owner="$marketforge_db_owner" \
        marketforge_test
)