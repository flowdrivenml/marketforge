export UV_PROJECT_ENVIRONMENT="$PWD/.venv"

uv lock --project forge
uv sync --project forge --locked
