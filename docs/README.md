# Documentation development

MarketForge uses Zensical for its public documentation. User-facing sources live in `content/`. The site navigation is defined in `zensical.toml`. Build and serve from the documentation directory. If a preview process already shares that output directory, use an isolated copy for verification to avoid mixing assets written by different Zensical versions.

From the repository root, create the documentation-only environment if it does not already exist:

```bash
python3 -m venv docs/.venv
docs/.venv/bin/python -m pip install -r docs/requirements.txt

# Preview locally (default: http://127.0.0.1:8000)
(cd docs && .venv/bin/zensical serve)

# Validate the documentation build
(cd docs && .venv/bin/zensical build --strict)
python3 docs/check_links.py
```

Stop the preview with Ctrl-C. Generated files go to `docs/site/`; that directory, `docs/.venv/`, and the Zensical cache (`docs/.cache/`) are ignored. Do not use this environment for Forge. [The documentation workflow](../.github/workflows/docs.yml) builds and publishes the site to GitHub Pages from `main`. Run it manually from the Actions tab when needed. The previous `gh-pages` branch is kept as deployment history.

Zensical is pinned in `requirements.txt`. Mermaid and source footnotes are enabled in the site configuration. Check both in a browser after changing a diagram or citation. Configuration follows the official [basics](https://zensical.org/docs/setup/basics/) and [navigation](https://zensical.org/docs/setup/navigation/) documentation and is checked by a strict build. System fonts and local branding keep the core site independent of remote assets after dependencies are installed.

When updating a workflow, also execute its focused verification in the guarded testing environment. Keep provider-dependent checks separate from the site build and link checker.
