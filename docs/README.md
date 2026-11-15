# Documentation development

This is a small Zensical documentation scaffold. User-facing sources live in `content/`; older engineering records in `historical/` and `tofixlater/` remain in place and are outside this site's source tree.

From the repository root, create the documentation-only environment if it does not already exist:

```bash
python3 -m venv docs/.venv
docs/.venv/bin/python -m pip install -r docs/requirements.txt

# Preview locally (default: http://127.0.0.1:8000)
(cd docs && .venv/bin/zensical serve)

# Validate the documentation build
(cd docs && .venv/bin/zensical build --strict)
```

Stop the preview with Ctrl-C. Generated files go to `docs/site/`; that directory, `docs/.venv/`, and the Zensical cache (`docs/.cache/`) are ignored. Do not use this environment for Forge. No deployment is configured.

Zensical is pinned to 0.1.1. Configuration follows the official [basics](https://zensical.org/docs/setup/basics/) and [navigation](https://zensical.org/docs/setup/navigation/) documentation and is checked by a strict build. System fonts and local branding keep the core site independent of remote assets after dependencies are installed.

Keep pages short and label unfinished work precisely: **Documentation draft** for writing or verification gaps, **Planned capability** for future functionality, and **Implementation status not assessed** when readiness has not been established. A site build validates documentation, not application examples.
