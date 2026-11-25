# Release procedure

MarketForge uses [Semantic Versioning](https://semver.org/). Forge and the Engine share a release version. During the initial `0.y.z` series, document changes to compatibility, including any recipe, dataset, or API migration, in the changelog.

| Release | When to use it | Example |
| --- | --- | --- |
| Patch | Fix a bug or correct behavior while preserving compatibility | `0.1.0` → `0.1.1` |
| Minor | Add user-facing capabilities and reset the patch number. During initial development, use a minor release for deliberate compatibility changes and document the migration. | `0.1.0` → `0.2.0` |

These version numbers are examples. Choose the next version after reviewing the [existing releases](https://github.com/flowdrivenml/marketforge/releases) and remote tags. Version changes are made as part of release preparation.

## Prepare the release commit

1. Set the same version in `forge/pyproject.toml` and `engine/Cargo.toml`.
2. Refresh the project entries in `forge/uv.lock` and `engine/Cargo.lock` with the commands below. Check that the locked versions of `marketforge` and `marketforge-engine` match their manifests. Review any unrelated dependency changes before committing.
3. Add the chosen version and UTC publication date to `CHANGELOG.md` and update `CITATION.cff`. Record changes, compatibility notes, and platform limitations in the changelog. If publication moves to another UTC date, update both files before tagging.
4. Run the [checks appropriate to the changes](testing.md), including the pinned [documentation build and link checks](setup.md#build-and-preview-the-handbook). Review and commit the release changes, push them to `main`, and verify the remote commit and Pages deployment.

```sh
uv lock --project forge
cargo update --workspace --manifest-path engine/Cargo.toml
```

## Tag and publish

Confirm that both the target tag and release name are available in the repository. Set `version` to the number in the release commit and `release_commit` to the exact verified commit on `origin/main`. From a clean checkout, create and push one annotated tag:

```sh
git ls-remote origin refs/heads/main "refs/tags/v${version}*"
gh release list
git tag -a "v${version}" "$release_commit" -m "MarketForge v${version}"
git push origin "refs/tags/v${version}"
git ls-remote origin "refs/tags/v${version}^{}"
```

Check that the peeled remote tag resolves to the release commit. Prepare `release-notes.md` from the changelog entry, with links to the installation guide, Quick Start, and roadmap. Publish the source release:

```sh
gh release create "v${version}" --verify-tag --latest \
  --title "MarketForge v${version}" --notes-file release-notes.md
gh release view "v${version}"
```

Confirm that GitHub marks the stable release as latest, that the notes and source archives appear on the release page, and that the documentation links work. GitHub generates the source archives. Preserve published tags and use a new version for follow-up fixes.
