# Releasing eventsource-py

This document describes the release process for eventsource-py.

## Overview

Releases are automated via GitHub Actions. When you push a version tag, the release workflow:

1. Validates the tag version matches `pyproject.toml`
2. Runs all CI checks (lint, type-check, tests)
3. Builds wheel and source distributions
4. Publishes to PyPI or TestPyPI (based on version type)
5. Creates a GitHub Release with changelog and artifacts

## Release Types

| Version Pattern | Example | Destination | GitHub Release |
|-----------------|---------|-------------|----------------|
| Stable | `v1.0.0` | PyPI | Stable |
| Alpha | `v1.0.0a1` | TestPyPI | Pre-release |
| Beta | `v1.0.0b1` | TestPyPI | Pre-release |
| Release Candidate | `v1.0.0rc1` | PyPI | Pre-release |

## Creating a Release

### 1. Update Version

Edit `pyproject.toml` and update the version:

```toml
[project]
version = "X.Y.Z"
```

### 2. Update Changelog

Move entries from `[Unreleased]` to a new version section in `CHANGELOG.md`:

```markdown
## [Unreleased]

## [X.Y.Z] - YYYY-MM-DD

### Added
- New feature description

### Changed
- Change description

### Fixed
- Bug fix description
```

Update the comparison links at the bottom:

```markdown
[Unreleased]: https://github.com/tyevans/eventsource-py/compare/vX.Y.Z...HEAD
[X.Y.Z]: https://github.com/tyevans/eventsource-py/releases/tag/vX.Y.Z
```

### 3. Commit Changes

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "chore: prepare release X.Y.Z"
git push origin main
```

### 4. Create and Push Tag

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

### 5. Monitor Release

1. Go to [GitHub Actions](https://github.com/tyevans/eventsource-py/actions)
2. Watch the "Release" workflow
3. Verify the [GitHub Release](https://github.com/tyevans/eventsource-py/releases) was created
4. Verify the package on [PyPI](https://pypi.org/project/eventsource-py/)

## Version Numbering (SemVer)

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (`X.0.0`): Breaking API changes
- **MINOR** (`0.X.0`): New features, backwards compatible
- **PATCH** (`0.0.X`): Bug fixes, backwards compatible

### Pre-release Versions

- **Alpha** (`X.Y.Za1`): Early development, API may change significantly
- **Beta** (`X.Y.Zb1`): Feature complete, API stabilizing
- **RC** (`X.Y.Zrc1`): Release candidate, final testing before stable

## Testing a Release (TestPyPI)

For testing the release process without affecting production PyPI:

```bash
# Update version to alpha
# In pyproject.toml: version = "0.2.0a1"

git add pyproject.toml
git commit -m "chore: prepare alpha release 0.2.0a1"
git tag v0.2.0a1
git push origin v0.2.0a1
```

Install from TestPyPI:

```bash
pip install -i https://test.pypi.org/simple/ eventsource-py==0.2.0a1
```

## Hotfix Releases

For urgent fixes to a released version:

```bash
# Create hotfix branch from the release tag
git checkout -b hotfix/0.1.1 v0.1.0

# Apply fix
# Update version in pyproject.toml to 0.1.1
# Update CHANGELOG.md

git add -A
git commit -m "fix: description of the fix"
git tag v0.1.1
git push origin v0.1.1

# Merge back to main
git checkout main
git merge hotfix/0.1.1
git push origin main
```

## Troubleshooting

### Version Mismatch Error

If the workflow fails with "Tag version does not match pyproject.toml":

1. Delete the tag: `git tag -d vX.Y.Z && git push origin :refs/tags/vX.Y.Z`
2. Fix the version in `pyproject.toml`
3. Commit and recreate the tag

### CI Failure During Release

If CI fails after pushing a tag:

1. Do NOT delete the tag yet
2. Fix the issue on main
3. Delete the tag: `git tag -d vX.Y.Z && git push origin :refs/tags/vX.Y.Z`
4. Cherry-pick the fix if needed
5. Recreate the tag

### PyPI Upload Failure

If publishing to PyPI fails:

1. Check the GitHub Actions logs for the error
2. If it's a transient error, re-run the workflow
3. If it's a version conflict (version already exists), increment the version

## Infrastructure

### Trusted Publishing

This project uses [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC) instead of API tokens. The configuration is:

- **Owner**: `tyevans`
- **Repository**: `eventsource-py`
- **Workflow**: `release.yml`
- **Environments**: `pypi` (production), `testpypi` (testing)

### GitHub Environments

Two environments are configured in the repository:

- `pypi`: For production releases to PyPI
- `testpypi`: For pre-releases to TestPyPI

## Agent-Assisted Releases

AI agents can help prepare releases by:

1. Analyzing commits since the last tag
2. Generating changelog entries
3. Updating the version in `pyproject.toml`
4. Creating a release preparation PR

**Human approval is always required** before:
- Merging the release preparation PR
- Creating and pushing the release tag
