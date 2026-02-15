---
paths:
  - "**/*"
---

# Commit Message Convention

This project uses a loose conventional commit style inferred from git history.

## Format

```
<type>: <short description in lowercase>
```

## Common Types

- `feat:` -- new feature or capability
- `fix:` -- bug fix
- `refactor:` -- code restructuring without behavior change
- `chore:` -- version bumps, dependency updates, CI changes
- `test:` -- test additions or fixes
- `docs:` -- documentation changes

## Notes

- Short descriptions are lowercase, no period at end
- Merge commits use default GitHub PR merge format
- Release commits: `release X.Y.Z`
- Simple changes sometimes omit the type prefix (e.g., "update tutorials")
