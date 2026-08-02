# CLAUDE.md

Production-ready event sourcing library for Python (pydantic, sqlalchemy, async-first).

- Docs: `docs/` (mkdocs) -- architecture, ADRs, tutorials, reference
- Conventions and workflow rules: `.claude/rules/`
- Commands: `make help` for all targets; `make check` is CI parity

**Before writing or reviewing code:** `.claude/rules/recurring-defects.md`
records the six mistakes this project actually repeats (derived from ~130 fix
commits). Most defects here are one fact stored in two places with nothing that
fails when the copies disagree.
