# ADR-0001: Specification as Code and Opinionated SDLC Guardrails

## Status
Accepted

## Context
Traditional agile tooling isolates user stories and backlog items in external web silos, leading to specification drift, context blindness for coding agents, and merge conflicts across parallel streams.

## Decision
We adopt **Project Management as Code**:
1. All project specifications (Personas, PRDs, User Stories, ADRs, Backlog Tasks) live inside `docs/project/` as Markdown documents with YAML frontmatter.
2. Specifications branch, review, and merge directly alongside production code.
3. Automated tools parse documentation into machine-readable relational graphs, ensuring zero specification drift.

## Consequences
- **Positive**: Full version-locking of requirements and code; conflict-free parallel worker execution; living interactive visualizer directly from git history.
- **Negative**: Requires discipline to maintain frontmatter metadata and lean JIT buffers.
