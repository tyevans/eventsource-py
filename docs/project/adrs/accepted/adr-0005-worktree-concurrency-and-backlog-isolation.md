# ADR-0005: Git Worktree Concurrency and Strict Backlog Isolation

## Status
Accepted

## Context
When multiple coding agents work concurrently on adjacent backlog tasks, having feature branches modify shared planning files (like `PRIORITY.md` or moving task files) causes guaranteed merge conflicts upon PR integration.

## Decision
We enforce **Strict Backlog Isolation on Feature Branches**:
1. Autonomous worker streams execute in isolated git worktrees (`feat/<task-slug>`).
2. Feature branches are **strictly forbidden from modifying `docs/project/backlog/`**.
3. Backlog state transitions (moving the task from `refined/` to `complete/` and updating `PRIORITY.md`) are executed exclusively by the orchestrator under `MERGE_LOCK` directly on `main` upon PR merge.

## Consequences
- **Positive**: Enables N concurrent worker streams to merge cleanly in parallel without merge conflicts.
- **Negative**: Backlog state changes can only be finalized upon integration into `main`.
