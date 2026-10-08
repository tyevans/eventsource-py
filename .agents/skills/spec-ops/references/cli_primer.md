# SpecOps CLI Primer & Lifecycle Cheatsheet

This primer documents the public CLI entry points of SpecOps for orchestrators, agents, and human leads. SpecOps is managed via `uv run spec-ops <command>`.

---

## 1. Codebase Health & Quality Invariants

Inspect hard invariants, file lengths (<500 lines per ADR-0002), and backlog queue synchronization:

```bash
# Run full codebase health check
uv run spec-ops health

# Check supply-chain security policies and allowed licenses
uv run spec-ops health --security

# Audit project dependencies and lockfile immutability
uv run spec-ops audit

# Check constitution drift between specops.toml and AGENTS.md
uv run spec-ops constitution check

# Re-synchronize AGENTS.md while preserving custom invariants
uv run spec-ops constitution sync
```

---

## 2. Product Discovery & PRDs

PRDs live in `docs/project/product/` across lifecycle stages (`idea/`, `shaped/`, `accepted/`, `shipped/`):

```bash
# Audit PRD decomposition state and buffer readiness
uv run spec-ops prd audit PRD-XXXX

# Lint PRDs against falsifiable criteria and checkable outcomes
uv run spec-ops prd lint docs/project/product/accepted/PRD-XXXX.md
```

---

## 3. Backlog Management & Task Scaffolding

Tasks live in `docs/project/backlog/` across `proposed/`, `refined/`, and `complete/`:

```bash
# Scaffold a new PMaC task with Definition of Ready frontmatter
uv run spec-ops task create \
  --title "My New Feature Slice" \
  --bc core \
  --prd PRD-0006 \
  --story US-0117 \
  --adr ADR-0001,ADR-0003 \
  --stage proposed \
  --non-interactive

# Audit backlog flow telemetry, bottleneck detection, and queue depth
uv run spec-ops backlog telemetry

# Diagnose and repair backlog health (orphans, cycle blocks, priority drift)
uv run spec-ops backlog doctor --repair
```

---

## 4. Cognitive Backlog Curation (JIT Refinement)

Curate proposed tasks into the ready buffer (`refined/`) to maintain ~10 ready tasks:

```bash
# Dry-run cognitive curation to inspect proposed tasks and architectural drift
uv run spec-ops curate --infer --dry-run

# Execute cognitive curation (promotes tasks, reconciles drift, synthesizes DoR criteria)
uv run spec-ops curate --infer

# Traditional priority-based curation buffer fill
uv run spec-ops curate
```

---

## 5. Worktree Sandboxing & Worker Execution

SpecOps enforces strict backlog isolation (ADR-0005) by executing in isolated git worktrees:

```bash
# Scaffold a human or agent worktree for a task
uv run spec-ops worktree create TASK-XXXX

# List active worktrees and their status
uv run spec-ops worktree list

# Run autonomous worker on the next ready task
uv run spec-ops worker

# Target a specific refined task
uv run spec-ops worker --task TASK-XXXX --dry-run

# Run full autonomous cycle with concurrent worker batching
uv run spec-ops cycle --max-workers 2
```

---

## 6. Worktree Rescue & Failure Triage

When a task fails or stalls during autonomous development:

```bash
# Inspect stalled worktree diagnostics and error logs
uv run spec-ops rescue inspect TASK-XXXX

# Salvage partial progress and stage clean files into a human branch
uv run spec-ops rescue salvage TASK-XXXX --files src/core/

# Reset stalled task with anti-loop failure memory to prevent recurring errors
uv run spec-ops rescue reset TASK-XXXX --anti-loop

# Clean up orphaned worktree directories and branches safely
uv run spec-ops rescue prune --older-than 7d
```

---

## 7. Quality Gates & Frontdoor Verification

Ensure 100% blackbox frontdoor pass rate before finalizing:

```bash
# Run test suite
uv run pytest

# Check lockfile synchronization
uv lock --check

# Audit for illegal private backdoor mocks (ADR-0003)
uv run spec-ops test anti-mock

# Run property-based invariant checks (Hypothesis)
uv run spec-ops verify properties
```

---

## 8. Documentation, Visualizer & Release Management

```bash
# Build Diataxis documentation site
uv run spec-ops docs build

# Launch zero-dependency living 2D graph visualizer
uv run spec-ops visualizer serve --port 8080

# Generate customer-facing release notes and business changelog
uv run spec-ops release generate --milestone v1.0.0

# Export executive roadmaps and stakeholder presentations
uv run spec-ops report burndown --format html
```
