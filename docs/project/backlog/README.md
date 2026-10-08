# Backlog Management Guide

Tasks move through three lifecycle stages:
- `proposed/`: Unrefined ideas, feature proposals, and discovered refactoring candidates.
- `refined/`: Architectural impact review completed, governing ADRs/PRDs cited, and testable blackbox definition of done established.
- `complete/`: Verified against tests, linted, committed, and integrated into the codebase.

## Core Invariants
1. **Just-In-Time (JIT) Refinement**: Maintain a lean ready buffer of ~10 tasks in `refined/`.
2. **Backlog Isolation**: Feature branches never modify `docs/project/backlog/` to ensure zero-conflict parallel merges.
3. **Blackbox Frontdoor Verification**: Test criteria must verify observable outputs via public entrypoints with zero private backdoor manipulation.
