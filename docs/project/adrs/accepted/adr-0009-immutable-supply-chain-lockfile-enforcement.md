# ADR-0009: Immutable Supply-Chain Lockfile Enforcement

## Status
Accepted

## Context
LLMs frequently hallucinate dependency names or introduce malicious or vulnerable packages into lockfiles without verification.

## Decision
We enforce **Immutable Supply-Chain Lockfile Enforcement**:
1. Autonomous agents are strictly forbidden from modifying lockfiles (`uv.lock`, `package-lock.json`) without explicit human approval.
2. Preflight verification enforces strict lockfile integrity checks (`uv lock --check`).
3. Dependency updates must undergo human-reviewed security audits.

## Consequences
- **Positive**: Eliminates hallucinated package attacks, dependency confusion, and untracked supply-chain drifts.
- **Negative**: Adding new dependencies requires an explicit human review step.
