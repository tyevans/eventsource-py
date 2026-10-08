# ADR-0010: Real-Time Secret Scanning and Credential Leak Defense

## Status
Accepted

## Context
Hardcoded credentials, API tokens, and private keys committed to git repositories cause catastrophic data leaks that persist forever in commit histories.

## Decision
We enforce **Real-Time Secret Scanning and Credential Leak Defense**:
1. Autonomous agents and human contributors are strictly forbidden from hardcoding credentials, tokens, or private keys.
2. Preflight and pre-commit hooks inspect diffs for high-entropy strings and known secret patterns.
3. Commits containing detected secrets are rejected immediately before staging or pushing.

## Consequences
- **Positive**: Guarantees zero credentials leak into git history or pull requests.
- **Negative**: Occasional false positives on high-entropy non-secret test fixtures require explicit inline ignores.
