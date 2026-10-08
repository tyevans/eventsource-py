# ADR-0008: Zero-Trust Autonomous Worker Process Sandboxing

## Status
Accepted

## Context
Autonomous coding agents running unrestricted shell commands in developer environments pose significant supply-chain, privilege escalation, and lateral movement risks.

## Decision
We enforce **Zero-Trust Autonomous Worker Process Sandboxing**:
1. Autonomous worker processes execute in isolated sandboxes with strict command allowlisting.
2. Arbitrary shell command execution outside approved development toolchains is blocked.
3. Network egress during autonomous code generation is monitored and restricted.

## Consequences
- **Positive**: Prevents unauthorized execution, supply-chain exfiltration, and destructive commands.
- **Negative**: Requires explicit allowlisting of development tools and commands.
