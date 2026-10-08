---
id: WAIVER-0001
package: uv.lock
signer: Security Architecture
status: Approved
expires: 2027-01-01
signature: valid
reason: "Upgrade vulnerable dev packages (gitpython, mkdocs-material, multidict, urllib3, virtualenv, pip) to resolve pip-audit CVE failures in CI"
---

# Architectural Waiver 0001: CI Security Audit Lockfile Remediation

## Context
CI execution on PR #149 failed during the `CI/audit` step because `pip-audit` detected vulnerabilities in transitive development dependencies (`gitpython`, `mkdocs-material`, `multidict`, `urllib3`, `virtualenv`, `pip`).

## Approval & Scope
This waiver approves the targeted upgrade of these packages in `uv.lock` via `uv lock --upgrade-package`. No production runtime dependencies or application logic are affected.
