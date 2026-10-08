# Security Policy and Vulnerability Disclosure

## 1. Vulnerability Disclosure Policy & Workflows

We take the security of this system seriously. If you identify a security vulnerability, we request that you disclose it responsibly:
- **Private Reporting**: Do not disclose vulnerabilities in public issue trackers, pull requests, or discussion forums.
- **Response Timeline**: The security team acknowledges vulnerability reports within 24 hours and provides triage within 72 hours.
- **Coordinated Disclosure**: Fixes are developed in private security worktrees and published alongside advisories upon release.

## 2. Reporting Contacts & Incident Management

To report a vulnerability or security incident, contact the security team:
- **Primary Security Contact**: security@example.com
- **Incident Escalation**: security-incidents@example.com

## 3. PGP Key Fingerprint

Submissions containing sensitive vulnerability descriptions should be encrypted using our PGP key:
- **PGP Fingerprint**: `ABCD 1234 EF56 7890 ABCD 1234 EF56 7890 SPEC OPS1`
- **Key Server**: `keys.openpgp.org`

## 4. Autonomous Worker Execution Restrictions

Autonomous coding agents executing in this repository must operate under zero-trust constraints:
1. Forbids agents from hardcoding credentials, API keys, or secrets.
2. Forbids agents from modifying unapproved lockfiles.
3. Forbids agents from executing non-allowlisted shell commands.
