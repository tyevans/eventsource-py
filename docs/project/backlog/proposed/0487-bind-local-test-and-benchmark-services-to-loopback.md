---
id: 0487
title: Bind local test and benchmark services to loopback
status: Proposed
governing_adrs:
- ADR-0010
governing_prds:
- PRD-0004
governing_stories:
- US-0013
target_bc: core
priority_rank: 27
---

# TASK-0487: Bind local test and benchmark services to loopback

## Summary
The compose files publish PostgreSQL, Redis, RabbitMQ and Kafka on all interfaces with known credentials (test/test, bench/bench, guest/guest) or no authentication. Hosts on the same network can reach them while the stack runs.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-1327 (binding to an unrestricted IP address)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The data is test data only. The risk is to contributors on shared networks.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `docker-compose.test.yml:25-26`, 28, 43
- `docker-compose.bench.yml:16-17`, 19, 31, 42-43, 45, 57, 61
- `bench/README.md:24`, 27

**Verification**: Compose files read. Short port syntax binds 0.0.0.0.

## Definition of Done (Blackbox Frontdoor TDD)
1. Every published port uses the `127.0.0.1:` prefix. Kafka advertises a loopback listener.
2. The compose files state that the credentials are local-only.
3. `make integration-up` and `make bench-up` still work.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
