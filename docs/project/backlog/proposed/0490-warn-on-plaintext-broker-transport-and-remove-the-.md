---
id: 0490
title: Warn on plaintext broker transport and remove the default guest credential
status: Proposed
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: adapters
priority_rank: 30
---

# TASK-0490: Warn on plaintext broker transport and remove the default guest credential

## Summary
The RabbitMQ default URL is `amqp://guest:guest@localhost:5672/`. RabbitMQ and Redis give no warning when a non-TLS URL points to a remote host. Kafka warns but accepts SASL_PLAINTEXT. Disabled certificate checks give a warning only. Kafka `create_ssl_context` has no callers (dead code).

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-319 (cleartext transmission of sensitive information)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The defaults point to localhost, and RabbitMQ limits `guest` to localhost by default, so the risk is a configuration that moves to production unchanged. Topic, exchange and group names are operator configuration (Info).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/rabbitmq/config.py:16;` adapters/rabbitmq/connection_ssl.py:52-63, 108-114
- `src/eventsource/adapters/kafka/config.py:47`, 82-86, 127, 129-139
- `src/eventsource/adapters/redis/config.py:77`

**Verification**: Code read; grep shows no caller of `create_ssl_context`.

## Definition of Done (Blackbox Frontdoor TDD)
1. RabbitMQ and Redis log a warning when the URL has no TLS scheme and the host is not loopback, as Kafka does.
2. The RabbitMQ URL has no default credentials, or the adapter warns when it connects with `guest` to a non-loopback host.
3. The unused Kafka `create_ssl_context` is removed or connected.
4. Tests use the public config constructors and capture the warnings.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
