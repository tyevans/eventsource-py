---
id: 0480
title: Redact broker credentials in config repr and connection logs
status: Complete
governing_adrs:
- ADR-0010
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: adapters
priority_rank: 20
---

# TASK-0480: Redact broker credentials in config repr and connection logs

## Summary
The Redis bus logs `redis_url` (with its password) at INFO on each connect. The default `repr()` of the Kafka, RabbitMQ and Redis config dataclasses shows passwords. The RabbitMQ URL sanitizer leaves part of a password that contains `@`.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-532 (sensitive information in log file)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The Redis leak shows with structured (JSON) log formatters. The repr leak shows when an application or a traceback prints the config object. `KafkaEventBusConfig.get_sanitized_config()` shows that masking was intended.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/redis/bus_connection.py:62-64`
- `src/eventsource/adapters/kafka/config.py:50`, 141-145
- `src/eventsource/adapters/rabbitmq/config.py:16;` adapters/redis/config.py:77
- `src/eventsource/adapters/rabbitmq/connection_ssl.py:28` (regex); used at adapters/rabbitmq/connection.py:64-65, 75, 92-100

**Verification**: Proven: `repr(KafkaEventBusConfig(sasl_password="s3cret"))` and the RabbitMQ and Redis config reprs show the secret. `sanitize_url("amqp://u:p@ss@host/")` returns `amqp://***:***@ss@host/`.

## Definition of Done (Blackbox Frontdoor TDD)
1. One shared URL redaction helper splits credentials at the last `@` of the authority. RabbitMQ and Redis use it for every logged URL.
2. Password and URL fields use `field(repr=False)` or a redacted repr.
3. A test builds each config with a known secret, connects with a fake client, and asserts that the secret is not in `repr()` or in any captured log record.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
