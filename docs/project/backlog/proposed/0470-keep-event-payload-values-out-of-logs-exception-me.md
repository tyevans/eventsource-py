---
id: '0470'
title: Keep event payload values out of logs, exception messages and health output
status: Proposed
dependencies:
- TASK-0462
governing_adrs:
- ADR-0010
governing_prds:
- PRD-0004
governing_stories:
- US-0003
target_bc: adapters
priority_rank: 10
---

# TASK-0470: Keep event payload values out of logs, exception messages and health output

## Summary
Deserialization and projection errors log `str(e)`. Pydantic errors include the input values and SQLAlchemy errors include the bound parameters, so personal data from events goes to ERROR logs, to exception messages and to `last_error` in the subscription health report. Some log lines also put the header `event_type` into an f-string message.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-532 (sensitive information in log file)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

Log aggregators and health endpoints often have wider access than the event store. The header `event_type` comes from the broker, so a value with newline characters can forge log lines (CWE-117).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/rabbitmq/serialization.py:129-131`, 138-152
- `src/eventsource/adapters/kafka/serialization.py:70-73;` adapters/kafka/consumer_message.py:106
- `src/eventsource/adapters/rabbitmq/consumer_dispatch.py:84;` adapters/redis/bus_consume.py:125
- `src/eventsource/adapters/sql/projection.py:75-76`, 116
- `src/eventsource/application/subscriptions/health_provider_probes.py:65;` health_subscription_indicators.py:45-46
- `src/eventsource/application/subscriptions/error_handling_types.py:~57-66` (to_dict includes error_stacktrace)
- `src/eventsource/adapters/kafka/consumer_message.py:68-71`, 82 (raw header event_type as metric label and span name: unbounded cardinality)
- `src/eventsource/adapters/kafka/consumer_helpers.py:65` (str(error) copied into DLQ headers)

**Verification**: Proven: `str(ValidationError)` contained `input_value='victim-secret@example.com'`. A SQLite IntegrityError string contained `[parameters: (2, 'x')]`.

## Definition of Done (Blackbox Frontdoor TDD)
1. These sites log the error class, the event id, the event type and the pydantic error locations. They do not log input values or SQL parameters.
2. Untrusted header values go only into structured `extra` fields, not into the message text.
3. Metric labels and span names use the registered event type, or the fixed value `unknown`.
4. The health report does not contain stack traces. `last_error` contains the error class and a redacted message.
5. A test sends an invalid event that contains a known marker string. It asserts that the marker is not in any captured log record or in the health report.
6. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
