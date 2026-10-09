# Serialization Adapter

Reference for `eventsource.adapters.serialization` — the high-performance JSON encoding and decoding utilities used by `eventsource` storage adapters, outbox tables, and dead-letter queues.

## Overview

JSON serialization in `eventsource` is backed by `orjson`, a compiled C/Rust extension that natively serializes `UUID`, `datetime`, and complex payloads orders of magnitude faster than standard library `json`.

The package exposes:

| Exported Name | Kind | Signature | Purpose |
|---|---|---|---|
| `json_dumps` | function | `json_dumps(obj: Any) -> str` | Fast JSON serialization to string with native UUID, datetime, and non-string key support. |
| `json_loads` | function | `json_loads(s: str) -> Any` | Fast JSON deserialization from string to Python primitives. |
| `EventSourceJSONEncoder` | class | `json.JSONEncoder` subclass | Compatibility shim for stdlib `json.dumps(..., cls=EventSourceJSONEncoder)` consumers. |

```python
from uuid import uuid4
from datetime import UTC, datetime
from eventsource.adapters.serialization import json_dumps, json_loads

payload = {
    "event_id": uuid4(),
    "occurred_at": datetime.now(UTC),
    "data": {"count": 42},
}

# Fast serialization to string
raw = json_dumps(payload)

# Deserialization back to primitives
data = json_loads(raw)
```

## Import Locations

All three serialization utilities are exported from:

```python
from eventsource.adapters.serialization import json_dumps, json_loads, EventSourceJSONEncoder
```

`EventSourceJSONEncoder` is additionally re-exported from the top-level package namespace (`eventsource.EventSourceJSONEncoder`).

---

## Contract & Serialization Constraints

The encoder adheres to explicit performance and safety contracts:

### 1. Native UUID & Datetime Handling

`orjson` serializes `UUID` and `datetime` directly in compiled code without calling custom Python callbacks on each instance:
- `UUID`: Formatted as canonical 36-character string (`"12345678-1234-5678-1234-567812345678"`).
- `datetime`: Formatted as ISO 8601 string (e.g. `"2026-10-09T18:00:00+00:00"`). Aware datetimes preserve timezone offsets.

### 2. Non-String Dict Keys

`json_dumps` enables `orjson.OPT_NON_STR_KEYS`. Dict keys that are `UUID`, `int`, or other non-string types serialize cleanly:

```python
event_id = uuid4()
raw = json_dumps({event_id: "order-123"})  # Serializes successfully to {"<uuid-str>": "order-123"}
```

### 3. 64-Bit Integer Bounds

`orjson` supports signed and unsigned 64-bit integers (`[-2**63, 2**64 - 1]`). Integers outside this range trigger a `ValueError`:

```python
json_dumps({"overflow": 2**64})  # Raises ValueError: Integer exceeds 64-bit range
```

`json_dumps` translates `orjson`'s internal `TypeError` into a clean, predictable `ValueError` without incurring the latency penalty of pre-scanning payloads.

### 4. Floating Point Numbers & Non-Finite Values

- Standard floats are formatted conforming to RFC 8259.
- **Upstream Validation**: `DomainEvent` models enforce `allow_inf_nan=False` at event instantiation, ensuring non-finite floats (`inf`, `-inf`, `nan`) fail fast with Pydantic `ValidationError` before reaching serialization.
- For raw dicts passed directly to `json_dumps`, non-finite floats are rendered as JSON `null` by `orjson`.

For a full specification of serialization boundaries, see [`docs/reference/serialization-limits.md`](../../../docs/reference/serialization-limits.md).
