"""
Performance benchmarks for repository operations.

Tests critical paths:
- Checkpoint repository operations (update, get, reset)
- DLQ repository operations (add, get)
- Outbox repository operations (add, get, mark published)

Target baselines:
- Update checkpoint: < 0.5ms
- Get checkpoint: < 0.2ms
- Add to DLQ: < 1ms
- Add to outbox: < 0.5ms
"""
