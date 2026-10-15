"""
Performance benchmarks for event store operations.

Tests critical paths:
- Single event append operations
- Batch event append operations
- Event reading (single aggregate)
- Concurrent append operations

Target baselines (InMemory):
- Append single event: < 0.1ms
- Read 100 events: < 5ms
- Read 1000 events: < 10ms
- 100 concurrent appends: < 50ms
"""
