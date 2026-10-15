"""
Performance benchmarks for projection operations.

Tests critical paths:
- Event handler routing
- Projection event processing throughput
- Checkpoint tracking overhead
- Multiple handler dispatch

Target baselines:
- Process single event: < 0.5ms
- Process 100 events: < 50ms
- Process 1000 events: < 500ms
"""
