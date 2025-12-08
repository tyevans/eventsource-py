"""
Performance benchmarks for the eventsource library.

This module provides benchmark tests for critical paths:
- Event store operations (append, read)
- Projection processing throughput
- Repository operations (checkpoint, outbox, DLQ)
- Serialization/deserialization performance

Usage:
    # Run all benchmarks
    pytest tests/benchmarks/ --benchmark-only

    # Run with comparison to baseline
    pytest tests/benchmarks/ --benchmark-compare

    # Save baseline
    pytest tests/benchmarks/ --benchmark-save=baseline

    # Generate JSON report
    pytest tests/benchmarks/ --benchmark-json=results.json

For more options, see: pytest --help | grep benchmark
"""
