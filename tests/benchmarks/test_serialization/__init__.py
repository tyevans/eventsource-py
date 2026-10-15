"""
Performance benchmarks for serialization/deserialization operations.

Tests critical paths:
- Event to_dict() serialization
- Event from_dict() deserialization
- Pydantic model_dump() and model_validate()
- JSON-compatible serialization

Target baselines:
- Single event serialization: < 0.1ms
- Single event deserialization: < 0.1ms
- Batch serialization (100 events): < 10ms
- Batch deserialization (100 events): < 10ms
"""
