"""
Integration tests for observability (tracing) functionality.

Tests in this package verify:
- End-to-end trace context propagation
- Span hierarchy across components
- Distributed tracing for message buses (RabbitMQ, Kafka)
- Graceful degradation when tracing is disabled
- Performance overhead of tracing
"""
