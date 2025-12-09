"""
Subscription Manager Examples

This package contains working examples demonstrating SubscriptionManager usage:

- basic_projection.py: Simple read model projection with order counts by status
- resilient_projection.py: Production-ready projection with retry, circuit breaker, DLQ
- multi_subscriber.py: Multiple projections running concurrently with health monitoring

Run examples with:
    python -m examples.subscriptions.basic_projection
    python -m examples.subscriptions.resilient_projection
    python -m examples.subscriptions.multi_subscriber
"""
