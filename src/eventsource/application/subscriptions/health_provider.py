"""
Health check provider for subscription health monitoring.

The HealthCheckProvider follows the Single Responsibility Principle by
handling only health-related operations:
- Health checks for individual subscriptions
- Manager-wide health aggregation
- Kubernetes-style readiness and liveness probes

Example:
    >>> provider = HealthCheckProvider(
    ...     registry=registry,
    ...     error_handlers=error_handlers,
    ...     config=HealthCheckConfig(),
    ... )
    >>> health = await provider.health_check()
    >>> readiness = await provider.readiness_check()
"""

from eventsource.application.subscriptions.health_provider_core import HealthCheckProvider

__all__ = ["HealthCheckProvider"]
