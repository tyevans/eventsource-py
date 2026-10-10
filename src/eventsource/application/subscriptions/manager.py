"""
Subscription manager for coordinating catch-up and live event subscriptions.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
- TASK-0008 (Decompose Monolithic Modules SubscriptionManager and Shutdown)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.error_handling import (
    ErrorCallback,
    ErrorHandlingConfig,
    SubscriptionErrorHandler,
)
from eventsource.application.subscriptions.health import HealthCheckConfig
from eventsource.application.subscriptions.health_provider import HealthCheckProvider
from eventsource.application.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.application.subscriptions.manager_dispatch import ManagerDispatchMixin
from eventsource.application.subscriptions.manager_lifecycle import ManagerLifecycleMixin
from eventsource.application.subscriptions.manager_registry import ManagerRegistryMixin
from eventsource.application.subscriptions.pause_resume import PauseResumeController
from eventsource.application.subscriptions.registry import SubscriptionRegistry
from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownResult,
)
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.ports.bus import SubscribableEventBus
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.dlq import DLQRepository
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class SubscriptionManager(ManagerLifecycleMixin, ManagerDispatchMixin, ManagerRegistryMixin):
    """
    Manages catch-up and live event subscriptions.

    The SubscriptionManager coordinates between the event store (for historical
    events) and the event bus (for live events), providing a seamless subscription
    experience for projections.

    Composed of modular mixins:
    - ManagerRegistryMixin: Subscription registration and pause/resume
    - ManagerLifecycleMixin: Start/stop and graceful shutdown
    - ManagerDispatchMixin: Error handling and health check dispatch
    """

    def __init__(
        self,
        event_store: GlobalEventFeed,
        event_bus: SubscribableEventBus,
        checkpoint_repo: SubscriptionPositions,
        shutdown_timeout: float = 30.0,
        drain_timeout: float = 10.0,
        dlq_repo: DLQRepository | None = None,
        error_handling_config: ErrorHandlingConfig | None = None,
        health_check_config: HealthCheckConfig | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the subscription manager."""
        self.event_store = event_store
        self.event_bus = event_bus
        self.checkpoint_repo = checkpoint_repo
        self._dlq_repo = dlq_repo
        self._running = False

        self._error_handling_config = error_handling_config or ErrorHandlingConfig()
        self._health_check_config = health_check_config or HealthCheckConfig()

        self._error_handlers: dict[str, SubscriptionErrorHandler] = {}
        self._global_error_callbacks: list[ErrorCallback] = []

        self._registry = SubscriptionRegistry()
        self._lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=enable_tracing,
        )
        self._health_provider = HealthCheckProvider(
            registry=self._registry,
            error_handlers=self._error_handlers,
            config=self._health_check_config,
            lock=self._registry.lock,
            handler_circuit_breaker_lookup=self._lifecycle.get_handler_circuit_breaker,
            infra_circuit_breaker_lookup=self._lifecycle.get_infra_circuit_breaker,
        )
        self._pause_resume = PauseResumeController(
            registry=self._registry,
            lifecycle=self._lifecycle,
            enable_tracing=enable_tracing,
        )

        self._shutdown_coordinator = ShutdownCoordinator(
            timeout=shutdown_timeout,
            drain_timeout=drain_timeout,
        )
        self._last_shutdown_result: ShutdownResult | None = None

        self._created_at = datetime.now(UTC)
        self._started_at: datetime | None = None

        self._tracer = tracer or create_tracer("eventsource.subscription_manager", enable_tracing)


__all__ = [
    "SubscriptionManager",
]
