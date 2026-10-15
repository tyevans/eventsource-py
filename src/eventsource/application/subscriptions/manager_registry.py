"""Registry and subscription management mixin for SubscriptionManager."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.error_handling import (
    ErrorCallback,
    ErrorHandlingConfig,
    SubscriptionErrorHandler,
)
from eventsource.application.subscriptions.health_provider import HealthCheckProvider
from eventsource.application.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.application.subscriptions.pause_resume import PauseResumeController
from eventsource.application.subscriptions.registry import SubscriptionRegistry
from eventsource.application.subscriptions.subscription import (
    PauseReason,
    Subscription,
    SubscriptionStatus,
)
from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_SUBSCRIPTION_NAME

if TYPE_CHECKING:
    from eventsource.ports.dlq import DLQRepository
    from eventsource.ports.handlers import EventSubscriber

logger = logging.getLogger(__name__)


class ManagerRegistryMixin:
    """Mixin providing registry, lookup, and pause/resume coordination for SubscriptionManager."""

    _registry: SubscriptionRegistry
    _error_handlers: dict[str, SubscriptionErrorHandler]
    _global_error_callbacks: list[ErrorCallback]
    _health_provider: HealthCheckProvider
    _pause_resume: PauseResumeController
    _lifecycle: SubscriptionLifecycleManager
    _error_handling_config: ErrorHandlingConfig
    _dlq_repo: DLQRepository | None
    _tracer: Tracer

    @staticmethod
    def _resolve_name(target: str | Subscription | EventSubscriber) -> str:
        if isinstance(target, str):
            return target
        if isinstance(target, Subscription):
            return target.name
        return target.__class__.__name__

    async def subscribe(
        self,
        subscriber: EventSubscriber,
        config: SubscriptionConfig | None = None,
        name: str | None = None,
    ) -> Subscription:
        """Register a subscriber for event delivery."""
        subscription_name = name or subscriber.__class__.__name__
        with self._tracer.span(
            "eventsource.subscription_manager.subscribe",
            {ATTR_SUBSCRIPTION_NAME: subscription_name},
        ):
            subscription = await self._registry.register(subscriber, config, name)
            error_handler = SubscriptionErrorHandler(
                subscription_name=subscription_name,
                config=self._error_handling_config,
                dlq_repo=self._dlq_repo,
            )
            for callback in self._global_error_callbacks:
                error_handler.on_error(callback)
            self._error_handlers[subscription_name] = error_handler
            self._health_provider.register_subscription(subscription, error_handler)
            return subscription

    async def subscribe_many(
        self,
        *subscribers: EventSubscriber,
        config: SubscriptionConfig | None = None,
    ) -> list[Subscription]:
        """Register multiple subscribers in order and return created subscriptions."""
        return [await self.subscribe(sub, config=config) for sub in subscribers]

    async def unsubscribe(self, target: str | Subscription | EventSubscriber) -> bool:
        """Unregister a subscription by name, Subscription, or EventSubscriber."""
        name = self._resolve_name(target)
        with self._tracer.span(
            "eventsource.subscription_manager.unsubscribe",
            {ATTR_SUBSCRIPTION_NAME: name},
        ):
            coordinator = self._lifecycle.remove_coordinator(name)
            if coordinator:
                await coordinator.stop()
            subscription = await self._registry.unregister(name)
            if subscription is None:
                return False
            self._error_handlers.pop(name, None)
            self._health_provider.unregister_subscription(name)
            return True

    @property
    def subscriptions(self) -> list[Subscription]:
        return self._registry.get_all()

    @property
    def subscription_names(self) -> list[str]:
        return self._registry.get_names()

    @property
    def subscription_count(self) -> int:
        return len(self._registry)

    def get_subscription(self, target: str | Subscription | EventSubscriber) -> Subscription | None:
        return self._registry.get(self._resolve_name(target))

    def get_all_statuses(self) -> dict[str, SubscriptionStatus]:
        return self._registry.get_statuses()

    def __len__(self) -> int:
        return len(self._registry)

    def __bool__(self) -> bool:
        return True

    def __contains__(self, target: str | Subscription | EventSubscriber) -> bool:
        return target in self._registry

    def __iter__(self) -> Iterator[Subscription]:
        return iter(self._registry)

    async def pause_subscription(self, name: str, reason: PauseReason | None = None) -> bool:
        return await self._pause_resume.pause(name, reason)

    async def resume_subscription(self, name: str) -> bool:
        return await self._pause_resume.resume(name)

    async def pause_all(self, reason: PauseReason | None = None) -> dict[str, bool]:
        return await self._pause_resume.pause_all(reason)

    async def resume_all(self) -> dict[str, bool]:
        return await self._pause_resume.resume_all()

    @property
    def paused_subscriptions(self) -> list[Subscription]:
        return self._pause_resume.get_paused()

    @property
    def paused_subscription_names(self) -> list[str]:
        return self._pause_resume.get_paused_names()
