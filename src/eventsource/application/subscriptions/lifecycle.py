"""Subscription lifecycle management for coordinating start/stop operations."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from eventsource.observability import Tracer, create_tracer
from eventsource.observability import attributes as attr
from eventsource.ports.exceptions import SubscriptionError, TransitionError

from .subscription import Subscription, SubscriptionState, render_position
from .transition import StartFromResolver, TransitionCoordinator

logger = logging.getLogger(__name__)


class SubscriptionLifecycleManager:
    """Manages subscription start, stop, and catch-up-to-live transition operations."""

    def __init__(
        self,
        event_store: Any,
        event_bus: Any,
        checkpoint_repo: Any,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        leader_elector: Any = None,
    ) -> None:
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self.event_store, self.event_bus = event_store, event_bus
        self.checkpoint_repo, self.leader_elector = checkpoint_repo, leader_elector
        self._coordinators: dict[str, TransitionCoordinator] = {}
        self._start_resolver = StartFromResolver(event_store, checkpoint_repo)

    async def verify_leadership_lease(self) -> bool:
        if self.leader_elector is None:
            return True
        try:
            return bool(await self.leader_elector.renew() and self.leader_elector.is_leader)
        except Exception as e:
            extra = {"identity": self.leader_elector.identity, "error": str(e)}
            logger.error("Leadership lease renewal failed", extra=extra, exc_info=True)
            return False

    async def start_subscription(self, subscription: Subscription) -> None:
        name = subscription.name
        span_attrs = {attr.ATTR_SUBSCRIPTION_NAME: name}
        with self._tracer.span("eventsource.lifecycle.start_subscription", span_attrs) as span:
            try:
                if self.leader_elector is not None and not await self.verify_leadership_lease():
                    ident = self.leader_elector.identity
                    msg = f"Instance '{ident}' does not hold a valid leadership lease; "
                    raise TransitionError(
                        f"{msg}unauthorized cluster transition blocked for subscription '{name}'"
                    )
                start_pos = await self._start_resolver.resolve(subscription)
                subscription.last_processed_position = start_pos
                pos_token = render_position(start_pos)
                if span and pos_token is not None:
                    span.set_attribute(attr.ATTR_POSITION, pos_token)
                logger.info("Starting subscription", extra={"subscription": name, "start_position": pos_token})  # fmt: skip
                coord = TransitionCoordinator(self.event_store, self.event_bus, self.checkpoint_repo, subscription)  # fmt: skip
                self._coordinators[name] = coord
                result = await coord.execute()
                if not result.success:
                    await subscription.set_error(result.error or SubscriptionError("Transition failed"))  # fmt: skip
                    raise SubscriptionError(f"Subscription {name} failed to transition: {result.error}")  # fmt: skip
                if span:
                    span.set_attribute(attr.ATTR_EVENTS_PROCESSED, result.catchup_events_processed)
                ok_extra = {
                    "subscription": name,
                    "catchup_events": result.catchup_events_processed,
                    "buffer_events": result.buffer_events_processed,
                    "final_position": render_position(result.final_position),
                }
                logger.info("Subscription started successfully", extra=ok_extra)
            except Exception as e:
                logger.error("Failed to start subscription", extra={"subscription": name, "error": str(e)}, exc_info=True)  # fmt: skip
                await subscription.set_error(e)
                raise

    async def start_subscription_isolated(self, subscription: Subscription) -> Exception | None:
        try:
            await self.start_subscription(subscription)
            return None
        except Exception as e:
            return e

    async def start_all(self, subscriptions: list[Subscription], concurrent: bool = True) -> dict[str, Exception | None]:  # fmt: skip
        logger.info("Starting subscriptions", extra={"subscription_count": len(subscriptions), "concurrent": concurrent})  # fmt: skip
        results: dict[str, Exception | None] = {}
        if concurrent and len(subscriptions) > 1:
            coro = self.start_subscription_isolated
            tasks = [(s.name, asyncio.create_task(coro(s), name=f"start_{s.name}")) for s in subscriptions]  # fmt: skip
            for name, task in tasks:
                try:
                    results[name] = await task
                except Exception as e:
                    results[name] = e
                    logger.error("Unexpected error starting subscription", extra={"subscription": name, "error": str(e)}, exc_info=True)  # fmt: skip
        else:
            for sub in subscriptions:
                results[sub.name] = await self.start_subscription_isolated(sub)
        succeeded = sum(1 for r in results.values() if r is None)
        if failed := sum(1 for r in results.values() if r is not None):
            logger.warning("Some subscriptions failed to start", extra={"succeeded": succeeded, "failed": failed})  # fmt: skip
        else:
            logger.info("All subscriptions started successfully", extra={"count": succeeded})
        return results

    async def stop_subscription(self, name: str, timeout: float = 30.0) -> None:
        with self._tracer.span("eventsource.lifecycle.stop_subscription", {attr.ATTR_SUBSCRIPTION_NAME: name}):  # fmt: skip
            if (coordinator := self._coordinators.get(name)) is None:
                return
            try:
                await asyncio.wait_for(coordinator.stop(), timeout=timeout)
                logger.info("Subscription stopped", extra={"subscription": name})
            except TimeoutError:
                logger.warning("Subscription stop timed out", extra={"subscription": name, "timeout": timeout})  # fmt: skip

    async def stop_all(self, subscriptions: list[Subscription], timeout: float = 30.0) -> None:
        logger.info("Stopping subscriptions", extra={"subscription_count": len(subscriptions), "timeout": timeout})  # fmt: skip
        if active := [s.name for s in subscriptions if s.name in self._coordinators]:
            await asyncio.gather(*(self.stop_subscription(n, timeout) for n in active), return_exceptions=True)  # fmt: skip
        for sub in subscriptions:
            if not sub.is_terminal:
                await sub.transition_to(SubscriptionState.STOPPED)
        logger.info("Subscriptions stopped")

    def get_coordinator(self, name: str) -> TransitionCoordinator | None:
        return self._coordinators.get(name)

    def get_handler_circuit_breaker(self, name: str) -> Any:
        return coord.handler_circuit_breaker if (coord := self.get_coordinator(name)) else None

    def get_infra_circuit_breaker(self, name: str) -> Any:
        return coord.infra_circuit_breaker if (coord := self.get_coordinator(name)) else None

    def remove_coordinator(self, name: str) -> TransitionCoordinator | None:
        return self._coordinators.pop(name, None)

    coordinator_names: Any = property(lambda self: list(self._coordinators.keys()))
    coordinators: Any = property(lambda self: self._coordinators)


__all__ = ["SubscriptionLifecycleManager"]
