"""Declarative projection base with @handles discovery and tenant filtering."""

from __future__ import annotations

import logging
from uuid import UUID

from eventsource.application.projections.base_checkpoint import CheckpointTrackingProjection
from eventsource.application.projections.base_protocols import (
    TenantFilter,
    UnregisteredEventHandling,
)
from eventsource.application.projections.handlers import HandlerRegistry
from eventsource.application.projections.retry import ProjectionRetryPolicy
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository

logger = logging.getLogger(__name__)


class DeclarativeProjection(CheckpointTrackingProjection):
    """Projection that uses declarative event handlers with the `@handles` decorator.

    Automatically discovers `@handles`-decorated methods, builds `subscribed_to()`,
    and supports static (`UUID`) or dynamic (`Callable[[], UUID | None]`) tenant filtering.
    """

    unregistered_event_handling: UnregisteredEventHandling = "ignore"

    def __init__(
        self,
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
        *,
        retry_policy: ProjectionRetryPolicy | None = None,
        tracer: Tracer | None = None,
        tenant_filter: TenantFilter = None,
    ) -> None:
        """Initialize the declarative projection and discover `@handles` methods."""
        self._handler_registry = HandlerRegistry(
            self,
            require_async=True,
            unregistered_event_handling=self.unregistered_event_handling,  # type: ignore[arg-type]
            validate_on_init=True,
        )
        self._tenant_filter = tenant_filter

        super().__init__(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            retry_policy=retry_policy,
            tracer=tracer,
            enable_tracing=enable_tracing,
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return list of event types this projection handles from `@handles` decorators."""
        return self._handler_registry.get_subscribed_events()

    def _get_tenant_filter_value(self) -> UUID | None:
        """Resolve the current tenant filter value (`UUID` or `None`)."""
        if self._tenant_filter is None:
            return None
        if isinstance(self._tenant_filter, UUID):
            return self._tenant_filter
        return self._tenant_filter()

    def _should_process_event(self, event: DomainEvent) -> bool:
        """Check if event should be processed based on tenant filter."""
        filter_value = self._get_tenant_filter_value()
        if filter_value is None:
            return True

        event_tenant: UUID | None = getattr(event, "tenant_id", None)
        if event_tenant is None:
            return True

        return bool(event_tenant == filter_value)

    async def _process_event(self, event: DomainEvent) -> None:
        """Route event to appropriate handler method with tenant filtering."""
        if not self._should_process_event(event):
            logger.debug(
                "Skipping event %s: tenant %s doesn't match filter %s",
                event.event_id,
                getattr(event, "tenant_id", None),
                self._get_tenant_filter_value(),
                extra={
                    "projection": self._projection_name,
                    "event_id": str(event.event_id),
                    "event_type": type(event).__name__,
                    "event_tenant_id": str(getattr(event, "tenant_id", None)),
                    "filter_tenant_id": str(self._get_tenant_filter_value()),
                },
            )
            return

        handler_info = self._handler_registry.get_handler(type(event))
        if handler_info is None:
            await self._handler_registry.dispatch(event, context=None)
            return

        handler_name = handler_info.handler_name
        with self._tracer.span(
            "eventsource.projection.handler",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_HANDLER_NAME: handler_name,
            },
        ):
            await self._handler_registry.dispatch(event, context=None)


__all__ = ["DeclarativeProjection"]
