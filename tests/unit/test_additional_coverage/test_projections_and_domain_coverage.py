"""Additional unit tests for projections, aggregate base, and protocol compliance coverage."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    EventHandlerBase,
    Projection,
    SyncProjection,
)
from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import (
    AsyncEventHandler,
    FlexibleEventSubscriber,
)
from eventsource.ports.handlers import (
    FlexibleEventHandler as EventHandler,
)


class AdditionalTestEvent(DomainEvent):
    aggregate_type: str = "Test"
    data: str = ""


class AnotherCoverageEvent(DomainEvent):
    aggregate_type: str = "Test"
    value: int = 0


class TestProjectionAbstractMethods:
    def test_projection_requires_handle_implementation(self):
        with pytest.raises(TypeError):

            class IncompleteProjection(Projection):
                async def reset(self):
                    pass

            IncompleteProjection()

    def test_sync_projection_requires_both_methods(self):
        with pytest.raises(TypeError):

            class IncompleteSyncProjection(SyncProjection):
                def handle(self, event):
                    pass

            IncompleteSyncProjection()

    def test_event_handler_base_requires_methods(self):
        with pytest.raises(TypeError):

            class IncompleteHandler(EventHandlerBase):
                def can_handle(self, event):
                    return True

            IncompleteHandler()


class TestCheckpointTrackingProjectionDLQFailure:
    @pytest.mark.asyncio
    async def test_dlq_write_failure_is_logged(self):
        class TestProjection(CheckpointTrackingProjection):
            MAX_RETRIES = 1

            def subscribed_to(self):
                return [AdditionalTestEvent]

            async def _process_event(self, event):
                raise RuntimeError("Processing failed")

            async def reset(self):
                pass

        failing_dlq = AsyncMock(
            add_failed_event=AsyncMock(side_effect=Exception("DLQ write failed"))
        )
        projection = TestProjection(
            checkpoint_repo=InMemoryCheckpointRepository(), dlq_repo=failing_dlq
        )
        event = AdditionalTestEvent(aggregate_id=uuid4(), data="test")

        with pytest.raises(RuntimeError, match="Processing failed"):
            await projection.handle(event)

        failing_dlq.add_failed_event.assert_called_once()


class TestDeclarativeProjectionEdgeCases:
    @pytest.mark.asyncio
    async def test_handler_with_self_only(self):
        class SelfOnlyProjection(DeclarativeProjection):
            def __init__(self):
                super().__init__()
                self.events = []

            def subscribed_to(self):
                return [AdditionalTestEvent]

            @handles(AdditionalTestEvent)
            async def on_test_event(self, event: DomainEvent):
                self.events.append(event)

            async def reset(self):
                self.events.clear()

        proj = SelfOnlyProjection()
        await proj.handle(AdditionalTestEvent(aggregate_id=uuid4(), data="test"))
        assert len(proj.events) == 1


class TestAsyncEventHandlerABC:
    def test_can_handle_default_implementation(self):
        class MyHandler(AsyncEventHandler):
            def event_types(self):
                return [AdditionalTestEvent]

            async def handle(self, event):
                pass

        handler = MyHandler()
        assert handler.can_handle(AdditionalTestEvent(aggregate_id=uuid4())) is True
        assert handler.can_handle(AnotherCoverageEvent(aggregate_id=uuid4())) is False


class TestAggregateRootAbstractMethods:
    def test_aggregate_root_requires_apply_implementation(self):
        with pytest.raises(TypeError):

            class IncompleteAggregate(AggregateRoot[dict]):
                def _get_initial_state(self):
                    return {}

            IncompleteAggregate(uuid4())

    def test_aggregate_root_requires_initial_state(self):
        with pytest.raises(TypeError):

            class IncompleteAggregate(AggregateRoot[dict]):
                def _apply(self, event):
                    pass

            IncompleteAggregate(uuid4())


class TestDeclarativeAggregateClass:
    def test_declarative_aggregate_with_no_handlers(self):
        class EmptyAggregate(DeclarativeAggregate[dict]):
            aggregate_type = "EmptyAggregate"
            unregistered_event_handling = "ignore"

            def _get_initial_state(self):
                return {}

        agg = EmptyAggregate(uuid4())
        agg._apply(AdditionalTestEvent(aggregate_id=agg.aggregate_id, data="test"))
        assert agg.state is None or agg.state == {}


class TestProtocolCompliance:
    def test_event_handler_protocol_with_async_method(self):
        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(AsyncHandler(), EventHandler)

    def test_event_subscriber_protocol(self):
        class MySubscriber:
            def subscribed_to(self):
                return [AdditionalTestEvent]

            async def handle(self, event):
                pass

        assert isinstance(MySubscriber(), FlexibleEventSubscriber)
