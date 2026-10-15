"""Unit tests for handler registry and signature validation."""

from uuid import uuid4

import pytest

from eventsource.application.projections.handlers import HandlerRegistry
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import EventSourceError, HandlerSignatureError


class TestEvent(DomainEvent):
    aggregate_type: str = "TestAggregate"


class AnotherTestEvent(DomainEvent):
    aggregate_type: str = "TestAggregate"


class _SingleAsyncHandler:
    @handles(TestEvent)
    async def _handle(self, event):
        pass


class _BadThreeParamAsyncHandler:
    @handles(TestEvent)
    async def _handle(self, a, b, c):
        pass


class TestHandlerSignatureError:
    def test_exception_attributes_and_reason(self):
        error = HandlerSignatureError(
            "_handle_order", "OrderProjection", TestEvent, 3, True, "too many parameters"
        )
        assert error.handler_name == "_handle_order" and error.owner_name == "OrderProjection"
        assert error.event_type is TestEvent and error.param_count == 3
        assert error.is_async_required is True and error.reason == "too many parameters"
        assert isinstance(error, EventSourceError)

    def test_exception_message_format_async(self):
        msg = str(HandlerSignatureError("_handle", "MyProjection", TestEvent, 3))
        assert "Handler '_handle' in MyProjection has invalid signature" in msg
        assert "@handles(TestEvent)" in msg and "Expected one of:" in msg and "Hint:" in msg
        assert "async def _handle(self, event: TestEvent) -> None" in msg
        assert "async def _handle(self, context, event: TestEvent) -> None" in msg
        assert "Got: 3 parameter(s) (excluding self)" in msg

    def test_exception_message_format_sync(self):
        error = HandlerSignatureError(
            "_apply", "MyAggregate", TestEvent, 0, is_async_required=False
        )
        message = str(error)
        assert "def _apply(self, event: TestEvent) -> None" in message
        assert "async def" not in message and "Got: 0 parameter(s)" in message


class TestHandlerSignatureValidation:
    def test_too_many_parameters_raises_signature_error(self):
        class BadHandler(_BadThreeParamAsyncHandler):
            pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=True)
        error = exc_info.value
        assert error.handler_name == "_handle" and error.owner_name == "BadHandler"
        assert error.event_type is TestEvent and error.param_count == 3
        assert "Expected one of:" in str(error)
        assert "async def _handle(self, event: TestEvent)" in str(error)
        assert "async def _handle(self, context, event: TestEvent)" in str(error)

    def test_zero_parameters_raises_signature_error(self):
        class BadHandler:
            @handles(TestEvent)
            async def _handle(self):
                pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=True)
        assert exc_info.value.param_count == 0 and "Got: 0 parameter(s)" in str(exc_info.value)

    def test_sync_when_async_required_shows_helpful_message(self):
        class BadHandler:
            @handles(TestEvent)
            def _handle(self, event):
                pass

        with pytest.raises(ValueError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=True)
        message = str(exc_info.value)
        for part in ("must be async", "async def", "Change:", "To:"):
            assert part in message

    def test_valid_single_and_two_param_handlers_accepted(self):
        class GoodTwoParamHandler:
            @handles(TestEvent)
            async def _handle(self, context, event):
                pass

        for owner in (_SingleAsyncHandler(), GoodTwoParamHandler()):
            registry = HandlerRegistry(owner, require_async=True)
            assert registry.has_handler(TestEvent) and registry.handler_count == 1

    def test_sync_handler_valid_when_async_not_required(self):
        class SyncHandler:
            @handles(TestEvent)
            def _handle(self, event):
                pass

        registry = HandlerRegistry(SyncHandler(), require_async=False)
        assert registry.has_handler(TestEvent)
        handler_info = registry.get_handler(TestEvent)
        assert handler_info is not None and handler_info.is_async is False

    def test_signature_error_for_sync_handler_shows_sync_example(self):
        class BadHandler:
            @handles(TestEvent)
            def _handle(self, a, b, c, d):
                pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=False)
        message = str(exc_info.value)
        assert "def _handle(self, event: TestEvent) -> None" in message
        assert "async def _handle" not in message

    def test_multiple_handlers_all_validated(self):
        class MultiHandler(_SingleAsyncHandler):
            @handles(AnotherTestEvent)
            async def _handle_another(self, a, b, c, d):
                pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(MultiHandler(), require_async=True)
        assert exc_info.value.handler_name == "_handle_another"

    def test_validate_on_init_can_be_disabled(self):
        reg = HandlerRegistry(_BadThreeParamAsyncHandler(), require_async=True, validate_on_init=False)  # fmt: skip
        assert reg.has_handler(TestEvent)


class TestHandlerSignatureErrorImport:
    def test_exception_exported_and_caught_programmatically(self):
        from eventsource.domain.exceptions import HandlerSignatureError as ImportedError

        assert ImportedError is HandlerSignatureError
        try:
            HandlerRegistry(_BadThreeParamAsyncHandler(), require_async=True)
            pytest.fail("Expected HandlerSignatureError to be raised")
        except HandlerSignatureError as e:
            assert e.handler_name == "_handle" and e.event_type is TestEvent and e.param_count == 3


class TestHandlerRegistryBasic:
    def test_get_subscribed_events(self):
        class Handler(_SingleAsyncHandler):
            @handles(AnotherTestEvent)
            async def _handle_another(self, event):
                pass

        events = HandlerRegistry(Handler(), require_async=True).get_subscribed_events()
        assert len(events) == 2 and TestEvent in events and AnotherTestEvent in events

    def test_has_and_get_handler_with_class_and_instance(self):
        registry = HandlerRegistry(_SingleAsyncHandler(), require_async=True)
        test_instance = TestEvent(aggregate_id=uuid4())
        other_instance = AnotherTestEvent(aggregate_id=uuid4())
        assert registry.has_handler(TestEvent) and registry.has_handler(test_instance)
        assert not registry.has_handler(AnotherTestEvent) and not registry.has_handler(other_instance)  # fmt: skip

        info_by_cls = registry.get_handler(TestEvent)
        info_by_inst = registry.get_handler(test_instance)
        assert info_by_cls is not None and info_by_inst is info_by_cls
        assert info_by_cls.event_type is TestEvent and info_by_cls.handler_name == "_handle"
        assert info_by_cls.is_async is True and info_by_cls.param_count == 1
        assert registry.get_handler(AnotherTestEvent) is None
        assert registry.get_handler(other_instance) is None

    def test_container_dunders(self):
        empty_registry = HandlerRegistry(object(), require_async=True)
        assert len(empty_registry) == 0 and bool(empty_registry) is True
        assert list(empty_registry) == [] and TestEvent not in empty_registry

        registry = HandlerRegistry(_SingleAsyncHandler(), require_async=True)
        assert len(registry) == 1 and bool(registry) is True and list(registry) == [TestEvent]
        assert TestEvent in registry and TestEvent(aggregate_id=uuid4()) in registry
        assert AnotherTestEvent not in registry and AnotherTestEvent(aggregate_id=uuid4()) not in registry  # fmt: skip

    def test_repr(self):
        class Handler(_SingleAsyncHandler):
            pass

        repr_str = repr(HandlerRegistry(Handler(), require_async=True))
        assert "HandlerRegistry" in repr_str and "Handler" in repr_str and "handlers=1" in repr_str
