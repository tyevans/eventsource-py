"""
Tests for the eventsource.testing module structure.

These tests verify that the testing module is properly structured,
all exports are available, and the module can be imported without errors.
"""

import pytest


class TestModuleImportable:
    """Tests that the testing module and submodules are importable."""

    def test_module_importable(self) -> None:
        """Test that the testing module can be imported."""
        import eventsource.testing

        assert eventsource.testing is not None

    def test_builder_module_importable(self) -> None:
        """Test that the builder submodule can be imported."""
        import eventsource.testing.builder

        assert eventsource.testing.builder is not None

    def test_harness_module_importable(self) -> None:
        """Test that the harness submodule can be imported."""
        import eventsource.testing.harness

        assert eventsource.testing.harness is not None

    def test_assertions_module_importable(self) -> None:
        """Test that the assertions submodule can be imported."""
        import eventsource.testing.assertions

        assert eventsource.testing.assertions is not None

    def test_bdd_module_importable(self) -> None:
        """Test that the bdd submodule can be imported."""
        import eventsource.testing.bdd

        assert eventsource.testing.bdd is not None


class TestClassesImportable:
    """Tests that all classes can be imported from the testing module."""

    def test_event_builder_importable(self) -> None:
        """Test EventBuilder can be imported."""
        from eventsource.testing import EventBuilder

        assert EventBuilder is not None

    def test_harness_importable(self) -> None:
        """Test InMemoryTestHarness can be imported."""
        from eventsource.testing import InMemoryTestHarness

        assert InMemoryTestHarness is not None

    def test_assertions_importable(self) -> None:
        """Test EventAssertions can be imported."""
        from eventsource.testing import EventAssertions

        assert EventAssertions is not None


class TestBDDHelpersImportable:
    """Tests that all BDD helpers can be imported from the testing module."""

    def test_given_events_importable(self) -> None:
        """Test given_events can be imported."""
        from eventsource.testing import given_events

        assert given_events is not None

    def test_when_command_importable(self) -> None:
        """Test when_command can be imported."""
        from eventsource.testing import when_command

        assert when_command is not None

    def test_then_event_published_importable(self) -> None:
        """Test then_event_published can be imported."""
        from eventsource.testing import then_event_published

        assert then_event_published is not None

    def test_then_no_events_published_importable(self) -> None:
        """Test then_no_events_published can be imported."""
        from eventsource.testing import then_no_events_published

        assert then_no_events_published is not None

    def test_then_event_sequence_importable(self) -> None:
        """Test then_event_sequence can be imported."""
        from eventsource.testing import then_event_sequence

        assert then_event_sequence is not None

    def test_then_event_count_importable(self) -> None:
        """Test then_event_count can be imported."""
        from eventsource.testing import then_event_count

        assert then_event_count is not None

    def test_bdd_helpers_all_importable(self) -> None:
        """Test all BDD helpers can be imported together."""
        from eventsource.testing import (
            given_events,
            then_event_count,
            then_event_published,
            then_event_sequence,
            then_no_events_published,
            when_command,
        )

        assert given_events is not None
        assert when_command is not None
        assert then_event_published is not None
        assert then_no_events_published is not None
        assert then_event_sequence is not None
        assert then_event_count is not None


class TestAllExports:
    """Tests that __all__ contains all expected exports."""

    def test_all_exports_defined(self) -> None:
        """Test __all__ is defined in the module."""
        from eventsource.testing import __all__

        assert __all__ is not None
        assert isinstance(__all__, list)

    def test_all_contains_expected_exports(self) -> None:
        """Test __all__ contains expected exports."""
        from eventsource.testing import __all__

        expected = {
            "EventBuilder",
            "InMemoryTestHarness",
            "EventAssertions",
            "given_events",
            "when_command",
            "then_event_published",
            "then_no_events_published",
            "then_event_sequence",
            "then_event_count",
        }
        assert set(__all__) == expected

    def test_all_exports_are_importable(self) -> None:
        """Test that every item in __all__ can be imported."""
        import eventsource.testing
        from eventsource.testing import __all__

        for name in __all__:
            obj = getattr(eventsource.testing, name, None)
            assert obj is not None, f"Export '{name}' is None or missing"


class TestImplementationStatus:
    """Tests that all components are fully implemented."""

    def test_event_builder_is_implemented(self) -> None:
        """Test EventBuilder is fully implemented (DX-002 complete)."""
        from eventsource.events.base import DomainEvent
        from eventsource.testing import EventBuilder

        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "Test"

        # EventBuilder should now be fully implemented (DX-002)
        builder = EventBuilder(TestEvent)
        assert builder is not None
        event = builder.build()
        assert isinstance(event, TestEvent)

    def test_harness_is_implemented(self) -> None:
        """Test InMemoryTestHarness is fully implemented (DX-003 complete)."""
        from eventsource.testing import InMemoryTestHarness

        # InMemoryTestHarness should now be fully implemented (DX-003)
        harness = InMemoryTestHarness()
        assert harness is not None
        assert harness.event_store is not None
        assert harness.event_bus is not None
        assert harness.published_events == []

    def test_assertions_is_implemented(self) -> None:
        """Test EventAssertions is fully implemented (DX-004 complete)."""
        from eventsource.testing import EventAssertions

        # EventAssertions should now be fully implemented (DX-004)
        assertions = EventAssertions([])
        assert assertions is not None
        assertions.assert_no_events_published()

    @pytest.mark.asyncio
    async def test_given_events_is_implemented(self) -> None:
        """Test given_events is fully implemented (DX-004 complete)."""
        from eventsource.testing import InMemoryTestHarness, given_events

        # given_events should now be fully implemented (DX-004)
        harness = InMemoryTestHarness()
        await given_events(harness, [])
        # Should complete without error

    def test_when_command_is_implemented(self) -> None:
        """Test when_command is fully implemented (DX-004 complete)."""
        from uuid import uuid4

        from pydantic import BaseModel

        from eventsource.aggregates.base import AggregateRoot
        from eventsource.events.base import DomainEvent
        from eventsource.testing import when_command

        class TestState(BaseModel):
            name: str = ""

        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "Test"

        class TestAggregate(AggregateRoot[TestState]):
            aggregate_type: str = "Test"

            def _get_initial_state(self) -> TestState:
                return TestState()

            def _apply(self, event: DomainEvent) -> None:
                pass

            def do_nothing(self) -> None:
                pass

        # when_command should now be fully implemented (DX-004)
        agg = TestAggregate(uuid4())
        events = when_command(agg, lambda a: a.do_nothing())
        assert events == []

    def test_then_event_published_is_implemented(self) -> None:
        """Test then_event_published is fully implemented (DX-004 complete)."""

        from eventsource.events.base import DomainEvent
        from eventsource.testing import InMemoryTestHarness, then_event_published

        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "Test"

        harness = InMemoryTestHarness()
        # Should raise AssertionError (not NotImplementedError) when no events
        with pytest.raises(AssertionError) as exc_info:
            then_event_published(harness, TestEvent)

        assert "TestEvent" in str(exc_info.value)

    def test_then_no_events_published_is_implemented(self) -> None:
        """Test then_no_events_published is fully implemented (DX-004 complete)."""
        from eventsource.testing import InMemoryTestHarness, then_no_events_published

        harness = InMemoryTestHarness()
        # Should complete without error (no events published)
        then_no_events_published(harness)


class TestTypeAnnotations:
    """Tests that type annotations are present and valid."""

    def test_event_builder_has_type_parameter(self) -> None:
        """Test EventBuilder is Generic with type parameter."""

        from eventsource.testing import EventBuilder

        # EventBuilder should be Generic
        assert hasattr(EventBuilder, "__class_getitem__")

    def test_bdd_functions_have_type_hints(self) -> None:
        """Test BDD functions have proper type hints."""
        import inspect

        from eventsource.testing import (
            given_events,
            then_event_published,
            then_no_events_published,
            when_command,
        )

        # Check that functions have type hints
        for func in [
            given_events,
            when_command,
            then_event_published,
            then_no_events_published,
        ]:
            sig = inspect.signature(func)
            # At least some parameters should have annotations
            has_annotations = any(
                p.annotation != inspect.Parameter.empty for p in sig.parameters.values()
            )
            assert has_annotations, f"{func.__name__} missing type hints"
