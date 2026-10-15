"""Tests for the eventsource.testing module structure and exports."""

import inspect
import types
from uuid import uuid4

import pytest
from pydantic import BaseModel

import eventsource.testing
import eventsource.testing.assertions
import eventsource.testing.bdd
import eventsource.testing.builder
import eventsource.testing.harness
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.testing import (
    AppenderConformance,
    EventAssertions,
    EventBuilder,
    InMemoryTestHarness,
    StreamReaderConformance,
    conformance_ports,
    given_events,
    then_event_count,
    then_event_published,
    then_event_sequence,
    then_no_events_published,
    when_command,
)
from eventsource.testing import (
    __all__ as testing_all,
)


class _TestEvent(DomainEvent):
    aggregate_type: str = "Test"


class _TestState(BaseModel):
    name: str = ""


class _TestAggregate(AggregateRoot[_TestState]):
    aggregate_type: str = "Test"

    def _get_initial_state(self) -> _TestState:
        return _TestState()

    def _apply(self, event: DomainEvent) -> None:
        pass

    def do_nothing(self) -> None:
        pass


class TestModuleImportable:
    def test_module_importable(self) -> None:
        assert eventsource.testing is not None

    def test_builder_module_importable(self) -> None:
        assert eventsource.testing.builder is not None

    def test_harness_module_importable(self) -> None:
        assert eventsource.testing.harness is not None

    def test_assertions_module_importable(self) -> None:
        assert eventsource.testing.assertions is not None

    def test_bdd_module_importable(self) -> None:
        assert eventsource.testing.bdd is not None


class TestClassesImportable:
    def test_event_builder_importable(self) -> None:
        assert EventBuilder is not None

    def test_harness_importable(self) -> None:
        assert InMemoryTestHarness is not None

    def test_assertions_importable(self) -> None:
        assert EventAssertions is not None


class TestBDDHelpersImportable:
    def test_given_events_importable(self) -> None:
        assert given_events is not None

    def test_when_command_importable(self) -> None:
        assert when_command is not None

    def test_then_event_published_importable(self) -> None:
        assert then_event_published is not None

    def test_then_no_events_published_importable(self) -> None:
        assert then_no_events_published is not None

    def test_then_event_sequence_importable(self) -> None:
        assert then_event_sequence is not None

    def test_then_event_count_importable(self) -> None:
        assert then_event_count is not None

    def test_bdd_helpers_all_importable(self) -> None:
        helpers = (
            given_events,
            when_command,
            then_event_published,
            then_no_events_published,
            then_event_sequence,
            then_event_count,
        )
        assert None not in helpers


class TestAllExports:
    def test_all_exports_defined(self) -> None:
        assert testing_all is not None and isinstance(testing_all, list)

    def test_all_contains_no_duplicates_or_stale_names(self) -> None:
        assert len(testing_all) == len(set(testing_all)), "__all__ contains duplicate entries"
        for name in testing_all:
            assert hasattr(eventsource.testing, name), f"'{name}' missing from eventsource.testing"

    def test_all_contains_every_public_export(self) -> None:
        public_attrs = {
            name
            for name in dir(eventsource.testing)
            if not name.startswith("_")
            and not isinstance(getattr(eventsource.testing, name), types.ModuleType)
        }
        assert not (public_attrs - set(testing_all))

    def test_all_exports_are_importable(self) -> None:
        for name in testing_all:
            assert getattr(eventsource.testing, name, None) is not None, f"Export '{name}' missing"


class TestPortConformanceSuitesReExported:
    def test_port_suites_importable_from_testing(self) -> None:
        missing = [n for n in conformance_ports.__all__ if not hasattr(eventsource.testing, n)]
        assert not missing, f"conformance_ports suites not re-exported: {missing}"

    def test_port_suites_are_the_same_objects(self) -> None:
        for name in conformance_ports.__all__:
            assert getattr(eventsource.testing, name) is getattr(conformance_ports, name)

    def test_port_suites_listed_in_all(self) -> None:
        assert not (set(conformance_ports.__all__) - set(testing_all))

    def test_named_suite_import_works(self) -> None:
        assert AppenderConformance is not None and StreamReaderConformance is not None


class TestImplementationStatus:
    def test_event_builder_is_implemented(self) -> None:
        builder = EventBuilder(_TestEvent)
        assert builder is not None and isinstance(builder.build(), _TestEvent)

    def test_harness_is_implemented(self) -> None:
        harness = InMemoryTestHarness()
        assert harness is not None and harness.event_store is not None
        assert harness.event_bus is not None and harness.published_events == []

    def test_assertions_is_implemented(self) -> None:
        assertions = EventAssertions([])
        assert assertions is not None
        assertions.assert_no_events_published()

    @pytest.mark.asyncio
    async def test_given_events_is_implemented(self) -> None:
        await given_events(InMemoryTestHarness(), [])

    def test_when_command_is_implemented(self) -> None:
        assert when_command(_TestAggregate(uuid4()), lambda a: a.do_nothing()) == []

    def test_then_event_published_is_implemented(self) -> None:
        with pytest.raises(AssertionError) as exc_info:
            then_event_published(InMemoryTestHarness(), _TestEvent)
        assert "_TestEvent" in str(exc_info.value)

    def test_then_no_events_published_is_implemented(self) -> None:
        then_no_events_published(InMemoryTestHarness())


class TestTypeAnnotations:
    def test_event_builder_has_type_parameter(self) -> None:
        assert hasattr(EventBuilder, "__class_getitem__")

    def test_bdd_functions_have_type_hints(self) -> None:
        for func in [given_events, when_command, then_event_published, then_no_events_published]:
            sig = inspect.signature(func)
            assert any(p.annotation != inspect.Parameter.empty for p in sig.parameters.values())
