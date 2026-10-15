"""Unit tests for RabbitMQ module exports (ADR-0031)."""

from __future__ import annotations


class TestRabbitMQExportsFromEventsource:
    """Tests for RabbitMQ exports from main eventsource package."""

    def test_rabbitmq_event_bus_exported(self) -> None:
        from eventsource import RabbitMQEventBus

        assert RabbitMQEventBus is not None

    def test_rabbitmq_event_bus_config_exported(self) -> None:
        from eventsource import RabbitMQEventBusConfig

        assert RabbitMQEventBusConfig is not None

    def test_rabbitmq_event_bus_stats_exported(self) -> None:
        from eventsource import RabbitMQEventBusStats

        assert RabbitMQEventBusStats is not None

    def test_rabbitmq_not_available_error_exported(self) -> None:
        from eventsource import RabbitMQNotAvailableError

        assert RabbitMQNotAvailableError is not None
        assert issubclass(RabbitMQNotAvailableError, ImportError)

    def test_rabbitmq_available_constant_exported(self) -> None:
        from eventsource import RABBITMQ_AVAILABLE

        assert isinstance(RABBITMQ_AVAILABLE, bool)

    def test_all_rabbitmq_exports_in_single_import(self) -> None:
        from eventsource import (
            RABBITMQ_AVAILABLE,
            RabbitMQEventBus,
            RabbitMQEventBusConfig,
            RabbitMQEventBusStats,
            RabbitMQNotAvailableError,
        )

        assert RabbitMQEventBus is not None
        assert RabbitMQEventBusConfig is not None
        assert RabbitMQEventBusStats is not None
        assert RabbitMQNotAvailableError is not None
        assert isinstance(RABBITMQ_AVAILABLE, bool)

    def test_rabbitmq_exports_in_eventsource_all(self) -> None:
        import eventsource

        assert "RabbitMQEventBus" in eventsource.__all__
        assert "RabbitMQEventBusConfig" in eventsource.__all__
        assert "RabbitMQEventBusStats" in eventsource.__all__
        assert "RabbitMQNotAvailableError" in eventsource.__all__
        assert "RABBITMQ_AVAILABLE" in eventsource.__all__


class TestRabbitMQNotExportedFromBusFacade:
    """The eventsource.bus package is deleted outright (ADR-0031)."""

    def test_bus_package_removed(self) -> None:
        import importlib

        import pytest

        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("eventsource.bus")


class TestRabbitMQExportsFromDirectModule:
    """Tests for RabbitMQ exports from eventsource.adapters.rabbitmq directly."""

    def test_rabbitmq_event_bus_exported(self) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        assert RabbitMQEventBus is not None

    def test_rabbitmq_event_bus_config_exported(self) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig

        assert RabbitMQEventBusConfig is not None

    def test_rabbitmq_event_bus_stats_exported(self) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBusStats

        assert RabbitMQEventBusStats is not None

    def test_rabbitmq_not_available_error_exported(self) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQNotAvailableError

        assert RabbitMQNotAvailableError is not None
        assert issubclass(RabbitMQNotAvailableError, ImportError)

    def test_rabbitmq_available_constant_exported(self) -> None:
        from eventsource.adapters.rabbitmq import RABBITMQ_AVAILABLE

        assert isinstance(RABBITMQ_AVAILABLE, bool)

    def test_all_rabbitmq_exports_from_direct_module(self) -> None:
        from eventsource.adapters.rabbitmq import (
            RABBITMQ_AVAILABLE,
            RabbitMQEventBus,
            RabbitMQEventBusConfig,
            RabbitMQEventBusStats,
            RabbitMQNotAvailableError,
        )

        assert RabbitMQEventBus is not None
        assert RabbitMQEventBusConfig is not None
        assert RabbitMQEventBusStats is not None
        assert RabbitMQNotAvailableError is not None
        assert isinstance(RABBITMQ_AVAILABLE, bool)

    def test_rabbitmq_exports_in_module_all(self) -> None:
        from eventsource.adapters import rabbitmq

        assert "RabbitMQEventBus" in rabbitmq.__all__
        assert "RabbitMQEventBusConfig" in rabbitmq.__all__
        assert "RabbitMQEventBusStats" in rabbitmq.__all__
        assert "RabbitMQNotAvailableError" in rabbitmq.__all__
        assert "RABBITMQ_AVAILABLE" in rabbitmq.__all__


class TestRabbitMQAvailabilityConstant:
    """Tests for RABBITMQ_AVAILABLE constant behavior."""

    def test_rabbitmq_available_is_true_when_aio_pika_installed(self) -> None:
        from eventsource import RABBITMQ_AVAILABLE

        try:
            import aio_pika  # noqa: F401

            assert RABBITMQ_AVAILABLE is True
        except ImportError:
            assert RABBITMQ_AVAILABLE is False

    def test_rabbitmq_available_consistent_across_imports(self) -> None:
        from eventsource import RABBITMQ_AVAILABLE as AVAILABLE_FROM_MAIN
        from eventsource.adapters.rabbitmq import RABBITMQ_AVAILABLE as AVAILABLE_FROM_MODULE

        assert AVAILABLE_FROM_MAIN == AVAILABLE_FROM_MODULE


class TestRabbitMQExportConsistencyWithRedis:
    """Tests that RabbitMQ exports follow the same pattern as Redis."""

    def test_both_availability_constants_exported(self) -> None:
        from eventsource import RABBITMQ_AVAILABLE, REDIS_AVAILABLE

        assert isinstance(REDIS_AVAILABLE, bool)
        assert isinstance(RABBITMQ_AVAILABLE, bool)

    def test_both_event_bus_classes_exported(self) -> None:
        from eventsource import RabbitMQEventBus, RedisEventBus

        assert RedisEventBus is not None
        assert RabbitMQEventBus is not None

    def test_both_config_classes_exported(self) -> None:
        from eventsource import RabbitMQEventBusConfig, RedisEventBusConfig

        assert RedisEventBusConfig is not None
        assert RabbitMQEventBusConfig is not None

    def test_both_stats_classes_exported(self) -> None:
        from eventsource import RabbitMQEventBusStats, RedisEventBusStats

        assert RedisEventBusStats is not None
        assert RabbitMQEventBusStats is not None

    def test_both_not_available_errors_exported(self) -> None:
        from eventsource import RabbitMQNotAvailableError, RedisNotAvailableError

        assert issubclass(RedisNotAvailableError, ImportError)
        assert issubclass(RabbitMQNotAvailableError, ImportError)
