"""Unit tests for the extracted Kafka metrics module.

Covers ``register_connection_gauge`` / ``register_consumer_lag_gauge`` in
isolation from ``KafkaEventBus`` -- they are pure functions over a mock
meter and a ``KafkaEventBusMetrics`` instance.
"""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("opentelemetry", reason="opentelemetry-sdk not installed")

from eventsource.adapters.kafka.metrics import (  # noqa: E402
    KafkaEventBusMetrics,
    register_connection_gauge,
    register_consumer_lag_gauge,
)


def _make_metrics(meter: MagicMock) -> KafkaEventBusMetrics:
    return KafkaEventBusMetrics(meter)


class TestRegisterConnectionGauge:
    def test_registers_once_with_mock_meter_and_returns_true(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)

        result = register_connection_gauge(meter, metrics, lambda: True, "test-group")

        assert result is True
        meter.create_observable_gauge.assert_called_once()
        call_kwargs = meter.create_observable_gauge.call_args.kwargs
        assert call_kwargs["name"] == "kafka.eventbus.connections.active"
        assert metrics.connection_gauge_registered is True

    def test_meter_none_returns_false_and_registers_nothing(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)

        result = register_connection_gauge(None, metrics, lambda: True, "test-group")

        assert result is False
        meter.create_observable_gauge.assert_not_called()
        assert metrics.connection_gauge_registered is False

    def test_double_call_does_not_double_register(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)

        first = register_connection_gauge(meter, metrics, lambda: True, "test-group")
        second = register_connection_gauge(meter, metrics, lambda: True, "test-group")

        assert first is True
        assert second is True
        meter.create_observable_gauge.assert_called_once()

    def test_callback_reports_is_connected_value(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)
        connected = {"value": False}

        register_connection_gauge(meter, metrics, lambda: connected["value"], "test-group")

        callback = meter.create_observable_gauge.call_args.kwargs["callbacks"][0]
        observations = list(callback(None))
        assert observations[0].value == 0
        assert observations[0].attributes == {"messaging.kafka.consumer_group": "test-group"}

        connected["value"] = True
        observations = list(callback(None))
        assert observations[0].value == 1


class TestRegisterConsumerLagGauge:
    def test_registers_once_with_mock_meter_and_returns_true(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)

        result = register_consumer_lag_gauge(meter, metrics, lambda: iter(()))

        assert result is True
        meter.create_observable_gauge.assert_called_once()
        call_kwargs = meter.create_observable_gauge.call_args.kwargs
        assert call_kwargs["name"] == "kafka.eventbus.consumer.lag"
        assert metrics.lag_gauge_registered is True

    def test_meter_none_returns_false_and_registers_nothing(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)

        result = register_consumer_lag_gauge(None, metrics, lambda: iter(()))

        assert result is False
        meter.create_observable_gauge.assert_not_called()
        assert metrics.lag_gauge_registered is False

    def test_double_call_does_not_double_register(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)

        first = register_consumer_lag_gauge(meter, metrics, lambda: iter(()))
        second = register_consumer_lag_gauge(meter, metrics, lambda: iter(()))

        assert first is True
        assert second is True
        meter.create_observable_gauge.assert_called_once()

    def test_callback_delegates_to_lag_supplier(self) -> None:
        meter = MagicMock()
        metrics = _make_metrics(meter)
        sentinel = object()

        register_consumer_lag_gauge(meter, metrics, lambda: iter((sentinel,)))

        callback = meter.create_observable_gauge.call_args.kwargs["callbacks"][0]
        assert list(callback(None)) == [sentinel]
