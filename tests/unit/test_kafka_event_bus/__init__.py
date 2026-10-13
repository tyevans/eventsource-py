"""Barrel export facade for KafkaEventBus unit tests."""

from ._fixtures import (
    OrderHandler,
    SampleKafkaEvent,
    SampleOrderCreated,
    SampleOrderShipped,
    SamplePaymentReceived,
    SampleSubscriber,
    SyncHandler,
    _get_gauge_value,
    _get_histogram_sum,
    _get_metric_attributes,
    _get_metric_value,
    _has_histogram_data,
    event_registry,
    mock_producer,
    sample_event,
)
from .test_kafka_event_bus_config import (
    TestKafkaEventBusConfig,
    TestKafkaEventBusConfigProducerConsumer,
    TestKafkaEventBusConfigSanitization,
)
from .test_kafka_event_bus_core import (
    TestKafkaEventBus,
    TestKafkaEventBusStats,
    TestKafkaNotAvailableError,
)
from .test_kafka_event_bus_gauge_metrics import (
    TestKafkaEventBusGaugeMetrics,
)
from .test_kafka_event_bus_metrics import (
    TestKafkaEventBusCounterMetrics,
    TestKafkaEventBusHistogramMetrics,
    TestKafkaEventBusMetricsConfig,
    TestKafkaEventBusMetricsInfrastructure,
)
from .test_kafka_event_bus_metrics_edge_cases import (
    TestKafkaEventBusMetricsEdgeCases,
)
from .test_kafka_event_bus_security import (
    TestKafkaEventBusConfigSecurity,
    TestKafkaEventBusConfigSSLContext,
)
from .test_kafka_event_bus_subscriber import (
    test_handlers_resolve_when_event_type_field_differs_from_class_name,
    test_publish_sends_all_events_before_awaiting_any,
    test_retry_delay_comes_from_the_shared_policy,
)

__all__ = [
    "OrderHandler",
    "SampleKafkaEvent",
    "SampleOrderCreated",
    "SampleOrderShipped",
    "SamplePaymentReceived",
    "SampleSubscriber",
    "SyncHandler",
    "TestKafkaEventBus",
    "TestKafkaEventBusConfig",
    "TestKafkaEventBusConfigProducerConsumer",  # pragma: allowlist secret
    "TestKafkaEventBusConfigSSLContext",
    "TestKafkaEventBusConfigSanitization",
    "TestKafkaEventBusConfigSecurity",
    "TestKafkaEventBusCounterMetrics",
    "TestKafkaEventBusGaugeMetrics",
    "TestKafkaEventBusHistogramMetrics",
    "TestKafkaEventBusMetricsConfig",
    "TestKafkaEventBusMetricsEdgeCases",
    "TestKafkaEventBusMetricsInfrastructure",
    "TestKafkaEventBusStats",
    "TestKafkaNotAvailableError",
    "_get_gauge_value",
    "_get_histogram_sum",
    "_get_metric_attributes",
    "_get_metric_value",
    "_has_histogram_data",
    "event_registry",
    "mock_producer",
    "sample_event",
    "test_handlers_resolve_when_event_type_field_differs_from_class_name",
    "test_publish_sends_all_events_before_awaiting_any",
    "test_retry_delay_comes_from_the_shared_policy",
]
