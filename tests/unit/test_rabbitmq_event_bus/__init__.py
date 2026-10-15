"""Barrel export facade for RabbitMQ event bus unit tests."""
# ruff: noqa: I001

from __future__ import annotations

# fmt: off
from ._fixtures import (
    ConsumerTestEvent, PublishTestEvent, SerializationOrderCreatedEvent, SerializationTestEvent,
)
from .test_rabbitmq_availability import (
    TestRabbitMQAvailability, TestRabbitMQEventBusInit, TestRabbitMQEventBusUrlSanitization,
)
from .test_rabbitmq_bindings import TestBindEventType, TestBindRoutingKey
from .test_rabbitmq_config import TestRabbitMQEventBusConfig
from .test_rabbitmq_config_edge_cases import (
    TestRabbitMQConfigImmutabilityAfterInit, TestRabbitMQConfigValidation,
    TestRabbitMQEventBusConfigEdgeCases,
)
from .test_rabbitmq_connection import (
    TestRabbitMQConnectionStateTransitions, TestRabbitMQEventBusConnection,
)
from .test_rabbitmq_connection_props import (
    TestRabbitMQEventBusContextManager, TestRabbitMQEventBusProperties,
)
from .test_rabbitmq_consumption_control import (
    TestRabbitMQStartConsuming, TestRabbitMQStartConsumingInBackground, TestRabbitMQStopConsuming,
)
from .test_rabbitmq_consumption_dispatch import (
    TestRabbitMQConsumerLoopErrorHandling, TestRabbitMQDispatchEvent,
)
from .test_rabbitmq_consumption_stats import TestRabbitMQConsumerStatistics
from .test_rabbitmq_deserialization import TestRabbitMQEventDeserialization
from .test_rabbitmq_dlq_config import (
    TestRabbitMQDLQConfigOptions, TestRabbitMQDLQHelperEdgeCases,
    TestRabbitMQDLQQueueDeclarationWithOptions,
)
from .test_rabbitmq_dlq_count import TestGetDLQMessageCount
from .test_rabbitmq_dlq_helpers import TestRabbitMQDLQHelperMethods
from .test_rabbitmq_dlq_messages import TestGetDLQMessages
from .test_rabbitmq_dlq_operations import TestPurgeDLQ, TestSendToDLQ
from .test_rabbitmq_dlq_retry import (
    TestCalculateRetryDelay, TestRepublishForRetry, TestRetryConfiguration,
    test_retry_delay_comes_from_the_shared_policy,
)
from .test_rabbitmq_exchanges_direct import TestDirectExchangeBinding, TestDirectExchangeConfig
from .test_rabbitmq_exchanges_direct_patterns import (
    TestDirectExchangeRoutingKeyGeneration, TestDirectExchangeWorkQueuePattern,
)
from .test_rabbitmq_exchanges_fanout import (
    TestFanoutExchangeBroadcastBehavior, TestFanoutExchangeMultipleConsumerGroups,
    TestFanoutExchangeUseCases,
)
from .test_rabbitmq_exchanges_fanout_dlq import TestFanoutExchangeLogging, TestFanoutExchangeWithDLQ
from .test_rabbitmq_force_disconnect import (
    TestDrainInFlight, TestForceDisconnect, TestShutdownConfigDocumentation,
    TestShutdownError, TestShutdownLogging,
)
from .test_rabbitmq_get_queue_info import TestGetQueueInfo
from .test_rabbitmq_handlers import TestRabbitMQGetHandlerName, TestRabbitMQHandlerNormalization
from .test_rabbitmq_handlers_edge_cases import (
    TestRabbitMQGetHandlerNameEdgeCases, TestRabbitMQHandlerNormalizationEdgeCases,
)
from .test_rabbitmq_health_check import TestHealthCheck
from .test_rabbitmq_logging_exports import (
    TestRabbitMQEventBusInterfaceCompliance, TestRabbitMQLoggingIntegration,
    TestRabbitMQModuleAllExports, TestRabbitMQStructuredLogging,
)
from .test_rabbitmq_message_creation import (
    TestDLQMessageDataclass, TestDLQMessageExport, TestRabbitMQCreateMessage,
)
from .test_rabbitmq_message_processing import (
    TestRabbitMQProcessMessage, TestRabbitMQProcessMessageWithDLQTracking,
)
from .test_rabbitmq_message_replay import TestReplayDLQMessage
from .test_rabbitmq_message_replay_raw import TestReplayMessage
from .test_rabbitmq_message_retry import TestHandleFailedMessage, TestProcessMessageWithRetry
from .test_rabbitmq_opentelemetry import TestOpenTelemetryTracing
from .test_rabbitmq_opentelemetry_consumer import (
    TestOpenTelemetryConsumerTracing, TestOpenTelemetryGracefulDegradation,
)
from .test_rabbitmq_publish import TestRabbitMQPublish
from .test_rabbitmq_publish_batch import (
    TestBatchPublishError, TestBatchPublishingConfig,
    TestBatchPublishingStats, TestPublishMethodBatchOptimization,
)
from .test_rabbitmq_publish_batch_method import TestPublishBatchMethod
from .test_rabbitmq_publish_multi import (
    TestRabbitMQPublishMultipleEvents, TestRabbitMQPublishSingle,
    TestRabbitMQPublishWithDifferentEventTypes,
)
from .test_rabbitmq_queue_info import TestHealthCheckResult, TestQueueInfo
from .test_rabbitmq_reconnection import (
    TestRabbitMQChannelCloseCallback, TestRabbitMQConnectionCloseCallback,
    TestRabbitMQReconnectionCallbacks,
)
from .test_rabbitmq_reconnection_tracking import (
    TestRabbitMQConnectWithReconnectionCallbacks, TestRabbitMQReconnectionStateTracking,
)
from .test_rabbitmq_serialization import (
    TestRabbitMQEventSerialization, TestRabbitMQRoutingKey, TestRabbitMQSerializationRoundTrip,
)
from .test_rabbitmq_shutdown import (
    TestGracefulShutdown, TestGracefulShutdownContextManager, TestStopConsumingGracefully,
)
from .test_rabbitmq_stats import (
    TestGetStatsMethod, TestRabbitMQEventBusStats,
    TestRabbitMQStatsResetBehavior, TestStatisticsTimingFields,
)
from .test_rabbitmq_stats_dict import TestBatchPublishingStatsDict, TestGetStatsDictMethod
from .test_rabbitmq_stats_integration import TestResetStatsMethod, TestStatisticsIntegration
from .test_rabbitmq_subscriptions import (
    TestRabbitMQSubscriptionIntegration, TestRabbitMQSubscriptionManagement,
)
from .test_rabbitmq_tls_config import TestTLSSupport
from .test_rabbitmq_tls_connection import TestTLSSupportConnection
from .test_rabbitmq_topology_connection import TestRabbitMQConnectionWithTopology
from .test_rabbitmq_topology_dlq import TestRabbitMQDLQDeclaration
from .test_rabbitmq_topology_errors import TestRabbitMQDeclarationErrors
from .test_rabbitmq_topology_exchange import TestRabbitMQExchangeDeclaration
from .test_rabbitmq_topology_queue import TestRabbitMQQueueBinding, TestRabbitMQQueueDeclaration

__all__ = [  # pragma: allowlist secret
    "ConsumerTestEvent", "PublishTestEvent", "SerializationOrderCreatedEvent",  # pragma: allowlist secret
    "SerializationTestEvent", "TestBatchPublishError", "TestBatchPublishingConfig",  # pragma: allowlist secret
    "TestBatchPublishingStats", "TestBatchPublishingStatsDict", "TestBindEventType",  # pragma: allowlist secret
    "TestBindRoutingKey", "TestCalculateRetryDelay", "TestDLQMessageDataclass",  # pragma: allowlist secret
    "TestDLQMessageExport", "TestDirectExchangeBinding", "TestDirectExchangeConfig",  # pragma: allowlist secret
    "TestDirectExchangeRoutingKeyGeneration", "TestDirectExchangeWorkQueuePattern",  # pragma: allowlist secret
    "TestDrainInFlight", "TestFanoutExchangeBroadcastBehavior", "TestFanoutExchangeLogging",  # pragma: allowlist secret
    "TestFanoutExchangeMultipleConsumerGroups", "TestFanoutExchangeUseCases",  # pragma: allowlist secret
    "TestFanoutExchangeWithDLQ", "TestForceDisconnect", "TestGetDLQMessageCount",  # pragma: allowlist secret
    "TestGetDLQMessages", "TestGetQueueInfo", "TestGetStatsDictMethod", "TestGetStatsMethod",  # pragma: allowlist secret
    "TestGracefulShutdown", "TestGracefulShutdownContextManager", "TestHandleFailedMessage",  # pragma: allowlist secret
    "TestHealthCheck", "TestHealthCheckResult", "TestOpenTelemetryConsumerTracing",  # pragma: allowlist secret
    "TestOpenTelemetryGracefulDegradation", "TestOpenTelemetryTracing",  # pragma: allowlist secret
    "TestProcessMessageWithRetry", "TestPublishBatchMethod", "TestPublishMethodBatchOptimization",  # pragma: allowlist secret
    "TestPurgeDLQ", "TestQueueInfo", "TestRabbitMQAvailability",  # pragma: allowlist secret
    "TestRabbitMQChannelCloseCallback", "TestRabbitMQConfigImmutabilityAfterInit",  # pragma: allowlist secret
    "TestRabbitMQConfigValidation", "TestRabbitMQConnectWithReconnectionCallbacks",  # pragma: allowlist secret
    "TestRabbitMQConnectionCloseCallback", "TestRabbitMQConnectionStateTransitions",  # pragma: allowlist secret
    "TestRabbitMQConnectionWithTopology", "TestRabbitMQConsumerLoopErrorHandling",  # pragma: allowlist secret
    "TestRabbitMQConsumerStatistics", "TestRabbitMQCreateMessage", "TestRabbitMQDLQConfigOptions",  # pragma: allowlist secret
    "TestRabbitMQDLQDeclaration", "TestRabbitMQDLQHelperEdgeCases", "TestRabbitMQDLQHelperMethods",  # pragma: allowlist secret
    "TestRabbitMQDLQQueueDeclarationWithOptions", "TestRabbitMQDeclarationErrors",  # pragma: allowlist secret
    "TestRabbitMQDispatchEvent", "TestRabbitMQEventBusConfig",  # pragma: allowlist secret
    "TestRabbitMQEventBusConfigEdgeCases", "TestRabbitMQEventBusConnection",  # pragma: allowlist secret
    "TestRabbitMQEventBusContextManager", "TestRabbitMQEventBusInit",  # pragma: allowlist secret
    "TestRabbitMQEventBusInterfaceCompliance", "TestRabbitMQEventBusProperties",  # pragma: allowlist secret
    "TestRabbitMQEventBusStats", "TestRabbitMQEventBusUrlSanitization",  # pragma: allowlist secret
    "TestRabbitMQEventDeserialization", "TestRabbitMQEventSerialization",  # pragma: allowlist secret
    "TestRabbitMQExchangeDeclaration", "TestRabbitMQGetHandlerName",  # pragma: allowlist secret
    "TestRabbitMQGetHandlerNameEdgeCases", "TestRabbitMQHandlerNormalization",  # pragma: allowlist secret
    "TestRabbitMQHandlerNormalizationEdgeCases", "TestRabbitMQLoggingIntegration",  # pragma: allowlist secret
    "TestRabbitMQModuleAllExports", "TestRabbitMQProcessMessage",  # pragma: allowlist secret
    "TestRabbitMQProcessMessageWithDLQTracking", "TestRabbitMQPublish",  # pragma: allowlist secret
    "TestRabbitMQPublishMultipleEvents", "TestRabbitMQPublishSingle",  # pragma: allowlist secret
    "TestRabbitMQPublishWithDifferentEventTypes", "TestRabbitMQQueueBinding",  # pragma: allowlist secret
    "TestRabbitMQQueueDeclaration", "TestRabbitMQReconnectionCallbacks",  # pragma: allowlist secret
    "TestRabbitMQReconnectionStateTracking", "TestRabbitMQRoutingKey",  # pragma: allowlist secret
    "TestRabbitMQSerializationRoundTrip", "TestRabbitMQStartConsuming",  # pragma: allowlist secret
    "TestRabbitMQStartConsumingInBackground", "TestRabbitMQStatsResetBehavior",  # pragma: allowlist secret
    "TestRabbitMQStopConsuming", "TestRabbitMQStructuredLogging",  # pragma: allowlist secret
    "TestRabbitMQSubscriptionIntegration", "TestRabbitMQSubscriptionManagement",  # pragma: allowlist secret
    "TestReplayDLQMessage", "TestReplayMessage", "TestRepublishForRetry", "TestResetStatsMethod",  # pragma: allowlist secret
    "TestRetryConfiguration", "TestSendToDLQ", "TestShutdownConfigDocumentation",  # pragma: allowlist secret
    "TestShutdownError", "TestShutdownLogging", "TestStatisticsIntegration",  # pragma: allowlist secret
    "TestStatisticsTimingFields", "TestStopConsumingGracefully", "TestTLSSupport",  # pragma: allowlist secret
    "TestTLSSupportConnection", "test_retry_delay_comes_from_the_shared_policy",  # pragma: allowlist secret
]
# fmt: on
