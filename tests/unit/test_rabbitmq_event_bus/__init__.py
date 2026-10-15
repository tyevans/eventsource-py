"""Barrel export facade for RabbitMQ event bus unit tests."""

from __future__ import annotations

from ._fixtures import (
    ConsumerTestEvent,
    PublishTestEvent,
    SerializationOrderCreatedEvent,
    SerializationTestEvent,
)
from .test_rabbitmq_availability import (
    TestRabbitMQAvailability,
    TestRabbitMQEventBusInit,
    TestRabbitMQEventBusUrlSanitization,
)
from .test_rabbitmq_bindings import TestBindEventType, TestBindRoutingKey
from .test_rabbitmq_config import TestRabbitMQEventBusConfig
from .test_rabbitmq_config_edge_cases import (
    TestRabbitMQConfigImmutabilityAfterInit,
    TestRabbitMQConfigValidation,
    TestRabbitMQEventBusConfigEdgeCases,
)
from .test_rabbitmq_connection import (
    TestRabbitMQConnectionStateTransitions,
    TestRabbitMQEventBusConnection,
)
from .test_rabbitmq_connection_props import (
    TestRabbitMQEventBusContextManager,
    TestRabbitMQEventBusProperties,
)
from .test_rabbitmq_consumption_control import (
    TestRabbitMQStartConsuming,
    TestRabbitMQStartConsumingInBackground,
    TestRabbitMQStopConsuming,
)
from .test_rabbitmq_consumption_dispatch import (
    TestRabbitMQConsumerLoopErrorHandling,
    TestRabbitMQDispatchEvent,
)
from .test_rabbitmq_consumption_stats import TestRabbitMQConsumerStatistics
from .test_rabbitmq_deserialization import TestRabbitMQEventDeserialization
from .test_rabbitmq_dlq_config import (
    TestRabbitMQDLQConfigOptions,
    TestRabbitMQDLQHelperEdgeCases,
    TestRabbitMQDLQQueueDeclarationWithOptions,
)
from .test_rabbitmq_dlq_count import TestGetDLQMessageCount
from .test_rabbitmq_dlq_helpers import TestRabbitMQDLQHelperMethods
from .test_rabbitmq_dlq_messages import TestGetDLQMessages
from .test_rabbitmq_dlq_operations import TestPurgeDLQ, TestSendToDLQ
from .test_rabbitmq_dlq_retry import (
    TestCalculateRetryDelay,
    TestRepublishForRetry,
    TestRetryConfiguration,
    test_retry_delay_comes_from_the_shared_policy,
)
from .test_rabbitmq_exchanges_direct import TestDirectExchangeBinding, TestDirectExchangeConfig
from .test_rabbitmq_exchanges_direct_patterns import (
    TestDirectExchangeRoutingKeyGeneration,
    TestDirectExchangeWorkQueuePattern,
)
from .test_rabbitmq_exchanges_fanout import (
    TestFanoutExchangeBroadcastBehavior,
    TestFanoutExchangeMultipleConsumerGroups,
    TestFanoutExchangeUseCases,
)
from .test_rabbitmq_exchanges_fanout_dlq import TestFanoutExchangeLogging, TestFanoutExchangeWithDLQ
from .test_rabbitmq_force_disconnect import (
    TestDrainInFlight,
    TestForceDisconnect,
    TestShutdownConfigDocumentation,
    TestShutdownError,
    TestShutdownLogging,
)
from .test_rabbitmq_get_queue_info import TestGetQueueInfo
from .test_rabbitmq_handlers import TestRabbitMQGetHandlerName, TestRabbitMQHandlerNormalization
from .test_rabbitmq_handlers_edge_cases import (
    TestRabbitMQGetHandlerNameEdgeCases,
    TestRabbitMQHandlerNormalizationEdgeCases,
)
from .test_rabbitmq_health_check import TestHealthCheck
from .test_rabbitmq_logging_exports import (
    TestRabbitMQEventBusInterfaceCompliance,
    TestRabbitMQLoggingIntegration,
    TestRabbitMQModuleAllExports,
    TestRabbitMQStructuredLogging,
)
from .test_rabbitmq_message_creation import (
    TestDLQMessageDataclass,
    TestDLQMessageExport,
    TestRabbitMQCreateMessage,
)
from .test_rabbitmq_message_processing import (
    TestRabbitMQProcessMessage,
    TestRabbitMQProcessMessageWithDLQTracking,
)
from .test_rabbitmq_message_replay import TestReplayDLQMessage
from .test_rabbitmq_message_replay_raw import TestReplayMessage
from .test_rabbitmq_message_retry import TestHandleFailedMessage, TestProcessMessageWithRetry
from .test_rabbitmq_opentelemetry import TestOpenTelemetryTracing
from .test_rabbitmq_opentelemetry_consumer import (
    TestOpenTelemetryConsumerTracing,
    TestOpenTelemetryGracefulDegradation,
)
from .test_rabbitmq_publish import TestRabbitMQPublish
from .test_rabbitmq_publish_batch import (
    TestBatchPublishError,
    TestBatchPublishingConfig,
    TestBatchPublishingStats,
    TestPublishMethodBatchOptimization,
)
from .test_rabbitmq_publish_batch_method import TestPublishBatchMethod
from .test_rabbitmq_publish_multi import (
    TestRabbitMQPublishMultipleEvents,
    TestRabbitMQPublishSingle,
    TestRabbitMQPublishWithDifferentEventTypes,
)
from .test_rabbitmq_queue_info import TestHealthCheckResult, TestQueueInfo
from .test_rabbitmq_reconnection import (
    TestRabbitMQChannelCloseCallback,
    TestRabbitMQConnectionCloseCallback,
    TestRabbitMQReconnectionCallbacks,
)
from .test_rabbitmq_reconnection_tracking import (
    TestRabbitMQConnectWithReconnectionCallbacks,
    TestRabbitMQReconnectionStateTracking,
)
from .test_rabbitmq_serialization import (
    TestRabbitMQEventSerialization,
    TestRabbitMQRoutingKey,
    TestRabbitMQSerializationRoundTrip,
)
from .test_rabbitmq_shutdown import (
    TestGracefulShutdown,
    TestGracefulShutdownContextManager,
    TestStopConsumingGracefully,
)
from .test_rabbitmq_stats import (
    TestGetStatsMethod,
    TestRabbitMQEventBusStats,
    TestRabbitMQStatsResetBehavior,
    TestStatisticsTimingFields,
)
from .test_rabbitmq_stats_dict import TestBatchPublishingStatsDict, TestGetStatsDictMethod
from .test_rabbitmq_stats_integration import TestResetStatsMethod, TestStatisticsIntegration
from .test_rabbitmq_subscriptions import (
    TestRabbitMQSubscriptionIntegration,
    TestRabbitMQSubscriptionManagement,
)
from .test_rabbitmq_tls_config import TestTLSSupport
from .test_rabbitmq_tls_connection import TestTLSSupportConnection
from .test_rabbitmq_topology_connection import TestRabbitMQConnectionWithTopology
from .test_rabbitmq_topology_dlq import TestRabbitMQDLQDeclaration
from .test_rabbitmq_topology_errors import TestRabbitMQDeclarationErrors
from .test_rabbitmq_topology_exchange import TestRabbitMQExchangeDeclaration
from .test_rabbitmq_topology_queue import TestRabbitMQQueueBinding, TestRabbitMQQueueDeclaration

__all__ = [  # pragma: allowlist secret
    "ConsumerTestEvent",  # pragma: allowlist secret
    "PublishTestEvent",  # pragma: allowlist secret
    "SerializationOrderCreatedEvent",  # pragma: allowlist secret
    "SerializationTestEvent",  # pragma: allowlist secret
    "TestBatchPublishError",  # pragma: allowlist secret
    "TestBatchPublishingConfig",  # pragma: allowlist secret
    "TestBatchPublishingStats",  # pragma: allowlist secret
    "TestBatchPublishingStatsDict",  # pragma: allowlist secret
    "TestBindEventType",  # pragma: allowlist secret
    "TestBindRoutingKey",  # pragma: allowlist secret
    "TestCalculateRetryDelay",  # pragma: allowlist secret
    "TestDLQMessageDataclass",  # pragma: allowlist secret
    "TestDLQMessageExport",  # pragma: allowlist secret
    "TestDirectExchangeBinding",  # pragma: allowlist secret
    "TestDirectExchangeConfig",  # pragma: allowlist secret
    "TestDirectExchangeRoutingKeyGeneration",  # pragma: allowlist secret
    "TestDirectExchangeWorkQueuePattern",  # pragma: allowlist secret
    "TestDrainInFlight",  # pragma: allowlist secret
    "TestFanoutExchangeBroadcastBehavior",  # pragma: allowlist secret
    "TestFanoutExchangeLogging",  # pragma: allowlist secret
    "TestFanoutExchangeMultipleConsumerGroups",  # pragma: allowlist secret
    "TestFanoutExchangeUseCases",  # pragma: allowlist secret
    "TestFanoutExchangeWithDLQ",  # pragma: allowlist secret
    "TestForceDisconnect",  # pragma: allowlist secret
    "TestGetDLQMessageCount",  # pragma: allowlist secret
    "TestGetDLQMessages",  # pragma: allowlist secret
    "TestGetQueueInfo",  # pragma: allowlist secret
    "TestGetStatsDictMethod",  # pragma: allowlist secret
    "TestGetStatsMethod",  # pragma: allowlist secret
    "TestGracefulShutdown",  # pragma: allowlist secret
    "TestGracefulShutdownContextManager",  # pragma: allowlist secret
    "TestHandleFailedMessage",  # pragma: allowlist secret
    "TestHealthCheck",  # pragma: allowlist secret
    "TestHealthCheckResult",  # pragma: allowlist secret
    "TestOpenTelemetryConsumerTracing",  # pragma: allowlist secret
    "TestOpenTelemetryGracefulDegradation",  # pragma: allowlist secret
    "TestOpenTelemetryTracing",  # pragma: allowlist secret
    "TestProcessMessageWithRetry",  # pragma: allowlist secret
    "TestPublishBatchMethod",  # pragma: allowlist secret
    "TestPublishMethodBatchOptimization",  # pragma: allowlist secret
    "TestPurgeDLQ",  # pragma: allowlist secret
    "TestQueueInfo",  # pragma: allowlist secret
    "TestRabbitMQAvailability",  # pragma: allowlist secret
    "TestRabbitMQChannelCloseCallback",  # pragma: allowlist secret
    "TestRabbitMQConfigImmutabilityAfterInit",  # pragma: allowlist secret
    "TestRabbitMQConfigValidation",  # pragma: allowlist secret
    "TestRabbitMQConnectWithReconnectionCallbacks",  # pragma: allowlist secret
    "TestRabbitMQConnectionCloseCallback",  # pragma: allowlist secret
    "TestRabbitMQConnectionStateTransitions",  # pragma: allowlist secret
    "TestRabbitMQConnectionWithTopology",  # pragma: allowlist secret
    "TestRabbitMQConsumerLoopErrorHandling",  # pragma: allowlist secret
    "TestRabbitMQConsumerStatistics",  # pragma: allowlist secret
    "TestRabbitMQCreateMessage",  # pragma: allowlist secret
    "TestRabbitMQDLQConfigOptions",  # pragma: allowlist secret
    "TestRabbitMQDLQDeclaration",  # pragma: allowlist secret
    "TestRabbitMQDLQHelperEdgeCases",  # pragma: allowlist secret
    "TestRabbitMQDLQHelperMethods",  # pragma: allowlist secret
    "TestRabbitMQDLQQueueDeclarationWithOptions",  # pragma: allowlist secret
    "TestRabbitMQDeclarationErrors",  # pragma: allowlist secret
    "TestRabbitMQDispatchEvent",  # pragma: allowlist secret
    "TestRabbitMQEventBusConfig",  # pragma: allowlist secret
    "TestRabbitMQEventBusConfigEdgeCases",  # pragma: allowlist secret
    "TestRabbitMQEventBusConnection",  # pragma: allowlist secret
    "TestRabbitMQEventBusContextManager",  # pragma: allowlist secret
    "TestRabbitMQEventBusInit",  # pragma: allowlist secret
    "TestRabbitMQEventBusInterfaceCompliance",  # pragma: allowlist secret
    "TestRabbitMQEventBusProperties",  # pragma: allowlist secret
    "TestRabbitMQEventBusStats",  # pragma: allowlist secret
    "TestRabbitMQEventBusUrlSanitization",  # pragma: allowlist secret
    "TestRabbitMQEventDeserialization",  # pragma: allowlist secret
    "TestRabbitMQEventSerialization",  # pragma: allowlist secret
    "TestRabbitMQExchangeDeclaration",  # pragma: allowlist secret
    "TestRabbitMQGetHandlerName",  # pragma: allowlist secret
    "TestRabbitMQGetHandlerNameEdgeCases",  # pragma: allowlist secret
    "TestRabbitMQHandlerNormalization",  # pragma: allowlist secret
    "TestRabbitMQHandlerNormalizationEdgeCases",  # pragma: allowlist secret
    "TestRabbitMQLoggingIntegration",  # pragma: allowlist secret
    "TestRabbitMQModuleAllExports",  # pragma: allowlist secret
    "TestRabbitMQProcessMessage",  # pragma: allowlist secret
    "TestRabbitMQProcessMessageWithDLQTracking",  # pragma: allowlist secret
    "TestRabbitMQPublish",  # pragma: allowlist secret
    "TestRabbitMQPublishMultipleEvents",  # pragma: allowlist secret
    "TestRabbitMQPublishSingle",  # pragma: allowlist secret
    "TestRabbitMQPublishWithDifferentEventTypes",  # pragma: allowlist secret
    "TestRabbitMQQueueBinding",  # pragma: allowlist secret
    "TestRabbitMQQueueDeclaration",  # pragma: allowlist secret
    "TestRabbitMQReconnectionCallbacks",  # pragma: allowlist secret
    "TestRabbitMQReconnectionStateTracking",  # pragma: allowlist secret
    "TestRabbitMQRoutingKey",  # pragma: allowlist secret
    "TestRabbitMQSerializationRoundTrip",  # pragma: allowlist secret
    "TestRabbitMQStartConsuming",  # pragma: allowlist secret
    "TestRabbitMQStartConsumingInBackground",  # pragma: allowlist secret
    "TestRabbitMQStatsResetBehavior",  # pragma: allowlist secret
    "TestRabbitMQStopConsuming",  # pragma: allowlist secret
    "TestRabbitMQStructuredLogging",  # pragma: allowlist secret
    "TestRabbitMQSubscriptionIntegration",  # pragma: allowlist secret
    "TestRabbitMQSubscriptionManagement",  # pragma: allowlist secret
    "TestReplayDLQMessage",  # pragma: allowlist secret
    "TestReplayMessage",  # pragma: allowlist secret
    "TestRepublishForRetry",  # pragma: allowlist secret
    "TestResetStatsMethod",  # pragma: allowlist secret
    "TestRetryConfiguration",  # pragma: allowlist secret
    "TestSendToDLQ",  # pragma: allowlist secret
    "TestShutdownConfigDocumentation",  # pragma: allowlist secret
    "TestShutdownError",  # pragma: allowlist secret
    "TestShutdownLogging",  # pragma: allowlist secret
    "TestStatisticsIntegration",  # pragma: allowlist secret
    "TestStatisticsTimingFields",  # pragma: allowlist secret
    "TestStopConsumingGracefully",  # pragma: allowlist secret
    "TestTLSSupport",  # pragma: allowlist secret
    "TestTLSSupportConnection",  # pragma: allowlist secret
    "test_retry_delay_comes_from_the_shared_policy",  # pragma: allowlist secret
]
