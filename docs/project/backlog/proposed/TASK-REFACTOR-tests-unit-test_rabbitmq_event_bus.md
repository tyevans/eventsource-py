---
id: REFACTOR-tests-unit-test_rabbitmq_event_bus
title: Refactor and Decompose Legacy File test_rabbitmq_event_bus.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-test_rabbitmq_event_bus: Refactor Legacy File test_rabbitmq_event_bus.py

## Summary
The grandfathered debt file `tests/unit/test_rabbitmq_event_bus.py` contains 11571 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_rabbitmq_event_bus_exchange.py, test_rabbitmq_event_bus_message.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/test_rabbitmq_event_bus/` with submodules:
- `test_rabbitmq_event_bus_exchange.py`: TestRabbitMQExchangeDeclaration, TestDirectExchangeConfig, TestDirectExchangeBinding, TestDirectExchangeWorkQueuePattern, TestDirectExchangeRoutingKeyGeneration, TestFanoutExchangeBroadcastBehavior, TestFanoutExchangeMultipleConsumerGroups, TestFanoutExchangeUseCases, TestFanoutExchangeWithDLQ, TestFanoutExchangeLogging, TestRabbitMQEventBusConfig, TestRabbitMQEventBusConfigEdgeCases, TestRabbitMQEventBusStats, TestRabbitMQAvailability, TestRabbitMQEventBusInit, TestRabbitMQEventBusConnection, TestRabbitMQEventBusContextManager, TestRabbitMQEventBusProperties, TestRabbitMQEventBusUrlSanitization, TestRabbitMQStructuredLogging, TestRabbitMQSubscriptionManagement, TestRabbitMQHandlerNormalization, TestRabbitMQGetHandlerName, TestRabbitMQSubscriptionIntegration, TestRabbitMQQueueDeclaration, TestRabbitMQQueueBinding, TestRabbitMQDLQDeclaration, TestRabbitMQConnectionWithTopology, TestRabbitMQDeclarationErrors, SerializationTestEvent, SerializationOrderCreatedEvent, TestRabbitMQRoutingKey, TestRabbitMQEventSerialization, TestRabbitMQEventDeserialization, TestRabbitMQSerializationRoundTrip, PublishTestEvent, TestRabbitMQPublish, TestRabbitMQPublishSingle, TestRabbitMQPublishMultipleEvents, TestRabbitMQPublishWithDifferentEventTypes, ConsumerTestEvent, TestRabbitMQStartConsuming, TestRabbitMQStopConsuming, TestRabbitMQStartConsumingInBackground, TestRabbitMQDispatchEvent, TestRabbitMQConsumerStatistics, TestRabbitMQHandlerNormalizationEdgeCases, TestRabbitMQGetHandlerNameEdgeCases, TestRabbitMQConsumerLoopErrorHandling, TestRabbitMQModuleAllExports, TestRabbitMQEventBusInterfaceCompliance, TestRabbitMQStatsResetBehavior, TestRabbitMQConfigImmutabilityAfterInit, TestRabbitMQConnectionStateTransitions, TestRabbitMQLoggingIntegration, TestRabbitMQConfigValidation, TestRabbitMQDLQConfigOptions, TestRabbitMQDLQQueueDeclarationWithOptions, TestRabbitMQDLQHelperMethods, TestRabbitMQDLQHelperEdgeCases, TestRetryConfiguration, TestCalculateRetryDelay, TestRepublishForRetry, TestSendToDLQ, TestGetDLQMessages, TestPurgeDLQ, TestRabbitMQReconnectionCallbacks, TestRabbitMQConnectionCloseCallback, TestRabbitMQChannelCloseCallback, TestRabbitMQConnectWithReconnectionCallbacks, TestRabbitMQReconnectionStateTracking, TestGracefulShutdown, TestGracefulShutdownContextManager, TestStopConsumingGracefully, TestDrainInFlight, TestForceDisconnect, TestShutdownError, TestShutdownLogging, TestShutdownConfigDocumentation, TestStatisticsTimingFields, TestGetStatsMethod, TestGetStatsDictMethod, TestResetStatsMethod, TestStatisticsIntegration, TestQueueInfo, TestHealthCheckResult, TestGetQueueInfo, TestHealthCheck, TestOpenTelemetryTracing, TestOpenTelemetryConsumerTracing, TestOpenTelemetryGracefulDegradation, TestBindEventType, TestBindRoutingKey, TestTLSSupport, TestBatchPublishingConfig, TestBatchPublishingStats, TestBatchPublishError, TestPublishBatchMethod, TestPublishMethodBatchOptimization, TestBatchPublishingStatsDict, test_retry_delay_comes_from_the_shared_policy
- `test_rabbitmq_event_bus_message.py`: TestRabbitMQCreateMessage, TestRabbitMQProcessMessage, TestRabbitMQProcessMessageWithDLQTracking, TestHandleFailedMessage, TestProcessMessageWithRetry, TestDLQMessageDataclass, TestGetDLQMessageCount, TestReplayDLQMessage, TestReplayMessage, TestDLQMessageExport

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/test_rabbitmq_event_bus.py (11571 lines):
  Submodule 'test_rabbitmq_event_bus_exchange.py' (~10117 lines):
    - [class] TestRabbitMQExchangeDeclaration (lines 1723-2003)
    - [class] TestDirectExchangeConfig (lines 9731-9801)
    - [class] TestDirectExchangeBinding (lines 9804-9932)
    - [class] TestDirectExchangeWorkQueuePattern (lines 10091-10187)
    - [class] TestDirectExchangeRoutingKeyGeneration (lines 10190-10229)
    - [class] TestFanoutExchangeBroadcastBehavior (lines 10237-10384)
    - [class] TestFanoutExchangeMultipleConsumerGroups (lines 10387-10486)
    - [class] TestFanoutExchangeUseCases (lines 10489-10553)
    - [class] TestFanoutExchangeWithDLQ (lines 10556-10610)
    - [class] TestFanoutExchangeLogging (lines 10613-10700)
    - [class] TestRabbitMQEventBusConfig (lines 38-330)
    - [class] TestRabbitMQEventBusConfigEdgeCases (lines 333-384)
    - [class] TestRabbitMQEventBusStats (lines 387-556)
    - [class] TestRabbitMQAvailability (lines 559-619)
    - [class] TestRabbitMQEventBusInit (lines 632-710)
    - [class] TestRabbitMQEventBusConnection (lines 713-927)
    - [class] TestRabbitMQEventBusContextManager (lines 930-1010)
    - [class] TestRabbitMQEventBusProperties (lines 1013-1100)
    - [class] TestRabbitMQEventBusUrlSanitization (lines 1103-1146)
    - [class] TestRabbitMQStructuredLogging (lines 1154-1256)
    - [class] TestRabbitMQSubscriptionManagement (lines 1264-1460)
    - [class] TestRabbitMQHandlerNormalization (lines 1463-1624)
    - [class] TestRabbitMQGetHandlerName (lines 1627-1670)
    - [class] TestRabbitMQSubscriptionIntegration (lines 1673-1715)
    - [class] TestRabbitMQQueueDeclaration (lines 2006-2133)
    - [class] TestRabbitMQQueueBinding (lines 2136-2221)
    - [class] TestRabbitMQDLQDeclaration (lines 2224-2424)
    - [class] TestRabbitMQConnectionWithTopology (lines 2427-2544)
    - [class] TestRabbitMQDeclarationErrors (lines 2547-2602)
    - [class] SerializationTestEvent (lines 2610-2614)
    - [class] SerializationOrderCreatedEvent (lines 2617-2623)
    - [class] TestRabbitMQRoutingKey (lines 2626-2671)
    - [class] TestRabbitMQEventSerialization (lines 2674-2815)
    - [class] TestRabbitMQEventDeserialization (lines 2914-3049)
    - [class] TestRabbitMQSerializationRoundTrip (lines 3052-3131)
    - [class] PublishTestEvent (lines 3139-3143)
    - [class] TestRabbitMQPublish (lines 3146-3498)
    - [class] TestRabbitMQPublishSingle (lines 3501-3582)
    - [class] TestRabbitMQPublishMultipleEvents (lines 3585-3663)
    - [class] TestRabbitMQPublishWithDifferentEventTypes (lines 3666-3713)
    - [class] ConsumerTestEvent (lines 3721-3725)
    - [class] TestRabbitMQStartConsuming (lines 3728-3884)
    - [class] TestRabbitMQStopConsuming (lines 3887-3912)
    - [class] TestRabbitMQStartConsumingInBackground (lines 3915-4005)
    - [class] TestRabbitMQDispatchEvent (lines 4149-4337)
    - [class] TestRabbitMQConsumerStatistics (lines 4340-4472)
    - [class] TestRabbitMQHandlerNormalizationEdgeCases (lines 4480-4542)
    - [class] TestRabbitMQGetHandlerNameEdgeCases (lines 4545-4597)
    - [class] TestRabbitMQConsumerLoopErrorHandling (lines 4600-4694)
    - [class] TestRabbitMQModuleAllExports (lines 4697-4727)
    - [class] TestRabbitMQEventBusInterfaceCompliance (lines 4730-4764)
    - [class] TestRabbitMQStatsResetBehavior (lines 4767-4780)
    - [class] TestRabbitMQConfigImmutabilityAfterInit (lines 4783-4798)
    - [class] TestRabbitMQConnectionStateTransitions (lines 4801-4852)
    - [class] TestRabbitMQLoggingIntegration (lines 4855-4865)
    - [class] TestRabbitMQConfigValidation (lines 4868-4890)
    - [class] TestRabbitMQDLQConfigOptions (lines 4898-4938)
    - [class] TestRabbitMQDLQQueueDeclarationWithOptions (lines 4941-5046)
    - [class] TestRabbitMQDLQHelperMethods (lines 5054-5288)
    - [class] TestRabbitMQDLQHelperEdgeCases (lines 5291-5378)
    - [class] TestRetryConfiguration (lines 5499-5526)
    - [class] TestCalculateRetryDelay (lines 5529-5602)
    - [class] TestRepublishForRetry (lines 5605-5726)
    - [class] TestSendToDLQ (lines 5729-5856)
    - [class] TestGetDLQMessages (lines 6165-6355)
    - [class] TestPurgeDLQ (lines 6793-6886)
    - [class] TestRabbitMQReconnectionCallbacks (lines 6910-7143)
    - [class] TestRabbitMQConnectionCloseCallback (lines 7146-7210)
    - [class] TestRabbitMQChannelCloseCallback (lines 7213-7243)
    - [class] TestRabbitMQConnectWithReconnectionCallbacks (lines 7246-7351)
    - [class] TestRabbitMQReconnectionStateTracking (lines 7354-7408)
    - [class] TestGracefulShutdown (lines 7416-7603)
    - [class] TestGracefulShutdownContextManager (lines 7606-7676)
    - [class] TestStopConsumingGracefully (lines 7679-7745)
    - [class] TestDrainInFlight (lines 7748-7778)
    - [class] TestForceDisconnect (lines 7781-7879)
    - [class] TestShutdownError (lines 7882-7913)
    - [class] TestShutdownLogging (lines 7916-7972)
    - [class] TestShutdownConfigDocumentation (lines 7975-7996)
    - [class] TestStatisticsTimingFields (lines 8004-8044)
    - [class] TestGetStatsMethod (lines 8047-8069)
    - [class] TestGetStatsDictMethod (lines 8072-8199)
    - [class] TestResetStatsMethod (lines 8202-8283)
    - [class] TestStatisticsIntegration (lines 8286-8392)
    - [class] TestQueueInfo (lines 8400-8465)
    - [class] TestHealthCheckResult (lines 8468-8559)
    - [class] TestGetQueueInfo (lines 8567-8753)
    - [class] TestHealthCheck (lines 8761-9097)
    - [class] TestOpenTelemetryTracing (lines 9105-9408)
    - [class] TestOpenTelemetryConsumerTracing (lines 9411-9633)
    - [class] TestOpenTelemetryGracefulDegradation (lines 9636-9723)
    - [class] TestBindEventType (lines 9935-9995)
    - [class] TestBindRoutingKey (lines 9998-10088)
    - [class] TestTLSSupport (lines 10703-11159)
    - [class] TestBatchPublishingConfig (lines 11167-11188)
    - [class] TestBatchPublishingStats (lines 11191-11206)
    - [class] TestBatchPublishError (lines 11209-11249)
    - [class] TestPublishBatchMethod (lines 11252-11439)
    - [class] TestPublishMethodBatchOptimization (lines 11442-11513)
    - [class] TestBatchPublishingStatsDict (lines 11516-11550)
    - [function] test_retry_delay_comes_from_the_shared_policy (lines 11553-11571)
  Submodule 'test_rabbitmq_event_bus_message.py' (~1082 lines):
    - [class] TestRabbitMQCreateMessage (lines 2818-2911)
    - [class] TestRabbitMQProcessMessage (lines 4008-4146)
    - [class] TestRabbitMQProcessMessageWithDLQTracking (lines 5381-5491)
    - [class] TestHandleFailedMessage (lines 5859-5956)
    - [class] TestProcessMessageWithRetry (lines 5959-6084)
    - [class] TestDLQMessageDataclass (lines 6092-6162)
    - [class] TestGetDLQMessageCount (lines 6358-6450)
    - [class] TestReplayDLQMessage (lines 6453-6607)
    - [class] TestReplayMessage (lines 6610-6790)
    - [class] TestDLQMessageExport (lines 6889-6902)
  Suggested barrel exports:
    from .test_rabbitmq_event_bus_exchange import TestRabbitMQExchangeDeclaration, TestDirectExchangeConfig, TestDirectExchangeBinding, TestDirectExchangeWorkQueuePattern, TestDirectExchangeRoutingKeyGeneration, TestFanoutExchangeBroadcastBehavior, TestFanoutExchangeMultipleConsumerGroups, TestFanoutExchangeUseCases, TestFanoutExchangeWithDLQ, TestFanoutExchangeLogging, TestRabbitMQEventBusConfig, TestRabbitMQEventBusConfigEdgeCases, TestRabbitMQEventBusStats, TestRabbitMQAvailability, TestRabbitMQEventBusInit, TestRabbitMQEventBusConnection, TestRabbitMQEventBusContextManager, TestRabbitMQEventBusProperties, TestRabbitMQEventBusUrlSanitization, TestRabbitMQStructuredLogging, TestRabbitMQSubscriptionManagement, TestRabbitMQHandlerNormalization, TestRabbitMQGetHandlerName, TestRabbitMQSubscriptionIntegration, TestRabbitMQQueueDeclaration, TestRabbitMQQueueBinding, TestRabbitMQDLQDeclaration, TestRabbitMQConnectionWithTopology, TestRabbitMQDeclarationErrors, SerializationTestEvent, SerializationOrderCreatedEvent, TestRabbitMQRoutingKey, TestRabbitMQEventSerialization, TestRabbitMQEventDeserialization, TestRabbitMQSerializationRoundTrip, PublishTestEvent, TestRabbitMQPublish, TestRabbitMQPublishSingle, TestRabbitMQPublishMultipleEvents, TestRabbitMQPublishWithDifferentEventTypes, ConsumerTestEvent, TestRabbitMQStartConsuming, TestRabbitMQStopConsuming, TestRabbitMQStartConsumingInBackground, TestRabbitMQDispatchEvent, TestRabbitMQConsumerStatistics, TestRabbitMQHandlerNormalizationEdgeCases, TestRabbitMQGetHandlerNameEdgeCases, TestRabbitMQConsumerLoopErrorHandling, TestRabbitMQModuleAllExports, TestRabbitMQEventBusInterfaceCompliance, TestRabbitMQStatsResetBehavior, TestRabbitMQConfigImmutabilityAfterInit, TestRabbitMQConnectionStateTransitions, TestRabbitMQLoggingIntegration, TestRabbitMQConfigValidation, TestRabbitMQDLQConfigOptions, TestRabbitMQDLQQueueDeclarationWithOptions, TestRabbitMQDLQHelperMethods, TestRabbitMQDLQHelperEdgeCases, TestRetryConfiguration, TestCalculateRetryDelay, TestRepublishForRetry, TestSendToDLQ, TestGetDLQMessages, TestPurgeDLQ, TestRabbitMQReconnectionCallbacks, TestRabbitMQConnectionCloseCallback, TestRabbitMQChannelCloseCallback, TestRabbitMQConnectWithReconnectionCallbacks, TestRabbitMQReconnectionStateTracking, TestGracefulShutdown, TestGracefulShutdownContextManager, TestStopConsumingGracefully, TestDrainInFlight, TestForceDisconnect, TestShutdownError, TestShutdownLogging, TestShutdownConfigDocumentation, TestStatisticsTimingFields, TestGetStatsMethod, TestGetStatsDictMethod, TestResetStatsMethod, TestStatisticsIntegration, TestQueueInfo, TestHealthCheckResult, TestGetQueueInfo, TestHealthCheck, TestOpenTelemetryTracing, TestOpenTelemetryConsumerTracing, TestOpenTelemetryGracefulDegradation, TestBindEventType, TestBindRoutingKey, TestTLSSupport, TestBatchPublishingConfig, TestBatchPublishingStats, TestBatchPublishError, TestPublishBatchMethod, TestPublishMethodBatchOptimization, TestBatchPublishingStatsDict, test_retry_delay_comes_from_the_shared_policy
    from .test_rabbitmq_event_bus_message import TestRabbitMQCreateMessage, TestRabbitMQProcessMessage, TestRabbitMQProcessMessageWithDLQTracking, TestHandleFailedMessage, TestProcessMessageWithRetry, TestDLQMessageDataclass, TestGetDLQMessageCount, TestReplayDLQMessage, TestReplayMessage, TestDLQMessageExport

    __all__ = ["TestRabbitMQExchangeDeclaration", "TestDirectExchangeConfig", "TestDirectExchangeBinding", "TestDirectExchangeWorkQueuePattern", "TestDirectExchangeRoutingKeyGeneration", "TestFanoutExchangeBroadcastBehavior", "TestFanoutExchangeMultipleConsumerGroups", "TestFanoutExchangeUseCases", "TestFanoutExchangeWithDLQ", "TestFanoutExchangeLogging", "TestRabbitMQEventBusConfig", "TestRabbitMQEventBusConfigEdgeCases", "TestRabbitMQEventBusStats", "TestRabbitMQAvailability", "TestRabbitMQEventBusInit", "TestRabbitMQEventBusConnection", "TestRabbitMQEventBusContextManager", "TestRabbitMQEventBusProperties", "TestRabbitMQEventBusUrlSanitization", "TestRabbitMQStructuredLogging", "TestRabbitMQSubscriptionManagement", "TestRabbitMQHandlerNormalization", "TestRabbitMQGetHandlerName", "TestRabbitMQSubscriptionIntegration", "TestRabbitMQQueueDeclaration", "TestRabbitMQQueueBinding", "TestRabbitMQDLQDeclaration", "TestRabbitMQConnectionWithTopology", "TestRabbitMQDeclarationErrors", "SerializationTestEvent", "SerializationOrderCreatedEvent", "TestRabbitMQRoutingKey", "TestRabbitMQEventSerialization", "TestRabbitMQEventDeserialization", "TestRabbitMQSerializationRoundTrip", "PublishTestEvent", "TestRabbitMQPublish", "TestRabbitMQPublishSingle", "TestRabbitMQPublishMultipleEvents", "TestRabbitMQPublishWithDifferentEventTypes", "ConsumerTestEvent", "TestRabbitMQStartConsuming", "TestRabbitMQStopConsuming", "TestRabbitMQStartConsumingInBackground", "TestRabbitMQDispatchEvent", "TestRabbitMQConsumerStatistics", "TestRabbitMQHandlerNormalizationEdgeCases", "TestRabbitMQGetHandlerNameEdgeCases", "TestRabbitMQConsumerLoopErrorHandling", "TestRabbitMQModuleAllExports", "TestRabbitMQEventBusInterfaceCompliance", "TestRabbitMQStatsResetBehavior", "TestRabbitMQConfigImmutabilityAfterInit", "TestRabbitMQConnectionStateTransitions", "TestRabbitMQLoggingIntegration", "TestRabbitMQConfigValidation", "TestRabbitMQDLQConfigOptions", "TestRabbitMQDLQQueueDeclarationWithOptions", "TestRabbitMQDLQHelperMethods", "TestRabbitMQDLQHelperEdgeCases", "TestRetryConfiguration", "TestCalculateRetryDelay", "TestRepublishForRetry", "TestSendToDLQ", "TestGetDLQMessages", "TestPurgeDLQ", "TestRabbitMQReconnectionCallbacks", "TestRabbitMQConnectionCloseCallback", "TestRabbitMQChannelCloseCallback", "TestRabbitMQConnectWithReconnectionCallbacks", "TestRabbitMQReconnectionStateTracking", "TestGracefulShutdown", "TestGracefulShutdownContextManager", "TestStopConsumingGracefully", "TestDrainInFlight", "TestForceDisconnect", "TestShutdownError", "TestShutdownLogging", "TestShutdownConfigDocumentation", "TestStatisticsTimingFields", "TestGetStatsMethod", "TestGetStatsDictMethod", "TestResetStatsMethod", "TestStatisticsIntegration", "TestQueueInfo", "TestHealthCheckResult", "TestGetQueueInfo", "TestHealthCheck", "TestOpenTelemetryTracing", "TestOpenTelemetryConsumerTracing", "TestOpenTelemetryGracefulDegradation", "TestBindEventType", "TestBindRoutingKey", "TestTLSSupport", "TestBatchPublishingConfig", "TestBatchPublishingStats", "TestBatchPublishError", "TestPublishBatchMethod", "TestPublishMethodBatchOptimization", "TestBatchPublishingStatsDict", "test_retry_delay_comes_from_the_shared_policy", "TestRabbitMQCreateMessage", "TestRabbitMQProcessMessage", "TestRabbitMQProcessMessageWithDLQTracking", "TestHandleFailedMessage", "TestProcessMessageWithRetry", "TestDLQMessageDataclass", "TestGetDLQMessageCount", "TestReplayDLQMessage", "TestReplayMessage", "TestDLQMessageExport"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
