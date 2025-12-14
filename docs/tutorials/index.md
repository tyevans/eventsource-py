# Tutorial Series

Welcome to the eventsource-py tutorial series! This comprehensive guide takes you from complete beginner to advanced practitioner through 21 progressive tutorials. Master event sourcing from first principles to production-ready distributed systems.

## Overview

| Attribute | Value |
|-----------|-------|
| **Total Time** | ~20 hours |
| **Tutorials** | 21 |
| **Skill Levels** | Beginner to Advanced |

Whether you're new to event sourcing or looking to master advanced patterns, this series provides hands-on learning with real code examples and exercises. Each tutorial builds on the previous ones, gradually introducing more sophisticated concepts and techniques.

## Prerequisites

Before starting, ensure you have:

- **Python 3.10 or later** - eventsource-py requires Python 3.10+
- **Basic Python knowledge** - Understanding of classes, type hints, and async/await
- **A code editor** - VS Code with Python extensions is recommended
- **Terminal access** - For running commands and examples

### Installation

Install eventsource-py to follow along with the tutorials:

```bash
# Basic installation
pip install eventsource-py

# With PostgreSQL support (recommended for production tutorials)
pip install eventsource-py[postgresql]

# With all optional dependencies
pip install eventsource-py[all]
```

For detailed installation options, see the [Installation Guide](../installation.md).

---

## Learning Path

### Phase 1: Foundations (Beginner)

Build your event sourcing foundation with core concepts. This phase introduces the fundamental building blocks that all event-sourced applications share.

| # | Tutorial | Time | Description |
|---|----------|------|-------------|
| 1 | [Introduction to Event Sourcing](01-introduction.md) | 45 min | Core concepts, real-world examples, and when to use event sourcing |
| 2 | [Your First Domain Event](02-first-event.md) | 45 min | Creating immutable domain events with Pydantic and event registration |
| 3 | [Building Your First Aggregate](03-first-aggregate.md) | 60 min | Aggregates, state management, command methods, and event handlers |
| 4 | [Event Stores Overview](04-event-stores.md) | 45 min | Persisting events, optimistic concurrency, and event streaming |
| 5 | [Repositories Pattern](05-repositories.md) | 45 min | Managing aggregate lifecycle with load-modify-save pattern |

**Phase 1 Total:** ~4 hours

**What You'll Learn:**

- The difference between state-based and event-sourced persistence
- How to model domain events that capture business facts
- Building aggregates that enforce business rules through commands
- Storing and retrieving events with different event store backends
- Using repositories for clean aggregate lifecycle management
- Optimistic concurrency control to prevent conflicts

---

### Phase 2: Core Patterns (Intermediate)

Master essential patterns for production applications. This phase covers the patterns you'll use in every real-world event-sourced system.

| # | Tutorial | Time | Description |
|---|----------|------|-------------|
| 6 | [Building Read Models with Projections](06-projections.md) | 60 min | CQRS pattern, projections, and SubscriptionManager |
| 7 | [Distributing Events with Event Bus](07-event-bus.md) | 45 min | In-process pub/sub with InMemoryEventBus and event subscribers |
| 8 | [Testing Event-Sourced Applications](08-testing.md) | 60 min | Unit tests, integration tests, pytest fixtures, and testing strategies |
| 9 | [Error Handling and Recovery](09-error-handling.md) | 45 min | Exception hierarchy, retry logic, DLQ, and resilient projections |
| 10 | [Checkpoint Management](10-checkpoints.md) | 45 min | Tracking projection progress for resumable event processing |

**Phase 2 Total:** ~5 hours

**What You'll Learn:**

- Separating read and write models with CQRS
- Creating projections that build query-optimized views
- Using the event bus for loose coupling between components
- Testing aggregates, projections, and event handlers comprehensively
- Handling errors gracefully with retry policies and dead letter queues
- Implementing checkpoints for reliable, resumable event processing

---

### Phase 3: Production Readiness (Intermediate-Advanced)

Prepare your application for production deployment. This phase focuses on the operational aspects of running event-sourced systems at scale.

| # | Tutorial | Time | Description |
|---|----------|------|-------------|
| 11 | [Using PostgreSQL Event Store](11-postgresql.md) | 45 min | Production-ready PostgreSQL setup with connection pooling and outbox |
| 12 | [Using SQLite for Development](12-sqlite.md) | 30 min | Lightweight local development and testing with SQLite |
| 13 | [Subscription Management](13-subscriptions.md) | 60 min | Coordinating catch-up and live events with SubscriptionManager |
| 14 | [Optimizing with Snapshotting](14-snapshotting.md) | 60 min | Performance optimization for aggregates with large event histories |
| 15 | [Production Deployment Guide](15-production.md) | 60 min | Health checks, monitoring, graceful shutdown, and production best practices |

**Phase 3 Total:** ~4.5 hours

**What You'll Learn:**

- Setting up PostgreSQL for production event storage
- Using SQLite effectively during development and testing
- Managing subscriptions with automatic catch-up and live event processing
- When and how to implement snapshotting for performance
- Production deployment patterns with health checks and observability
- Graceful shutdown and signal handling

---

### Phase 4: Advanced Patterns (Advanced)

Master sophisticated patterns for enterprise applications. This phase covers advanced topics for complex, distributed, and multi-tenant systems.

| # | Tutorial | Time | Description |
|---|----------|------|-------------|
| 16 | [Multi-Tenancy Implementation](16-multi-tenancy.md) | 75 min | SaaS multi-tenant patterns with tenant isolation and filtering |
| 17 | [Using Redis Event Bus](17-redis.md) | 60 min | Distributed event distribution with Redis Streams and consumer groups |
| 18 | [Using Kafka Event Bus](18-kafka.md) | 75 min | High-throughput event streaming with Apache Kafka partitions |
| 19 | [Using RabbitMQ Event Bus](19-rabbitmq.md) | 75 min | Flexible AMQP-based routing with RabbitMQ exchanges |
| 20 | [Observability with OpenTelemetry](20-observability.md) | 75 min | Distributed tracing, metrics, and logging for event systems |
| 21 | [Advanced Aggregate Patterns](21-advanced-aggregates.md) | 120 min | Value objects, child entities, schema versioning, and complex domain modeling |

**Phase 4 Total:** ~8 hours

**What You'll Learn:**

- Implementing tenant isolation in multi-tenant SaaS applications
- Scaling event distribution with Redis Streams
- Building ultra-high-throughput systems with Apache Kafka
- Using RabbitMQ for flexible message routing patterns
- Adding distributed tracing with OpenTelemetry
- Advanced aggregate patterns for complex domain models

---

## How to Use This Series

### Recommended Approach

1. **Follow sequentially** - Each tutorial builds on concepts from previous ones
2. **Complete exercises** - Hands-on practice reinforces learning
3. **Reference guides** - Use the API docs for deeper exploration of specific topics
4. **Build something** - Apply concepts to your own project as you learn

### Quick Reference

Looking for specific topics? Use these shortcuts:

| Your Goal | Start Here |
|-----------|------------|
| New to event sourcing | [Tutorial 1: Introduction](01-introduction.md) |
| Know the basics, want patterns | [Tutorial 6: Projections](06-projections.md) |
| Ready for production | [Tutorial 11: PostgreSQL](11-postgresql.md) |
| Building distributed systems | [Tutorial 16: Multi-Tenancy](16-multi-tenancy.md) |
| Need observability | [Tutorial 20: OpenTelemetry](20-observability.md) |

### Time Investment

| Phase | Tutorials | Time | Best For |
|-------|-----------|------|----------|
| Phase 1: Foundations | 1-5 | ~4 hours | Everyone - start here |
| Phase 2: Core Patterns | 6-10 | ~5 hours | Building real applications |
| Phase 3: Production | 11-15 | ~4.5 hours | Deploying to production |
| Phase 4: Advanced | 16-21 | ~8 hours | Enterprise and distributed systems |

---

## Complete Tutorial List

### Beginner Tutorials (1-5)

1. **Introduction to Event Sourcing** - Core concepts, real-world examples, benefits vs trade-offs
2. **Your First Domain Event** - DomainEvent base class, @register_event decorator, serialization
3. **Building Your First Aggregate** - AggregateRoot, DeclarativeAggregate, @handles decorator
4. **Event Stores Overview** - InMemoryEventStore, optimistic locking, event streaming
5. **Repositories Pattern** - AggregateRepository, load-modify-save pattern, event publishing

### Intermediate Tutorials (6-10)

6. **Building Read Models with Projections** - EventSubscriber protocol, CQRS, SubscriptionManager
7. **Distributing Events with Event Bus** - InMemoryEventBus, pub/sub, handler registration
8. **Testing Event-Sourced Applications** - pytest fixtures, unit tests, integration tests
9. **Error Handling and Recovery** - Exception hierarchy, retry logic, DLQ, circuit breakers
10. **Checkpoint Management** - CheckpointRepository, position tracking, lag monitoring

### Production Tutorials (11-15)

11. **Using PostgreSQL Event Store** - PostgreSQLEventStore, connection pooling, outbox pattern
12. **Using SQLite for Development** - SQLiteEventStore, in-memory mode, WAL configuration
13. **Subscription Management** - SubscriptionManager, catch-up phase, live phase, lifecycle
14. **Optimizing with Snapshotting** - SnapshotStore implementations, snapshot strategies
15. **Production Deployment Guide** - Health checks, graceful shutdown, monitoring, security

### Advanced Tutorials (16-21)

16. **Multi-Tenancy Implementation** - tenant_id field, tenant isolation, cross-tenant queries
17. **Using Redis Event Bus** - RedisEventBus, Redis Streams, consumer groups, pending messages
18. **Using Kafka Event Bus** - KafkaEventBus, partitions, consumer groups, high throughput
19. **Using RabbitMQ Event Bus** - RabbitMQEventBus, exchanges, routing keys, queue bindings
20. **Observability with OpenTelemetry** - Distributed tracing, metrics, logs, Tracer protocol
21. **Advanced Aggregate Patterns** - Value objects, child entities, schema versioning, complex state

---

## What You'll Build

Throughout this tutorial series, you'll build progressively more sophisticated applications:

- **Phase 1**: A simple task management system demonstrating core event sourcing concepts
- **Phase 2**: An order processing system with projections and event distribution
- **Phase 3**: A production-ready application with PostgreSQL, checkpoints, and snapshots
- **Phase 4**: A multi-tenant SaaS platform with distributed event processing and observability

Each phase builds on the previous one, so you'll see how concepts connect and how real systems evolve.

---

## Related Resources

As you work through the tutorials, you may want to reference these additional resources:

- [Getting Started Guide](../getting-started.md) - Quick introduction and setup
- [Architecture Overview](../architecture.md) - System design and concepts
- [API Reference](../api/index.md) - Detailed API documentation
- [Guides](../guides/subscriptions.md) - Topic-specific deep dives
- [Examples](../examples/basic-order.md) - Complete code examples
- [FAQ](../faq.md) - Frequently asked questions

---

## Getting Help

If you get stuck:

1. Check the [FAQ](../faq.md) for common questions
2. Review the [API Reference](../api/index.md) for detailed documentation
3. Look at the [Examples](../examples/basic-order.md) for working code
4. Open an issue on [GitHub](https://github.com/tyevans/eventsource-py/issues)

---

## Start Your Journey

Ready to begin? Let's start with the fundamentals of event sourcing!

[Start Tutorial 1: Introduction to Event Sourcing](01-introduction.md){ .md-button .md-button--primary }
