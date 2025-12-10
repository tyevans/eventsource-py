# Tutorial Series

Welcome to the eventsource-py tutorial series! This comprehensive guide takes you from complete beginner to advanced practitioner through 21 progressive tutorials.

## Overview

| Attribute | Value |
|-----------|-------|
| **Total Time** | ~20 hours |
| **Tutorials** | 21 |
| **Skill Levels** | Beginner to Advanced |

Whether you're new to event sourcing or looking to master advanced patterns, this series provides hands-on learning with real code examples and exercises. Each tutorial builds on the previous ones, gradually introducing more sophisticated concepts and techniques.

## Prerequisites

Before starting, ensure you have:

- **Python 3.11 or later** - eventsource-py requires Python 3.11+
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
| 1 | [Introduction to Event Sourcing](01-introduction.md) | 45 min | Core concepts, principles, and why event sourcing matters |
| 2 | [Your First Domain Event](02-first-event.md) | 45 min | Creating, registering, and serializing events with Pydantic |
| 3 | [Building Your First Aggregate](03-first-aggregate.md) | 60 min | State management, command methods, and event application |
| 4 | [Event Stores Overview](04-event-stores.md) | 45 min | Persisting and retrieving events with different backends |
| 5 | [Repositories Pattern](05-repositories.md) | 45 min | Abstracting persistence and loading aggregates |

**Phase 1 Total:** ~4 hours

**What You'll Learn:**

- The difference between state-based and event-sourced persistence
- How to model domain events that capture business facts
- Building aggregates that enforce business rules
- Choosing and configuring event stores
- Using repositories for clean aggregate lifecycle management

---

### Phase 2: Core Patterns (Intermediate)

Master essential patterns for production applications. This phase covers the patterns you'll use in every real-world event-sourced system.

| # | Tutorial | Time | Description |
|---|----------|------|-------------|
| 6 | [Building Read Models with Projections](06-projections.md) | 60 min | CQRS pattern and read model creation |
| 7 | [Distributing Events with Event Bus](07-event-bus.md) | 45 min | In-process event distribution and subscribers |
| 8 | [Testing Event-Sourced Applications](08-testing.md) | 60 min | Testing strategies, fixtures, and best practices |
| 9 | [Error Handling and Recovery](09-error-handling.md) | 45 min | Resilient event processing and failure recovery |
| 10 | [Checkpoint Management](10-checkpoints.md) | 45 min | Resumable event processing and position tracking |

**Phase 2 Total:** ~5 hours

**What You'll Learn:**

- Separating read and write models with CQRS
- Creating projections that build query-optimized views
- Using the event bus for loose coupling between components
- Testing aggregates, projections, and event handlers
- Handling errors gracefully without losing events
- Implementing checkpoints for reliable replay

---

### Phase 3: Production Readiness (Intermediate-Advanced)

Prepare your application for production deployment. This phase focuses on the operational aspects of running event-sourced systems.

| # | Tutorial | Time | Description |
|---|----------|------|-------------|
| 11 | [Using PostgreSQL Event Store](11-postgresql.md) | 45 min | Production database setup and configuration |
| 12 | [Using SQLite for Development](12-sqlite.md) | 30 min | Lightweight local development and testing |
| 13 | [Subscription Management](13-subscriptions.md) | 60 min | SubscriptionManager for catch-up and live events |
| 14 | [Optimizing with Snapshotting](14-snapshotting.md) | 60 min | Performance optimization for long-lived aggregates |
| 15 | [Production Deployment Guide](15-production.md) | 60 min | Deployment patterns, scaling, and operations |

**Phase 3 Total:** ~4.5 hours

**What You'll Learn:**

- Setting up PostgreSQL for production workloads
- Using SQLite effectively during development
- Managing subscriptions with automatic catch-up
- When and how to implement snapshotting
- Production deployment patterns and best practices

---

### Phase 4: Advanced Patterns (Advanced)

Master sophisticated patterns for enterprise applications. This phase covers advanced topics for complex, distributed systems.

| # | Tutorial | Time | Description |
|---|----------|------|-------------|
| 16 | [Multi-Tenancy Implementation](16-multi-tenancy.md) | 75 min | SaaS multi-tenant support and isolation strategies |
| 17 | [Using Redis Event Bus](17-redis.md) | 60 min | Distributed events with Redis Streams |
| 18 | [Using Kafka Event Bus](18-kafka.md) | 75 min | High-throughput event streaming with Apache Kafka |
| 19 | [Using RabbitMQ Event Bus](19-rabbitmq.md) | 75 min | AMQP-based event distribution with RabbitMQ |
| 20 | [Observability with OpenTelemetry](20-observability.md) | 75 min | Tracing, metrics, and logging for event systems |
| 21 | [Advanced Aggregate Patterns](21-advanced-aggregates.md) | 120 min | Expert-level patterns and complex domain modeling |

**Phase 4 Total:** ~8 hours

**What You'll Learn:**

- Implementing tenant isolation in multi-tenant systems
- Scaling event distribution with Redis Streams
- Building high-throughput systems with Apache Kafka
- Using RabbitMQ for reliable message delivery
- Adding observability with OpenTelemetry tracing
- Advanced aggregate patterns for complex domains

---

## How to Use This Series

### Recommended Approach

1. **Follow sequentially** - Each tutorial builds on concepts from previous ones
2. **Complete exercises** - Hands-on practice reinforces learning
3. **Reference guides** - Use the guides for deeper exploration of specific topics
4. **Build something** - Apply concepts to your own project as you learn

### Quick Reference

Looking for specific topics? Use these shortcuts:

| Your Goal | Start Here |
|-----------|------------|
| New to event sourcing | [Tutorial 1: Introduction](01-introduction.md) |
| Know the basics, want patterns | [Tutorial 6: Projections](06-projections.md) |
| Ready for production | [Tutorial 11: PostgreSQL](11-postgresql.md) |
| Building distributed systems | [Tutorial 16: Multi-Tenancy](16-multi-tenancy.md) |

### Time Investment

| Phase | Tutorials | Time | Best For |
|-------|-----------|------|----------|
| Phase 1: Foundations | 1-5 | ~4 hours | Everyone - start here |
| Phase 2: Core Patterns | 6-10 | ~5 hours | Building real applications |
| Phase 3: Production | 11-15 | ~4.5 hours | Deploying to production |
| Phase 4: Advanced | 16-21 | ~8 hours | Enterprise and distributed systems |

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

## What You'll Build

Throughout this tutorial series, you'll build progressively more sophisticated applications:

- **Phase 1**: A simple task management system demonstrating core concepts
- **Phase 2**: An order processing system with projections and event distribution
- **Phase 3**: A production-ready application with proper persistence and operations
- **Phase 4**: A multi-tenant SaaS platform with distributed event processing

Each phase builds on the previous one, so you'll see how concepts connect and how real systems evolve.

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
