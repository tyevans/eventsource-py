"""
Protocols for migration persistence.

Defines persistence contracts for live-migration use cases in
`eventsource.application.migration`. Colocated in ports ring without
ORM/SQL dependencies. Concrete implementations live in adapters.
"""

from __future__ import annotations

from eventsource.ports.migration.audit import MigrationAuditLogRepository
from eventsource.ports.migration.repositories.migration import MigrationRepository
from eventsource.ports.migration.repositories.position import PositionMappingRepository
from eventsource.ports.migration.repositories.routing import TenantRoutingRepository

__all__ = [
    "MigrationAuditLogRepository",
    "MigrationRepository",
    "PositionMappingRepository",
    "TenantRoutingRepository",
]
