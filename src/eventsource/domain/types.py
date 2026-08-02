"""
Domain vocabulary type aliases.

These aliases name the identities that flow through DomainEvent and
DomainCommand, and are threaded through those signatures so the published
vocabulary and the real annotations agree. Optionality belongs to the
field that references an identity, never to the identity type itself: an
event's *reference* to a causing event is optional; a causation id is a
UUID.

Positions are deliberately absent: global feed positions are opaque
ordered tokens owned by the adapter -- see eventsource.ports.positions.
"""

from uuid import UUID

# Identity vocabulary
AggregateId = UUID
EventId = UUID
TenantId = UUID
CorrelationId = UUID
CausationId = UUID
