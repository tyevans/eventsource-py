"""Compatibility re-export for `EventBusConformanceSuite`.

All port conformance suites live in `eventsource.testing.conformance_ports`.
This module re-exports `EventBusConformanceSuite` from
`eventsource.testing.conformance_ports.bus` for backward compatibility.
"""

from eventsource.testing.conformance_ports.bus import EventBusConformanceSuite

__all__ = ["EventBusConformanceSuite"]
