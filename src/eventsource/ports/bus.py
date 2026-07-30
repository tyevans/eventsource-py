# TRANSITION: alias module. The event publisher contract is unchanged by
# the ports spec; this re-exports the existing implementation from
# eventsource.stores.interface until sub-project 3 physically relocates it
# into this package.
"""Event publisher port re-exports."""

from eventsource.stores.interface import EventPublisher

__all__ = ["EventPublisher"]
