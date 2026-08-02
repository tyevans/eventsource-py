"""Adapters to specific backend technologies for the event sourcing interfaces.

Deliberately empty at the package level. Every adapter subpackage guards an
optional driver import, so re-exporting them here would make `import
eventsource.adapters` require every backend to be installed -- the eager
import ADR 0035's lazy front door exists to avoid. Import the backend you
need (`eventsource.adapters.sqlite`, `eventsource.adapters.kafka`, ...) or
take the name from `eventsource` itself.
"""

__all__: list[str] = []
