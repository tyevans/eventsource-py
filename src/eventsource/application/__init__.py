"""Use-case ring: application services composing domain objects with ports.

Deliberately empty at the package level. The use-case ring is large and its
subpackages are independent (`aggregates`, `projections`, `subscriptions`,
`migration`); re-exporting them here would make `import
eventsource.application` pull in all of them, and the public front door is
`eventsource` itself. Import from the subpackage you need.
"""

__all__: list[str] = []
