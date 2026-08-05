"""Backend-agnostic shared internals for store adapters.

Sibling to `_sql/` (dialect-specific) and `_bus/` (transport-specific): what
lands here is shared by adapters that have no technology in common, because
it is port semantics rather than storage mechanics. Adapters-internal, not
public API -- users get these behaviors through the adapters.
"""

from eventsource.adapters._common.expected_version import check_expected, describe_expected

__all__ = ["check_expected", "describe_expected"]
