"""Shared `ExpectedVersion` dispatch for store adapters.

Every store adapter has to answer the same question before it appends: does
the stream's current version satisfy what the caller asserted? The answer is
port semantics, not storage semantics -- it depends on `ExpectedVersion` and
`OptimisticLockError` and on nothing a backend knows -- so a backend that
re-derived it could only ever diverge from its siblings.

Which is what happened. Four adapters carried this function verbatim, and the
`read_category` batch-timestamp tie-break showed what that invites: a rule
asserted by the conformance suite, implemented independently N times, with
nothing failing when one copy drifts.

**A behavior the conformance suite asserts is implemented once.** If you find
yourself copying a function between adapters because "they all need it," it
belongs here (or in `_sql`/`_bus` if it is dialect- or transport-specific).
"""

from eventsource.domain import StreamId
from eventsource.domain.exceptions import OptimisticLockError
from eventsource.ports import ExpectedVersion


def check_expected(current: int, expected: ExpectedVersion, stream: StreamId) -> None:
    """Raise `OptimisticLockError` unless `current` satisfies `expected`.

    A stream that does not exist is version 0, which is what makes
    `no_stream` and `stream_exists` a comparison against 0 rather than a
    separate existence query.

    Args:
        current: The stream's version now, 0 if it does not exist.
        expected: What the caller asserted the version would be.
        stream: The stream being appended to, for the error's `aggregate_id`.

    Raises:
        OptimisticLockError: If the assertion does not hold.
        ValueError: If `expected.kind` is not a known kind -- unreachable
            through the public constructors, and a bug rather than a
            concurrency conflict if it ever fires.
    """
    if expected.kind == "any":
        return
    if expected.kind == "no_stream":
        if current != 0:
            raise OptimisticLockError(stream.aggregate_id, "no_stream", current)
        return
    if expected.kind == "stream_exists":
        if current == 0:
            raise OptimisticLockError(stream.aggregate_id, "stream_exists", current)
        return
    if expected.kind == "exact":
        if current != expected.version:
            raise OptimisticLockError(stream.aggregate_id, expected.version or 0, current)
        return
    raise ValueError(f"unknown ExpectedVersion kind: {expected.kind!r}")


def describe_expected(expected: ExpectedVersion) -> int | str:
    """What the caller asked for, as `OptimisticLockError` should report it.

    A numeric version renders as a number; the non-numeric kinds render by
    name, because reporting `no_stream` as the integer `0` claims the caller
    expected a version they never wrote.

    Used by the adapters whose unique-constraint violation is caught after
    the fact, where the conflicting version is known only once the insert
    fails and the pre-check's own error is not available to re-raise.
    """
    if expected.kind == "exact":
        return expected.version or 0
    return expected.kind


__all__ = ["check_expected", "describe_expected"]
