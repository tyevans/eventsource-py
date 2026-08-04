"""
Guard against projection subclasses silently narrowing their parent's constructor.

Every projection base in the tree re-declares its parent's ``__init__``
parameters rather than forwarding ``**kwargs``. That keeps the editor
experience good, but nothing fails when a subclass quietly drops a parameter
the parent accepts -- the narrowing is invisible until a caller passes an
argument the class does not take.

These tests assert the superset property directly: every parameter the parent
accepts must still be accepted by the subclass, and the subclass must actually
forward it (not just swallow it).
"""

import inspect
from unittest.mock import Mock

import pytest

from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
)
from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.application.subscriptions.retry import RetryConfig
from eventsource.observability import create_tracer


def _named_params(cls: type) -> set[str]:
    """Parameter names accepted by ``cls.__init__``, excluding self/*args/**kwargs."""
    return {
        name
        for name, param in inspect.signature(cls.__init__).parameters.items()
        if name != "self"
        and param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    }


# (subclass, parent) pairs along the projection inheritance chain.
_CHAIN = [
    (DeclarativeProjection, CheckpointTrackingProjection),
    (DatabaseProjection, DeclarativeProjection),
    (ReadModelProjection, DatabaseProjection),
]


@pytest.mark.parametrize(
    ("subclass", "parent"),
    _CHAIN,
    ids=lambda c: c.__name__ if isinstance(c, type) else str(c),
)
def test_subclass_constructor_is_superset_of_parent(subclass: type, parent: type) -> None:
    """A subclass may add constructor parameters, never drop them."""
    dropped = _named_params(parent) - _named_params(subclass)
    assert not dropped, (
        f"{subclass.__name__}.__init__ silently drops parameter(s) "
        f"{sorted(dropped)} accepted by {parent.__name__}.__init__"
    )


class _Declarative(DeclarativeProjection):
    pass


class _Database(DatabaseProjection):
    pass


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(lambda **kw: _Declarative(**kw), id="DeclarativeProjection"),
        pytest.param(
            lambda **kw: _Database(session_factory=Mock(), **kw),
            id="DatabaseProjection",
        ),
    ],
)
def test_retry_policy_and_tracer_reach_the_base(factory) -> None:  # type: ignore[no-untyped-def]
    """The forwarded parameters must land on the base, not be swallowed."""
    policy = ExponentialBackoffRetryPolicy(config=RetryConfig(max_retries=7))
    tracer = create_tracer("test", False)

    projection = factory(retry_policy=policy, tracer=tracer)

    assert projection._retry_policy is policy
    assert projection._tracer is tracer


def test_readmodel_projection_forwards_all_parent_params() -> None:
    """ReadModelProjection adds model_class without dropping anything."""
    from eventsource.ports.readmodels.model import ReadModel

    class _Model(ReadModel):
        pass

    class _ReadModel(ReadModelProjection[_Model]):
        pass

    policy = ExponentialBackoffRetryPolicy(config=RetryConfig(max_retries=7))
    tracer = create_tracer("test", False)
    tenant_filter = None

    projection = _ReadModel(
        session_factory=Mock(),
        model_class=_Model,
        retry_policy=policy,
        tracer=tracer,
        tenant_filter=tenant_filter,
    )

    assert projection._retry_policy is policy
    assert projection._tracer is tracer
    assert projection._tenant_filter is tenant_filter
