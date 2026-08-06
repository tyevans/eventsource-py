"""StoreProjection: the store reaches handlers, and the parent's options reach the parent.

The class exists so a downstream projection never restates its parent's
constructor. Two things have to hold for that to be true:

1. ``ProjectionOptions`` names exactly what ``DeclarativeProjection.__init__``
   accepts -- otherwise it is the same silent-narrowing bug one level up,
   with the TypedDict as the narrowing party.
2. A subclass that adds parameters of its own and forwards ``**options`` still
   delivers ``retry_policy``, ``tracer`` and ``tenant_filter`` to the base.
"""

import inspect
from typing import Unpack
from uuid import uuid4

from eventsource.application.projections.base import DeclarativeProjection
from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.application.projections.store import ProjectionOptions, StoreProjection
from eventsource.application.subscriptions.retry import RetryConfig
from eventsource.observability import create_tracer


class _Store:
    """Stand-in for whatever store a consumer folds the log into."""


class _Simple(StoreProjection[_Store]):
    """The common case: a store, handlers, and no constructor at all."""


class _WithOwnParams(StoreProjection[_Store]):
    """The case the class exists for: extra parameters, nothing restated."""

    def __init__(
        self,
        store: _Store,
        batch_size: int = 100,
        **options: Unpack[ProjectionOptions],
    ) -> None:
        self.batch_size = batch_size
        super().__init__(store, **options)


def test_projection_options_matches_the_parent_constructor() -> None:
    """The TypedDict is the parent's option set -- not a subset of it."""
    parent_params = {
        name
        for name, param in inspect.signature(DeclarativeProjection.__init__).parameters.items()
        if name != "self"
        and param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    }

    assert set(ProjectionOptions.__annotations__) == parent_params


def test_store_reaches_the_subclass() -> None:
    store = _Store()

    assert _Simple(store)._store is store


def test_subclass_with_own_params_still_forwards_everything() -> None:
    """Adding a parameter must not cost the parent's options."""
    store = _Store()
    policy = ExponentialBackoffRetryPolicy(config=RetryConfig(max_retries=7))
    tracer = create_tracer("test", False)
    tenant_filter = uuid4()

    projection = _WithOwnParams(
        store,
        batch_size=25,
        retry_policy=policy,
        tracer=tracer,
        tenant_filter=tenant_filter,
    )

    assert projection.batch_size == 25
    assert projection._store is store
    assert projection._retry_policy is policy
    assert projection._tracer is tracer
    assert projection._tenant_filter is tenant_filter


def test_options_reach_the_base_without_a_subclass_constructor() -> None:
    policy = ExponentialBackoffRetryPolicy(config=RetryConfig(max_retries=7))
    tracer = create_tracer("test", False)

    projection = _Simple(_Store(), retry_policy=policy, tracer=tracer, enable_tracing=False)

    assert projection._retry_policy is policy
    assert projection._tracer is tracer
