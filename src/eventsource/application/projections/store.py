"""StoreProjection: a declarative projection that folds the log into one store.

The recurring hazard this exists to remove is a subclass that silently narrows
its parent's constructor. Release 0.10.0 widened the projection bases because
subclasses had been dropping `retry_policy` and `tracer`; that fixed the
library's own layer and left every downstream subclass to re-derive the same
forwarding boilerplate by hand -- and re-deriving it wrong is invisible until
a caller passes an argument the class does not take.

`ProjectionOptions` is the parent's option set named once, so a subclass can
forward it whole (`**options: Unpack[ProjectionOptions]`) without restating a
single parameter name, and without giving up type checking the way
`**kwargs: Any` would. A test pins the TypedDict against
`DeclarativeProjection.__init__`, so the two cannot drift apart in silence.

The store is a type parameter rather than a concrete type: that is what keeps
this in the application ring. A class whose signature names an
`async_sessionmaker` is an adapter, which is why `DatabaseProjection` lives
under `adapters/sql/`.
"""

from typing import TypedDict, Unpack

from eventsource.application.projections.base import (
    DeclarativeProjection,
    TenantFilter,
)
from eventsource.application.projections.retry import ProjectionRetryPolicy
from eventsource.observability import Tracer
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository


class ProjectionOptions(TypedDict, total=False):
    """The full option set `DeclarativeProjection.__init__` accepts.

    Named once so subclasses forward it whole instead of re-listing it. See
    `DeclarativeProjection.__init__` for what each option means -- this is a
    restatement of its signature for forwarding purposes, and a test pins the
    two together.
    """

    checkpoint_repo: ProjectionCheckpoints | None
    dlq_repo: DLQRepository | None
    enable_tracing: bool
    retry_policy: ProjectionRetryPolicy | None
    tracer: Tracer | None
    tenant_filter: TenantFilter


class StoreProjection[TStore](DeclarativeProjection):
    """A declarative projection that maintains exactly one store.

    The store is exposed to handler methods as `self._store`. Subclasses add
    `@handles`-decorated methods and nothing else:

        >>> class OrderProjection(StoreProjection[OrderStore]):
        ...     @handles(OrderCreated)
        ...     async def _on_created(self, _context, event: OrderCreated) -> None:
        ...         await self._store.upsert(event.order)

    A subclass that needs its own constructor parameters forwards the rest
    without naming them:

        >>> class BatchingProjection(StoreProjection[OrderStore]):
        ...     def __init__(
        ...         self,
        ...         store: OrderStore,
        ...         batch_size: int = 100,
        ...         **options: Unpack[ProjectionOptions],
        ...     ) -> None:
        ...         self._batch_size = batch_size
        ...         super().__init__(store, **options)
    """

    def __init__(self, store: TStore, **options: Unpack[ProjectionOptions]) -> None:
        """Initialize the projection with its store.

        Args:
            store: The single store this projection writes to.
            **options: Every option `DeclarativeProjection` accepts, forwarded
                unchanged. See `ProjectionOptions`.
        """
        self._store = store
        super().__init__(**options)
