"""Shared connection handling for SQL adapters.

Deliberately distinct from `eventsource.repositories._connection`: that
helper has a different signature (`transactional=`) and different call
sites, and the outbox, read-model, and migration repositories still use
it. The two merge when the outbox slice removes its last non-adapter
caller.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


@asynccontextmanager
async def sql_connection(
    conn: AsyncConnection | AsyncEngine, *, write: bool
) -> AsyncIterator[AsyncConnection]:
    """Yield a connection to execute on.

    A live `AsyncConnection` is yielded directly and NOT committed -- the
    caller owns the transaction. An `AsyncEngine` gets `begin()` for writes
    (committed on successful exit) and `connect()` for reads.
    """
    if isinstance(conn, AsyncEngine):
        if write:
            async with conn.begin() as connection:
                yield connection
        else:
            async with conn.connect() as connection:
                yield connection
    else:
        yield conn


__all__ = ["sql_connection"]
