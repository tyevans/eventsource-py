"""
Connection handling helper for database operations.

This module provides utilities to handle both AsyncEngine and AsyncConnection
inputs consistently, reducing boilerplate code in repository implementations.

The main utility is the `execute_with_connection` async context manager which:
- Handles AsyncEngine inputs by creating a connection or transaction
- Passes through AsyncConnection inputs directly
- Provides consistent error handling and cleanup
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


@asynccontextmanager
async def execute_with_connection(
    conn: AsyncConnection | AsyncEngine,
    transactional: bool = True,
) -> AsyncIterator[AsyncConnection]:
    """
    Context manager for executing database operations.

    Handles both AsyncEngine and AsyncConnection inputs, providing
    a consistent interface for database operations. When an AsyncEngine
    is provided, it creates an appropriate connection context. When an
    AsyncConnection is provided, it yields it directly.

    Args:
        conn: Database connection or engine
        transactional: If True, wrap in transaction (begin).
                       If False, use bare connection (connect).
                       Only applies when conn is an AsyncEngine.

    Yields:
        AsyncConnection ready for execute() calls

    Example:
        >>> async with execute_with_connection(self.conn) as conn:
        ...     await conn.execute(query, params)

        >>> async with execute_with_connection(self.conn, transactional=False) as conn:
        ...     result = await conn.execute(select_query, params)
        ...     return result.fetchall()

    Note:
        - For write operations (INSERT, UPDATE, DELETE), use transactional=True (default)
        - For read-only operations (SELECT), use transactional=False
        - When passing an existing AsyncConnection, the transactional parameter
          has no effect - the connection is used directly
    """
    if isinstance(conn, AsyncEngine):
        if transactional:
            async with conn.begin() as connection:
                yield connection
        else:
            async with conn.connect() as connection:
                yield connection
    else:
        # Already have a connection, use it directly
        # Caller is responsible for transaction management
        yield conn
