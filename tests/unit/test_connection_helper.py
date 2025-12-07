"""
Unit tests for the connection handling helper.

Tests the execute_with_connection async context manager for:
- Handling AsyncEngine inputs in transactional mode
- Handling AsyncEngine inputs in read-only mode
- Passing through AsyncConnection inputs directly
- Proper transaction commit on success
- Proper transaction rollback on error
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.repositories._connection import execute_with_connection


class TestExecuteWithConnection:
    """Tests for execute_with_connection context manager."""

    @pytest.mark.asyncio
    async def test_with_async_engine_transactional(self):
        """AsyncEngine input with transactional=True uses begin()."""
        # Create mock engine and connection
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        # Setup async context manager for begin()
        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__.return_value = None
        mock_engine.begin.return_value = mock_begin_context

        # Patch isinstance to return True for AsyncEngine
        with patch(
            "eventsource.repositories._connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with execute_with_connection(mock_engine, transactional=True) as conn:
                assert conn is mock_connection

        # Verify begin() was called, not connect()
        mock_engine.begin.assert_called_once()
        assert not hasattr(mock_engine, "connect") or not mock_engine.connect.called

    @pytest.mark.asyncio
    async def test_with_async_engine_read_only(self):
        """AsyncEngine input with transactional=False uses connect()."""
        # Create mock engine and connection
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        # Setup async context manager for connect()
        mock_connect_context = AsyncMock()
        mock_connect_context.__aenter__.return_value = mock_connection
        mock_connect_context.__aexit__.return_value = None
        mock_engine.connect.return_value = mock_connect_context

        # Patch isinstance to return True for AsyncEngine
        with patch(
            "eventsource.repositories._connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with execute_with_connection(mock_engine, transactional=False) as conn:
                assert conn is mock_connection

        # Verify connect() was called, not begin()
        mock_engine.connect.assert_called_once()
        assert not hasattr(mock_engine, "begin") or not mock_engine.begin.called

    @pytest.mark.asyncio
    async def test_with_async_connection(self):
        """AsyncConnection input is used directly without creating new connection."""
        # Create mock connection (not engine)
        mock_connection = AsyncMock()

        # The isinstance check will return False for AsyncEngine since it's a mock connection
        async with execute_with_connection(mock_connection, transactional=True) as conn:
            assert conn is mock_connection

        # Connection should be yielded directly, no begin/connect methods called
        assert not hasattr(mock_connection, "begin") or not mock_connection.begin.called
        assert not hasattr(mock_connection, "connect") or not mock_connection.connect.called

    @pytest.mark.asyncio
    async def test_with_async_connection_transactional_ignored(self):
        """AsyncConnection input ignores transactional parameter."""
        mock_connection = AsyncMock()

        # transactional=False should have no effect with AsyncConnection
        async with execute_with_connection(mock_connection, transactional=False) as conn:
            assert conn is mock_connection

        # Still should not call any connection creation methods
        assert not hasattr(mock_connection, "begin") or not mock_connection.begin.called
        assert not hasattr(mock_connection, "connect") or not mock_connection.connect.called

    @pytest.mark.asyncio
    async def test_transaction_commit_on_success(self):
        """Transactional context commits on normal exit."""
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        # Track if context was properly exited
        exit_called = []

        async def mock_aexit(self, exc_type, exc_val, exc_tb):
            exit_called.append((exc_type, exc_val, exc_tb))
            return None

        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__ = mock_aexit
        mock_engine.begin.return_value = mock_begin_context

        with patch(
            "eventsource.repositories._connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with execute_with_connection(mock_engine, transactional=True) as conn:
                # Simulate successful operation
                await conn.execute("SELECT 1")

        # Verify __aexit__ was called with no exception
        assert len(exit_called) == 1
        assert exit_called[0] == (None, None, None)

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_error(self):
        """Transactional context rolls back on exception."""
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        # Track exception info passed to __aexit__
        exit_called = []

        async def mock_aexit(self, exc_type, exc_val, exc_tb):
            exit_called.append((exc_type, exc_val))
            return False  # Don't suppress the exception

        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__ = mock_aexit
        mock_engine.begin.return_value = mock_begin_context

        test_error = ValueError("Test error")

        with (
            patch(
                "eventsource.repositories._connection.isinstance",
                side_effect=lambda obj, cls: obj is mock_engine,
            ),
            pytest.raises(ValueError, match="Test error"),
        ):
            async with execute_with_connection(mock_engine, transactional=True) as _:
                raise test_error

        # Verify __aexit__ was called with the exception
        assert len(exit_called) == 1
        assert exit_called[0][0] is ValueError
        assert exit_called[0][1] is test_error

    @pytest.mark.asyncio
    async def test_default_transactional_is_true(self):
        """Default transactional parameter is True (uses begin())."""
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__.return_value = None
        mock_engine.begin.return_value = mock_begin_context

        with patch(
            "eventsource.repositories._connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            # Don't pass transactional, should default to True
            async with execute_with_connection(mock_engine) as conn:
                assert conn is mock_connection

        mock_engine.begin.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_is_yielded_within_context(self):
        """Connection is available within the context and operations can be performed."""
        mock_connection = AsyncMock()
        mock_engine = MagicMock()

        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__.return_value = None
        mock_engine.begin.return_value = mock_begin_context

        with patch(
            "eventsource.repositories._connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with execute_with_connection(mock_engine) as conn:
                # Perform operations within context
                await conn.execute("INSERT INTO test VALUES (1)")
                await conn.execute("UPDATE test SET value = 2")

        # Verify execute was called twice
        assert mock_connection.execute.call_count == 2


class TestExecuteWithConnectionIntegration:
    """Integration-style tests with more realistic mock behavior."""

    @pytest.mark.asyncio
    async def test_multiple_operations_in_transaction(self):
        """Multiple operations within the same transaction context."""
        operations = []
        mock_connection = AsyncMock()

        async def track_execute(query, params=None):
            operations.append((query, params))
            return MagicMock()

        mock_connection.execute = track_execute

        mock_engine = MagicMock()
        mock_begin_context = AsyncMock()
        mock_begin_context.__aenter__.return_value = mock_connection
        mock_begin_context.__aexit__.return_value = None
        mock_engine.begin.return_value = mock_begin_context

        with patch(
            "eventsource.repositories._connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with execute_with_connection(mock_engine) as conn:
                await conn.execute("SELECT * FROM users")
                await conn.execute("INSERT INTO logs VALUES (?)", {"id": 1})
                await conn.execute("UPDATE counters SET value = value + 1")

        assert len(operations) == 3
        assert operations[0][0] == "SELECT * FROM users"
        assert operations[1][0] == "INSERT INTO logs VALUES (?)"
        assert operations[1][1] == {"id": 1}
        assert operations[2][0] == "UPDATE counters SET value = value + 1"

    @pytest.mark.asyncio
    async def test_nested_usage_with_existing_connection(self):
        """When passed an existing connection, it should be used directly."""
        # Simulate a scenario where a connection is already managed externally
        mock_connection = AsyncMock()
        execute_results = []

        async def track_execute(query, params=None):
            execute_results.append(query)
            result = MagicMock()
            result.fetchone.return_value = (1, "test")
            return result

        mock_connection.execute = track_execute

        # Use connection directly (not engine) - simulates being within a transaction
        async with execute_with_connection(mock_connection, transactional=True) as conn:
            await conn.execute("SELECT 1")

        async with execute_with_connection(mock_connection, transactional=False) as conn:
            await conn.execute("SELECT 2")

        # Both should use the same connection
        assert len(execute_results) == 2
        assert execute_results[0] == "SELECT 1"
        assert execute_results[1] == "SELECT 2"


class TestExecuteWithConnectionTypeHandling:
    """Tests for proper type handling between AsyncEngine and AsyncConnection."""

    @pytest.mark.asyncio
    async def test_connection_type_preserved(self):
        """The yielded connection maintains its type characteristics."""
        mock_connection = AsyncMock()
        mock_connection.some_custom_attribute = "test_value"

        async with execute_with_connection(mock_connection) as conn:
            # Verify the same object is yielded
            assert conn.some_custom_attribute == "test_value"
            assert conn is mock_connection

    @pytest.mark.asyncio
    async def test_engine_creates_new_connection_each_time(self):
        """Each context manager invocation with AsyncEngine creates a new connection."""
        connections = []

        def create_mock_context():
            mock_conn = AsyncMock()
            mock_conn.connection_id = len(connections)
            connections.append(mock_conn)

            mock_context = AsyncMock()
            mock_context.__aenter__.return_value = mock_conn
            mock_context.__aexit__.return_value = None
            return mock_context

        mock_engine = MagicMock()
        mock_engine.begin.side_effect = lambda: create_mock_context()

        with patch(
            "eventsource.repositories._connection.isinstance",
            side_effect=lambda obj, cls: obj is mock_engine,
        ):
            async with execute_with_connection(mock_engine) as conn1:
                assert conn1.connection_id == 0

            async with execute_with_connection(mock_engine) as conn2:
                assert conn2.connection_id == 1

        assert len(connections) == 2
        assert mock_engine.begin.call_count == 2
