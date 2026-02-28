"""
Pytest configuration and fixtures for m3_graph testing.

Provides PostgreSQL database fixtures and test graph instances.
Uses existing PostgreSQL instance at localhost:5432.
"""
import os
import pytest
import asyncio
from typing import AsyncGenerator

from m3_graph.conn import connect, DBConn
from m3_graph.graph import Graph


# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)


@pytest.fixture(scope="function")
def event_loop():
    """Create an event loop for each test function."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def db_connection():
    """
    Create a database connection for each test function.

    Connects to existing PostgreSQL at localhost:5432/m3_test.
    Each test gets its own connection and schema for isolation.
    """
    conn = await connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "m3_test"),
    )
    yield conn
    await conn._conn.close()

@pytest.fixture
async def test_schema(db_connection: DBConn):
    """
    Create a unique test schema for each test.

    This ensures complete isolation between tests while reusing
    the same database connection.
    """
    # Generate unique schema name
    import uuid
    schema_name = f"test_{uuid.uuid4().hex[:8]}"

    yield schema_name

    # Cleanup: drop the schema after the test
    try:
        await db_connection.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    except Exception as e:
        print(f"Warning: Failed to cleanup schema {schema_name}: {e}")


class TestGraph(Graph):
    """Test graph implementation."""
    pass


@pytest.fixture
async def graph(db_connection: DBConn, test_schema: str) -> AsyncGenerator[TestGraph, None]:
    """
    Create a fresh Graph instance with database schema for each test.

    The graph is fully initialized with tables, triggers, and functions.
    """
    # Create the graph and its schema
    g = await TestGraph.create(db_connection, name=test_schema)

    yield g

    # Clear class-level registries to prevent pollution between tests
    TestGraph.types.clear()
    TestGraph.subtypes.clear()

    # Cleanup is handled by test_schema fixture
