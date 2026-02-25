# m3_graph

Lightweight PostgreSQL ORM with graph relationships, built on Pydantic.

## Features

- **Type-safe relationships** with Link/Backlink types
- **Automatic backlink management** via database triggers
- **Unique constraint indexing** at category/type/subtype levels
- **In-memory object caching** for fast lookups
- **Full Pydantic integration** for validation

## Installation

```bash
pip install -e .
```

## Quick Start

### Define Your Models

```python
from m3_graph import DBObject, Link

class Asset(DBObject):
    category = "financial"
    type = "asset"
    name: str
    symbol: str
    type_unique_attr = ['symbol']

class Position(DBObject):
    category = "financial"
    type = "position"
    quantity: float
    asset: Link[Asset, "positions"]
```

### Simple Setup (One-Liner)

```python
import asyncio
from m3_graph import setup, DBObject

async def main():
    # Connect and bind in one step
    graph = await setup(dbname='mydb', root_class=DBObject)

    # Start using your models
    btc = Asset(source='manual', name='Bitcoin', symbol='BTC')
    await btc.insert()

asyncio.run(main())
```

### Manual Setup (More Control)

```python
import asyncio
from m3_graph import connect, Graph, DBObject

async def main():
    # Step 1: Connect to database
    conn = await connect(host='localhost', port=5432, dbname='mydb')

    # Step 2: Create graph and bind models
    graph = Graph(conn, DBObject)  # Auto-binds DBObject and subclasses

    # Or bind manually
    graph = Graph(conn)
    graph.bind(DBObject)

    # Use your models
    asset = Asset.get(symbol='BTC')
    position = Position(source='manual', quantity=1.5, asset=asset)
    await position.insert()

asyncio.run(main())
```

## Common Operations

### Querying

```python
# Get by unique constraint
asset = Asset.get(symbol='BTC')

# Get all instances
all_assets = Asset.all()

# Filter by attributes
test_assets = Asset.filter(source='test')
```

### CRUD

```python
# Create
asset = Asset(source='manual', name='Bitcoin', symbol='BTC')
await asset.insert()

# Update
asset.name = 'Bitcoin Core'
await asset.update()

# Upsert
await asset.upsert()

# Delete
await asset.delete()
```

### Relationships

```python
# Assign via object
position = Position(source='manual', quantity=1.5, asset=btc)

# Assign via ID
position = Position(source='manual', quantity=1.5, asset_id=1)

# Access related object
print(position.asset.name)  # 'Bitcoin'
```

## Database Setup

```bash
psql -d mydb -f table.sql
```

## Testing

Tests use a real PostgreSQL database for integration testing:

```bash
# Setup: Create test database
createdb m3_test
psql m3_test -f table.sql

# Run tests
pytest test/
```

Each test runs in an isolated schema that's automatically created and cleaned up.

## Requirements

- Python 3.10+
- PostgreSQL 18+
- Pydantic 2.0+
