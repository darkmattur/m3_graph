# m3_graph

Lightweight PostgreSQL ORM with graph relationships, built on Pydantic.

## Features

- **Pydantic-based models**: Define database objects as Pydantic models with full type validation
- **Graph relationships**: First-class support for bidirectional links between objects
- **Type hierarchy**: Organize objects with category/type/subtype classification
- **In-memory indexes**: Fast lookups with automatic index management for unique constraints
- **JSONB storage**: Flexible schema using PostgreSQL JSONB for attributes
- **Async/await**: Built on asyncio and psycopg3 for async database operations

## Installation

```bash
pip install m3_graph
```

## Quick Start

```python
from m3_graph import Graph, Link, Backlink

# Define your graph schema
class MyGraph(Graph):
    pass

# Connect to PostgreSQL and create schema
graph = await MyGraph.connect(
    host="localhost",
    dbname="mydb",
    create=True
)

# Define your models
class Author(graph.DBObject):
    category = "content"
    type = "author"

    name: str
    email: str
    books: Backlink['Book']

class Book(graph.DBObject):
    category = "content"
    type = "book"

    title: str
    isbn: str
    author: Link[Author, "books"]  # Bidirectional link

# Maintain schema (register relationships and indexes)
await graph.maintain()

# Insert objects
author = Author(source="import", name="Jane Doe", email="jane@example.com")
await author.insert()

book = Book(source="import", title="My Book", isbn="978-0-123456-78-9", author=author)
await book.insert()

# Query relationships
print(book.author.name)  # "Jane Doe"
print(author.books)      # [Book(...)]

# Load all objects into graph
await graph.load()
```

## Core Concepts

### DBObject

All database objects inherit from `DBObject` and are Pydantic models:

```python
class Product(graph.DBObject):
    category = "catalog"
    type = "product"

    name: str
    price: Decimal
    tags: list[str]
```

### Relationships

**Forward links** (`Link`): One-to-one or many-to-one relationships stored as foreign keys:

```python
class Post(graph.DBObject):
    category = "content"
    type = "post"

    title: str
    author: Link[Author]           # Required link
    editor: Link[Editor] | None    # Optional link
```

**Backlinks** (`Backlink`): Reverse relationships automatically maintained by the database:

```python
class Author(graph.DBObject):
    category = "content"
    type = "author"

    name: str
    posts: Backlink['Post']  # Automatically populated (use string for forward refs)
```

### Unique Constraints & Indexes

Define unique constraints at different levels:

```python
class User(graph.DBObject):
    category = "user"
    type = "user"

    email: str
    username: str

    # Unique within category
    category_unique_attr = ["email"]

    # Unique within type
    type_unique_attr = ["username"]

    # Composite unique constraint
    subtype_unique_attr = [("email", "username")]
```

### CRUD Operations

```python
# Create
obj = Product(source="import", name="Widget", price=Decimal("9.99"))
await obj.insert()

# Update
obj.price = Decimal("12.99")
await obj.update()

# Upsert
await obj.upsert()

# Delete
await obj.delete()

# Load all objects into memory
await graph.load()
```

### Graph Registry

The graph maintains in-memory registries for fast lookups:

```python
# Access by ID
obj = graph.registry[123]

# Access by type
products = graph.registry_type["product"]

# Type-specific lookups with indexes
class Product(graph.DBObject):
    category = "catalog"
    type = "product"
    sku: str

    type_unique_attr = ["sku"]

# Fast lookup via index
product = Product._type_indexes[("sku",)]["SKU-001"]
```

## Database Schema

Objects are stored in a single table with JSONB attributes:

```sql
CREATE TABLE catalog.object (
    id BIGSERIAL PRIMARY KEY,
    category TEXT NOT NULL,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL,
    attr JSONB NOT NULL,
    source TEXT
);
```

Relationships are tracked via triggers and metadata tables, enabling automatic backlink maintenance.

## Requirements

- Python ≥3.10, <3.14
- PostgreSQL ≥18
- Pydantic ≥2.0
- psycopg ≥3.1

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Tests use pytest-postgresql for temporary databases
```

## License

Proprietary
