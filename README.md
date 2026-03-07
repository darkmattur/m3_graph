# m3_graph

Lightweight PostgreSQL ORM with graph relationships, built on Pydantic.

## Table of Contents

- [Why m3_graph?](#why-m3_graph)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [Category / Type / Subtype](#category--type--subtype)
  - [DBObject](#dbobject)
  - [Relationships](#relationships)
  - [Unique Constraints & Indexes](#unique-constraints--indexes)
  - [CRUD Operations](#crud-operations)
  - [Querying Objects](#querying-objects)
  - [Type Inheritance & Hierarchical Queries](#type-inheritance--hierarchical-queries)
- [Advanced Features](#advanced-features)
- [Database Schema](#database-schema)
- [Complete Example](#complete-example)
- [Best Practices](#best-practices)
- [Design Philosophy](#design-philosophy)
- [Performance Characteristics](#performance-characteristics)
- [Requirements](#requirements)
- [Development](#development)

## Why m3_graph?

**m3_graph** fills a specific niche: applications that model complex, interconnected entities with rich type hierarchies. Think knowledge graphs, business process models, financial systems, or content management - domains where:

- Entities have many bidirectional relationships
- You want OOP-style inheritance for your data model
- Flexible schemas beat rigid table structures
- Read-heavy workloads benefit from in-memory performance
- Strong typing and validation prevent data corruption

Unlike traditional ORMs (SQLAlchemy, Django ORM), m3_graph embraces graph-like structures and JSONB flexibility while maintaining type safety. Unlike pure graph databases (Neo4j), you keep PostgreSQL's reliability and ecosystem.

## Features

- **Pydantic-based models**: Define database objects as Pydantic models with full type validation
- **Graph relationships**: First-class support for bidirectional links between objects with automatic backlink maintenance
- **Type hierarchy**: Organize objects with category/type/subtype classification and inheritance
- **Hierarchical queries**: Query parent classes to find instances of any descendant type
- **In-memory indexes**: Lightning-fast lookups with automatic index management for unique constraints
- **Unsaved object references**: Link objects before saving them to the database
- **Cascading operations**: Automatic upsert of related objects with `upsert()`
- **JSONB storage**: Flexible schema using PostgreSQL JSONB for attributes
- **Async/await**: Built on asyncio and psycopg3 for async database operations
- **Computed property indexes**: Index and query by computed properties for powerful abstractions

## Installation

```bash
pip install m3_graph
```

## Quick Start

```python
from m3_graph import Graph, Link, Backlink

# 1. Define your graph schema
class MyGraph(Graph):
    pass

# 2. Connect to PostgreSQL and create schema
graph = await MyGraph.connect(
    host="localhost",
    dbname="mydb",
    create=True  # Creates tables on first run
)

# 3. Define your models with relationships
class Author(graph.DBObject):
    category = "content"
    type = "author"

    name: str
    email: str
    books: Backlink['Book']  # Reverse relationship

class Book(graph.DBObject):
    category = "content"
    type = "book"

    title: str
    isbn: str
    author: Link[Author, "books"]  # Forward link with backlink name

# 4. Register relationships and create indexes
await graph.maintain()

# 5. Create and save objects
author = Author(source="import", name="Jane Doe", email="jane@example.com")
await author.insert()

book = Book(source="import", title="My Book", isbn="978-0-123456-78-9", author=author)
await book.insert()

# 6. Navigate relationships
print(book.author.name)  # "Jane Doe"
print(author.books)      # [Book(...)]

# 7. Load data from database
await graph.load()  # Loads all objects into memory for fast access
```

## Core Concepts

### Category / Type / Subtype

Every object has a three-level classification:

- **Category**: Broad grouping (e.g., "financial", "content", "users")
- **Type**: Specific object type (e.g., "asset", "post", "user")
- **Subtype**: Defaults to type, but can differ for variants (e.g., "manager" subtype of "employee" type)

```python
class Employee(graph.DBObject):
    category = "people"    # Broad classification
    type = "employee"      # Specific type
    # subtype defaults to "employee"

class Manager(Employee):
    # Inherits category and type
    subtype = "manager"    # Explicitly set different subtype
```

The hierarchy enables:
- Different unique constraints at each level
- Inheritance with type-based queries
- Organized namespacing of your domain model

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

Each object has:
- **id**: Auto-assigned database ID (or None if not yet saved)
- **source**: Optional string tracking where the object came from (e.g., "import", "api", "user")
- **Pydantic validation**: Full type checking and validation on all attributes

### Relationships

**Forward links** (`Link`): One-to-one or many-to-one relationships stored as foreign keys:

```python
class Post(graph.DBObject):
    category = "content"
    type = "post"

    title: str
    author: Link[Author]                      # Required link
    editor: Link[Editor] | None               # Optional link
    reviewer: Link[User, "reviewed_posts"]    # With explicit backlink name
```

Behind the scenes, `Link[Author]` creates an `author_id: int | None` field in the database.

**Backlinks** (`Backlink`): Reverse relationships automatically maintained by the database:

```python
class Author(graph.DBObject):
    category = "content"
    type = "author"

    name: str
    posts: Backlink['Post']  # Automatically populated (use string for forward refs)
```

Backlinks are:
- Maintained by PostgreSQL triggers (not Python code)
- Always up-to-date, even across multiple application instances
- Returned as lists of related objects

### Unique Constraints & Indexes

Define unique constraints at different levels of the hierarchy:

```python
class User(graph.DBObject):
    category = "user"
    type = "user"

    email: str
    username: str

    # Unique within category (across all types in this category)
    category_unique_attr = ["email"]

    # Unique within type (across all subtypes of this type)
    type_unique_attr = ["username"]

    # Unique within subtype (only this specific subtype)
    subtype_unique_attr = [("email", "username")]  # Composite constraint
```

**Computed Property Indexes** for powerful abstractions:

```python
class Person(graph.DBObject):
    category = "people"
    type = "person"

    first_name: str
    last_name: str

    # Index computed properties for fast lookups
    computed_unique_attr = ['full_name']

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"

# Fast lookup by computed property
person = Person.get(full_name="Jane Doe")
```

### CRUD Operations

```python
# Create
obj = Product(source="import", name="Widget", price=Decimal("9.99"))
await obj.insert()

# Update
obj.price = Decimal("12.99")
await obj.update()

# Upsert (insert or update)
await obj.upsert()

# Delete
await obj.delete()

# Load all objects of a specific type into memory
await Product.load()

# Load all objects in the graph
await graph.load()
```

**Unsaved Object References** - link objects before saving:

```python
# Create linked objects without saving first
author = Author(source="import", name="Jane Doe", email="jane@example.com")
book = Book(source="import", title="My Book", isbn="978-0-123456-78-9", author=author)

# Backlinks work even for unsaved objects!
print(author.books)  # [Book(...)]

# Cascading upsert saves everything in the correct order
await book.upsert()  # Automatically saves author first, then book
```

### Querying Objects

**Direct registry access**:

```python
# Access by ID
obj = graph.registry[123]

# Access all objects of a type
products = graph.registry_type["product"]
```

**Query methods with hierarchical support**:

```python
# Get all objects of a type
all_products = Product.all()

# Get single object by unique attributes (uses indexes)
product = Product.get(sku="SKU-001")

# Filter objects (linear search)
active_users = User.filter(status="active")
premium_users = User.filter(status="active", tier="premium")
```

### Type Inheritance & Hierarchical Queries

Create type hierarchies and query across inheritance boundaries:

```python
class Asset(graph.DBObject):
    category = "financial"
    type = "asset"
    symbol: str
    type_unique_attr = ["symbol"]

class Token(Asset):
    type = "token"
    blockchain: str

class Stock(Asset):
    type = "stock"
    exchange: str

# Create instances
btc = Token(source="import", symbol="BTC", blockchain="ethereum")
await btc.insert()

tsla = Stock(source="import", symbol="TSLA", exchange="NASDAQ")
await tsla.insert()

# Parent class queries find descendant instances
asset = Asset.get(symbol="BTC")  # Returns Token instance
assert isinstance(asset, Token)

asset = Asset.get(symbol="TSLA")  # Returns Stock instance
assert isinstance(asset, Stock)

# Load all descendants
all_assets = await Asset.load()  # Loads Tokens AND Stocks

# Type-specific queries work as expected
token = Token.get(symbol="BTC")  # Only finds tokens
```

**Multi-level inheritance**:

```python
class ERC20Token(Token):
    type = "erc20_token"
    decimals: int

usdc = ERC20Token(source="import", symbol="USDC", blockchain="ethereum", decimals=6)
await usdc.insert()

# All levels work hierarchically
Asset.get(symbol="USDC")      # Returns ERC20Token instance
Token.get(symbol="USDC")      # Returns ERC20Token instance
ERC20Token.get(symbol="USDC") # Returns ERC20Token instance
```

## Advanced Features

### Excluding Attributes from Database Storage

Exclude transient or computed attributes from being stored in the database:

```python
class User(graph.DBObject):
    category = "user"
    type = "user"

    name: str
    email: str
    password_hash: str

    # Transient session data - not stored in DB
    last_seen: datetime | None = None

    excluded_attrs = {"last_seen"}
```

### Relationship Expansion

Load objects with all their related objects in a single query:

```python
# Load tokens and all connected objects (issuers, holders, etc.)
tokens = await Token.load(expand=True)
```

### Automatic Index Management

Indexes are automatically created and maintained in the database:

```python
await graph.maintain()  # Creates/updates all indexes and relationship triggers
```

### Dynamic Attribute Setting with Auto-Reindexing

When you change an indexed attribute, indexes are automatically updated:

```python
product = Product.get(sku="SKU-001")
product.sku = "SKU-002"  # Indexes automatically updated!
# Old index entry removed, new one added
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

**Metadata table** tracks type hierarchy and relationships:

```sql
CREATE TABLE catalog.meta (
    category TEXT NOT NULL,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL,
    forward JSONB,              -- Forward link mappings
    back JSONB,                 -- Backlink field names
    parent_types TEXT[],        -- Inheritance chain
    descendant_types TEXT[],    -- All descendant subtypes
    PRIMARY KEY (category, type, subtype)
);
```

Relationships are maintained via PostgreSQL triggers that automatically update backlink arrays when forward links change. This ensures bidirectional relationships stay in sync at the database level.

## Complete Example

Here's a comprehensive example showing many features working together:

```python
from m3_graph import Graph, Link, Backlink
from decimal import Decimal

# Define your graph schema
class MyGraph(Graph):
    pass

# Connect and create schema
graph = await MyGraph.connect(
    host="localhost",
    dbname="mydb",
    create=True
)

# Base class with shared attributes
class BaseEntity(graph.DBObject):
    category = "business"
    type = "entity"
    name: str
    created_at: str

# Organization hierarchy
class Company(BaseEntity):
    type = "company"
    registration_id: str
    employees: Backlink['Employee']
    products: Backlink['Product']

    type_unique_attr = ["registration_id"]

class Department(BaseEntity):
    type = "department"
    company: Link[Company, "departments"]
    manager: Link['Manager', "managed_departments"]

    departments: Backlink['Department']  # Sub-departments

# People hierarchy
class Person(BaseEntity):
    type = "person"
    email: str

    category_unique_attr = ["email"]
    computed_unique_attr = ["full_name"]

    @property
    def full_name(self) -> str:
        return self.name

class Employee(Person):
    type = "employee"
    employee_id: str
    company: Link[Company, "employees"]
    manager: Link['Manager', "direct_reports"] | None = None

    type_unique_attr = ["employee_id"]

class Manager(Employee):
    type = "manager"
    direct_reports: Backlink[Employee]
    managed_departments: Backlink[Department]

# Products
class Product(graph.DBObject):
    category = "catalog"
    type = "product"
    sku: str
    name: str
    price: Decimal
    manufacturer: Link[Company, "products"]

    type_unique_attr = ["sku"]

# Register all types and create indexes/triggers
await graph.maintain()

# Create a complete organizational structure
acme = Company(
    source="import",
    name="Acme Corp",
    created_at="2024-01-01",
    registration_id="REG-12345"
)

# Link objects before saving (unsaved references)
ceo = Manager(
    source="import",
    name="Jane Smith",
    email="jane@acme.com",
    employee_id="EMP001",
    created_at="2024-01-01",
    company=acme  # acme not yet saved!
)

engineering = Department(
    source="import",
    name="Engineering",
    created_at="2024-01-01",
    company=acme,
    manager=ceo
)

# Cascading upsert saves everything in correct order
await engineering.upsert()  # Saves: acme -> ceo -> engineering

# Add employees
dev = Employee(
    source="import",
    name="Bob Developer",
    email="bob@acme.com",
    employee_id="EMP002",
    created_at="2024-01-01",
    company=acme,
    manager=ceo
)
await dev.insert()

# Add products
widget = Product(
    source="import",
    sku="WDG-001",
    name="Super Widget",
    price=Decimal("99.99"),
    manufacturer=acme
)
await widget.insert()

# Load all data
await graph.load()

# Hierarchical queries work across the type hierarchy
person = Person.get(email="jane@acme.com")  # Returns Manager instance
assert isinstance(person, Manager)

employee = Employee.get(employee_id="EMP001")  # Returns Manager instance
assert isinstance(employee, Manager)

# Query by computed property
manager = Person.get(full_name="Jane Smith")
assert manager == ceo

# Navigate relationships
print(f"CEO manages {len(ceo.direct_reports)} people")
print(f"{acme.name} has {len(acme.employees)} employees")
print(f"{acme.name} makes {len(acme.products)} products")

# Fast indexed lookups
product = Product.get(sku="WDG-001")
company = Company.get(registration_id="REG-12345")

# Filter with linear search
managers = Manager.filter(company=acme)
engineering_staff = Employee.filter(manager=ceo)
```

## Best Practices

### When to use `insert()` vs `upsert()`

- **`insert()`**: Use when you know objects don't exist and all references are already saved. Fails if object has unsaved references or already exists.
- **`upsert()`**: Use when you want cascading saves or aren't sure if object exists. Automatically saves related objects first.
- **`update()`**: Use when you know object exists and all references are saved. Slightly faster than upsert.

### Organizing your models

```python
# Group related types by category
class ContentGraph(Graph):
    pass

# Use inheritance for shared attributes
class BaseContent(graph.DBObject):
    category = "content"
    type = "content"

    title: str
    created_at: datetime
    author: Link[User, "authored_content"]

class Article(BaseContent):
    type = "article"
    body: str
    tags: list[str]

class Video(BaseContent):
    type = "video"
    duration: int
    url: str
```

### Index design

- Use **category-level** constraints for truly global uniqueness (e.g., email addresses)
- Use **type-level** constraints for uniqueness within a type family (e.g., SKU across all products)
- Use **subtype-level** constraints for variant-specific uniqueness
- Use **computed property indexes** for derived values that you query frequently

### Memory management

```python
# Load only what you need
await Product.load()  # Load just products

# Load with relationships
await Product.load(expand=True)  # Load products + related objects

# Clear registry when done
graph.registry.clear()
graph.registry_type.clear()
```

## Design Philosophy

**m3_graph** is designed for applications that need:

- **Flexible schemas**: JSONB storage means you can add attributes without migrations
- **Complex relationships**: Graph-like data with bidirectional links between entities
- **Type hierarchies**: Object-oriented design with inheritance and polymorphism
- **In-memory performance**: Once loaded, queries are instant via indexes and registries
- **Strong typing**: Full Pydantic validation ensures data integrity

**Trade-offs**:

- Best for read-heavy workloads after initial load
- In-memory indexes require sufficient RAM for your dataset
- Single-table design optimizes for flexibility over relational query optimization
- Works best with moderate dataset sizes (thousands to millions of objects)

## Performance Characteristics

- **Indexed lookups**: O(1) average case for unique attribute queries via hash maps
- **Hierarchical get()**: O(h) where h is hierarchy depth (searches parent then children)
- **Filter operations**: O(n) linear search through type registry
- **Load operations**: Single SQL query per type, bulk object instantiation
- **Relationship traversal**: O(1) for forward links, O(1) for backlinks (array lookup)

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
