"""
Tests for Graph class functionality.

Tests cover:
- Graph initialization
- Type and subtype registration
- Object registry management
- Database creation and loading
- Schema isolation
- Multiple graph instances
"""
import pytest
from m3_graph.graph import Graph
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestGraph:
    """Test Graph class functionality."""

    async def test_graph_initialization(self, db_connection, test_schema):
        """Test basic graph initialization."""

        class MyGraph(Graph):
            pass

        graph = await MyGraph.create(db_connection, name=test_schema)

        assert graph is not None
        assert graph._schema == test_schema
        assert graph._conn is db_connection
        assert isinstance(graph.registry, dict)
        assert len(graph.registry) == 0

    async def test_graph_cannot_instantiate_base_class(self, db_connection):
        """Test that base Graph class cannot be instantiated directly."""

        with pytest.raises(TypeError, match="cannot be initialised"):
            Graph(db_connection, "test")

    async def test_type_registration(self, graph):
        """Test that types are registered in the graph."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        # Type should be registered
        assert "asset" in graph.__class__.types
        assert graph.__class__.types["asset"] is Asset

    async def test_subtype_registration(self, graph):
        """Test that subtypes are registered in the graph."""

        class Product(graph.DBObject):
            category = "shop"
            type = "product"
            name: str

        class ElectronicProduct(Product):
            subtype = "electronic"
            voltage: int


        # Subtype should be registered
        assert "electronic" in graph.__class__.subtypes
        assert graph.__class__.subtypes["electronic"] is ElectronicProduct

    async def test_duplicate_type_registration_error(self, graph):
        """Test that duplicate type names raise an error."""

        class FirstAsset(graph.DBObject):
            category = "financial"
            type = "duplicate_type"
            name: str


        # Try to register another class with same type
        with pytest.raises(ValueError, match="already registered"):

            class SecondAsset(graph.DBObject):
                category = "financial"
                type = "duplicate_type"
                symbol: str


    async def test_duplicate_subtype_registration_error(self, graph):
        """Test that duplicate subtype names raise an error."""

        class Product(graph.DBObject):
            category = "shop"
            type = "product"
            name: str

        class First(Product):
            subtype = "duplicate_subtype"

        with pytest.raises(ValueError, match="already registered"):
            class Second(Product):
                subtype = "duplicate_subtype"

    async def test_registry_management(self, graph):
        """Test object registry operations."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Object should be in registry
        assert asset.id in graph.registry
        assert graph.registry[asset.id] is asset

        # Delete should remove from registry
        await asset.delete()
        assert asset.id not in graph.registry

    async def test_type_specific_registry(self, graph):
        """Test type-specific registry management."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        class User(graph.DBObject):
            category = "auth"
            type = "user"
            username: str


        asset = Asset(source="test", symbol="BTC")
        user = User(source="test", username="john")

        await asset.insert()
        await user.insert()

        # Each should be in their own type registry
        assert "asset" in graph.registry_type
        assert "user" in graph.registry_type
        assert asset.id in graph.registry_type["asset"]
        assert user.id in graph.registry_type["user"]
        assert user.id not in graph.registry_type["asset"]
        assert asset.id not in graph.registry_type["user"]

    async def test_load_from_database(self, graph):
        """Test loading objects from database."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        # Create and insert objects
        btc = Asset(source="test", symbol="BTC")
        eth = Asset(source="test", symbol="ETH")
        await btc.insert()
        await eth.insert()

        btc_id = btc.id
        eth_id = eth.id

        # Clear registries
        graph.registry.clear()
        graph.registry_type.clear()

        # Load from database
        await graph.load()

        # Objects should be back in registry
        assert btc_id in graph.registry
        assert eth_id in graph.registry

        loaded_btc = graph.registry[btc_id]
        loaded_eth = graph.registry[eth_id]

        assert loaded_btc.symbol == "BTC"
        assert loaded_eth.symbol == "ETH"

    async def test_load_with_relationships(self, graph):
        """Test loading objects with relationships from database."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str
            books: Backlink['Book']

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author, "books"]


        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Test Book", author=author)
        await book.insert()

        # Clear and reload
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        # Verify relationships restored
        loaded_book = graph.registry[book.id]
        loaded_author = graph.registry[author.id]

        assert loaded_book.author_id == loaded_author.id
        assert loaded_book.author is loaded_author

    async def test_load_skips_unregistered_types(self, graph, db_connection, test_schema):
        """Test that load skips objects with unregistered types."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        # Insert an asset
        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Manually insert a different type that's not registered
        await db_connection.execute(
            f"""
            INSERT INTO {test_schema}.object (category, type, subtype, attr, source)
            VALUES ('other', 'unregistered', 'unregistered', %(attr)s, 'test')
            """,
            attr={"name": "Unknown"}
        )

        # Clear and reload
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        # Should only have the registered type
        assert len(graph.registry) == 1
        assert asset.id in graph.registry

    async def test_dbobject_property(self, graph):
        """Test that graph provides DBObject with graph attached."""

        DBObj = graph.DBObject

        assert hasattr(DBObj, 'graph')
        assert DBObj._graph_cls is graph.__class__
        assert DBObj.graph is graph

    async def test_schema_isolation(self, db_connection):
        """Test that different graphs have isolated schemas."""

        class Graph1(Graph):
            pass

        class Graph2(Graph):
            pass

        # Create two graphs with different schemas
        g1 = await Graph1.create(db_connection, name="schema1")
        g2 = await Graph2.create(db_connection, name="schema2")

        # Define classes for each graph
        class Asset1(g1.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        class Asset2(g2.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        # Insert into each
        asset1 = Asset1(source="test", symbol="BTC")
        asset2 = Asset2(source="test", symbol="ETH")

        await asset1.insert()
        await asset2.insert()

        # Each should only see their own objects
        assert len(g1.registry) == 1
        assert len(g2.registry) == 1
        assert asset1 is g1.registry[asset1.id]
        assert asset2 is g2.registry[asset2.id]

        assert asset2 is not g1.registry.get(asset2.id)
        assert asset1 is not g2.registry.get(asset1.id)

        # Cleanup
        await db_connection.execute("DROP SCHEMA IF EXISTS schema1 CASCADE")
        await db_connection.execute("DROP SCHEMA IF EXISTS schema2 CASCADE")

    async def test_registry_cleared_on_delete(self, graph):
        """Test that delete clears object from all registries."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id

        # Verify in both registries
        assert asset_id in graph.registry
        assert asset_id in graph.registry_type["asset"]

        # Delete
        await asset.delete()

        # Verify removed from both
        assert asset_id not in graph.registry
        assert asset_id not in graph.registry_type.get("asset", {})

    async def test_multiple_types_in_same_category(self, graph):
        """Test multiple types within the same category."""

        class BaseFinancial(graph.DBObject):
            category = "financial"
            type = "base"
            name: str

        class Asset(BaseFinancial):
            type = "asset"
            symbol: str

        class Account(BaseFinancial):
            type = "account"
            balance: float


        # All should be registered
        assert "base" in graph.__class__.types
        assert "asset" in graph.__class__.types
        assert "account" in graph.__class__.types

    async def test_inheritance_hierarchy_registration(self, graph):
        """Test that inheritance hierarchies register correctly."""

        class BaseItem(graph.DBObject):
            category = "inventory"
            type = "item"
            name: str

        class Product(BaseItem):
            type = "product"
            price: float

        class ElectronicProduct(Product):
            subtype = "electronic"
            voltage: int


        # All types should be registered
        assert "item" in graph.__class__.types
        assert "product" in graph.__class__.types

        # Subtype should be registered
        assert "electronic" in graph.__class__.subtypes

    async def test_graph_create_creates_tables(self, db_connection, test_schema):
        """Test that create creates all necessary tables."""

        class MyGraph(Graph):
            pass

        await MyGraph.create(db_connection, name=test_schema)

        # Verify tables exist
        result = await db_connection.query(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %(schema)s
            ORDER BY table_name
            """,
            schema=test_schema
        )

        table_names = [row['table_name'] for row in result]

        assert 'object' in table_names
        assert 'history' in table_names
        assert 'meta_relationship' in table_names
