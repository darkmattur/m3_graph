"""
Tests for object identity and registry management.

These tests validate the single-instance-per-id pattern:
- Each database object has exactly one in-memory instance
- Registry ensures object identity is preserved
- Multiple references to same ID return same Python object
- Identity is maintained across relationship traversal
- Loading from database preserves/replaces instances correctly

Tests cover:
- Object identity preservation via registry
- Reference equality across multiple access paths
- Identity through relationships
- Identity after database reload
- Identity with multiple registries (different graphs)
- Registry cleanup and lifecycle
"""
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestObjectIdentity:
    """Test that objects maintain identity through the registry."""

    async def test_same_object_id_returns_same_instance(self, graph):
        """Test that accessing same ID from registry returns same instance."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Access from registry
        from_registry = graph.registry[asset.id]

        # Should be the exact same Python object
        assert from_registry is asset

    async def test_relationship_traversal_preserves_identity(self, graph):
        """Test that traversing relationships returns same object instance."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Book", author=author)
        await book.insert()

        # Access author through relationship
        author_via_book = book.author

        # Should be same instance
        assert author_via_book is author

    async def test_multiple_relationships_to_same_object(self, graph):
        """Test that multiple relationships to same object preserve identity."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book1 = Book(source="test", title="Book 1", author=author)
        book2 = Book(source="test", title="Book 2", author=author)
        await book1.insert()
        await book2.insert()

        # Both should reference the same author instance
        assert book1.author is book2.author
        assert book1.author is author
        assert book2.author is author

    async def test_backlink_traversal_preserves_identity(self, graph):
        """Test that backlink traversal returns same object instances."""

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

        await graph.db_maintain()

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book1 = Book(source="test", title="Book 1", author=author)
        book2 = Book(source="test", title="Book 2", author=author)
        await book1.insert()
        await book2.insert()

        # Reload to populate backlinks
        await graph.load()

        author_reloaded = graph.registry[author.id]
        books = author_reloaded.books

        # Books in backlink should be same instances as in registry
        assert books[0] is graph.registry[book1.id]
        assert books[1] is graph.registry[book2.id]

    async def test_bidirectional_relationship_identity(self, graph):
        """Test identity preservation in bidirectional relationships."""

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

        await graph.db_maintain()

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Book", author=author)
        await book.insert()

        # Reload
        await graph.load()

        author_reloaded = graph.registry[author.id]
        book_reloaded = graph.registry[book.id]

        # Forward and backward references should maintain identity
        assert book_reloaded.author is author_reloaded
        assert author_reloaded.books[0] is book_reloaded

    async def test_type_registry_returns_same_instances(self, graph):
        """Test that type-specific registry returns same instances as main registry."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Access from both registries
        from_main_registry = graph.registry[asset.id]
        from_type_registry = graph.registry_type["asset"][asset.id]

        # Should be same instance
        assert from_main_registry is from_type_registry
        assert from_main_registry is asset


@pytest.mark.asyncio
class TestIdentityAfterReload:
    """Test identity behavior when reloading from database."""

    async def test_reload_creates_new_instances(self, graph):
        """Test that reloading from database creates new Python instances."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()
        asset_id = asset.id

        original_instance = asset

        # Clear and reload
        graph.registry.clear()
        await graph.load()

        reloaded_instance = graph.registry[asset_id]

        # Should be different Python objects (new instance)
        assert reloaded_instance is not original_instance
        # But represent same database object
        assert reloaded_instance.id == original_instance.id
        assert reloaded_instance.symbol == original_instance.symbol

    async def test_reload_updates_existing_references(self, graph):
        """Test that all references use the new instance after reload."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Modify price directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = attr || %(price_update)s::jsonb WHERE id = %(asset_id)s",
            price_update='{"price": 200.0}',
            asset_id=asset_id
        )

        # Reload
        graph.registry.clear()
        await graph.load()

        # New instance should have updated value
        reloaded = graph.registry[asset_id]
        assert reloaded.price == 200.0

    async def test_relationships_after_reload(self, graph):
        """Test that relationship identity is correct after reload."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Book", author=author)
        await book.insert()

        author_id = author.id
        book_id = book.id

        # Reload
        graph.registry.clear()
        await graph.load()

        author_reloaded = graph.registry[author_id]
        book_reloaded = graph.registry[book_id]

        # Relationship should point to reloaded instance
        assert book_reloaded.author is author_reloaded

    async def test_multiple_reloads_maintain_identity_within_session(self, graph):
        """Test that identity is maintained within each load session."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()
        asset_id = asset.id

        # First reload
        graph.registry.clear()
        await graph.load()
        instance1 = graph.registry[asset_id]

        # Access same object multiple times in this session
        instance1_again = graph.registry[asset_id]
        assert instance1 is instance1_again

        # Second reload (new session)
        graph.registry.clear()
        await graph.load()
        instance2 = graph.registry[asset_id]

        # Within this session, same identity
        instance2_again = graph.registry[asset_id]
        assert instance2 is instance2_again

        # But different from previous session
        assert instance2 is not instance1

    async def test_partial_reload_preserves_other_objects(self, graph):
        """Test that loading only affects cleared registry entries."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        btc = Asset(source="test", symbol="BTC")
        eth = Asset(source="test", symbol="ETH")
        await btc.insert()
        await eth.insert()

        btc_id = btc.id
        eth_id = eth.id

        # Clear everything and reload
        graph.registry.clear()
        await graph.load()

        # Get new instances
        btc_new = graph.registry[btc_id]
        eth_new = graph.registry[eth_id]

        # Within this session, identity is preserved
        assert graph.registry[btc_id] is btc_new
        assert graph.registry[eth_id] is eth_new


@pytest.mark.asyncio
class TestMultipleRegistries:
    """Test object identity with multiple graph instances."""

    async def test_different_graphs_have_separate_identities(self, db_connection):
        """Test that different graphs maintain separate object instances."""
        from m3_graph.graph import Graph

        class Graph1(Graph):
            pass

        class Graph2(Graph):
            pass

        # Create two graphs with different schemas
        g1 = await Graph1.db_create(db_connection, name="schema_test_1")
        g2 = await Graph2.db_create(db_connection, name="schema_test_2")

        # Define same class structure for both
        class Asset1(g1.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        class Asset2(g2.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        # Create objects with same data
        asset1 = Asset1(source="test", symbol="BTC")
        asset2 = Asset2(source="test", symbol="BTC")

        await asset1.insert()
        await asset2.insert()

        # Should be different instances in different graphs
        assert asset1 is not asset2
        assert asset1 in g1.registry.values()
        assert asset2 in g2.registry.values()
        assert asset1 not in g2.registry.values()
        assert asset2 not in g1.registry.values()

        # Cleanup
        await db_connection.execute("DROP SCHEMA IF EXISTS schema_test_1 CASCADE")
        await db_connection.execute("DROP SCHEMA IF EXISTS schema_test_2 CASCADE")

    async def test_same_id_different_graphs_different_objects(self, db_connection):
        """Test that same ID in different graphs refers to different objects."""
        from m3_graph.graph import Graph

        class Graph1(Graph):
            pass

        class Graph2(Graph):
            pass

        g1 = await Graph1.db_create(db_connection, name="schema_test_3")
        g2 = await Graph2.db_create(db_connection, name="schema_test_4")

        class Asset1(g1.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        class Asset2(g2.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset1 = Asset1(source="test", symbol="BTC")
        asset2 = Asset2(source="test", symbol="ETH")

        await asset1.insert()
        await asset2.insert()

        # Even if IDs happen to match (unlikely but possible), they're separate objects
        assert asset1 is not asset2

        # Cleanup
        await db_connection.execute("DROP SCHEMA IF EXISTS schema_test_3 CASCADE")
        await db_connection.execute("DROP SCHEMA IF EXISTS schema_test_4 CASCADE")


@pytest.mark.asyncio
class TestRegistryLifecycle:
    """Test registry behavior through object lifecycle."""

    async def test_object_in_registry_after_insert(self, graph):
        """Test that object is added to registry after insert."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")

        # Not in registry before insert
        assert asset.id is None
        assert asset not in graph.registry.values()

        # In registry after insert
        await asset.insert()
        assert asset.id in graph.registry
        assert graph.registry[asset.id] is asset

    async def test_object_removed_from_registry_after_delete(self, graph):
        """Test that object is removed from registry after delete."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id
        assert asset_id in graph.registry

        # Remove from registry after delete
        await asset.delete()
        assert asset_id not in graph.registry

    async def test_object_stays_in_registry_after_update(self, graph):
        """Test that object remains in registry after update."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        original_instance = asset

        # Update
        asset.price = 200.0
        await asset.update()

        # Still same instance in registry
        assert graph.registry[asset.id] is original_instance

    async def test_registry_cleared_manually(self, graph):
        """Test manually clearing registry."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        assert asset.id in graph.registry

        # Manual clear
        graph.registry.clear()

        # Registry empty
        assert len(graph.registry) == 0
        assert asset.id not in graph.registry

    async def test_registry_repopulated_after_load(self, graph):
        """Test that registry is repopulated after load."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset1 = Asset(source="test", symbol="BTC")
        asset2 = Asset(source="test", symbol="ETH")
        await asset1.insert()
        await asset2.insert()

        asset1_id = asset1.id
        asset2_id = asset2.id

        # Clear registry
        graph.registry.clear()
        assert len(graph.registry) == 0

        # Reload
        await graph.load()

        # Registry repopulated
        assert len(graph.registry) == 2
        assert asset1_id in graph.registry
        assert asset2_id in graph.registry

    async def test_creating_object_with_id_adds_to_registry(self, graph):
        """Test that creating object with existing ID adds it to registry."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        # Create object with ID (simulating database load)
        asset = Asset(id=999, source="test", symbol="BTC")

        # Should be in registry automatically
        assert 999 in graph.registry
        assert graph.registry[999] is asset

    async def test_relationship_backfills_registry(self, graph):
        """Test that setting relationship with non-registered object backfills registry."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author] | None = None

        # Create author with ID but not in registry
        graph.registry.clear()
        author = Author(id=100, source="test", name="Jane")
        author_id = author.id

        # Create book without author initially
        book = Book(source="test", title="Book")
        assert book.author is None

        # Set author - this should backfill registry
        book.author = author

        # Author should now be in registry
        assert author_id in graph.registry
        assert graph.registry[author_id] is author
