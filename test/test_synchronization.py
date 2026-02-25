"""
Tests for database synchronization and consistency.

These tests validate the synchronization between in-memory objects and database:
- Database is the authoritative source of truth
- Load operations correctly synchronize in-memory state with database state
- Concurrent modifications are handled correctly
- Stale data detection and handling
- Consistency after various operation sequences

Tests cover:
- Loading fresh state from database
- Detecting out-of-sync conditions
- Synchronization after external database changes
- Consistency across graph.load() operations
- Handling of deleted objects
- Orphaned relationship references
"""
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestLoadSynchronization:
    """Test that load() correctly synchronizes state from database."""

    async def test_load_reflects_database_state(self, graph):
        """Test that load() loads current database state."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Modify directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # Load should reflect database change
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 200.0

    async def test_load_after_external_insert(self, graph):
        """Test loading objects inserted externally to current session."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        # Insert via ORM
        asset1 = Asset(source="test", symbol="BTC")
        await asset1.insert()

        # Insert directly via database
        result = await graph._conn.query(
            f"""
            INSERT INTO {graph._name}.object (category, type, subtype, attr, source)
            VALUES ('financial', 'asset', 'asset', %(attr)s, 'test')
            RETURNING id
            """,
            attr={"symbol": "ETH"}
        )
        asset2_id = result[0]['id']

        # Clear and reload
        graph.registry.clear()
        await graph.load()

        # Should load both
        assert asset1.id in graph.registry
        assert asset2_id in graph.registry
        assert len(graph.registry) == 2

    async def test_load_after_external_delete(self, graph):
        """Test that load() handles externally deleted objects."""

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

        # Delete asset1 directly from database
        await graph._conn.execute(
            f"DELETE FROM {graph._name}.object WHERE id = %(asset1_id)s",
            asset1_id=asset1_id
        )

        # Reload
        graph.registry.clear()
        await graph.load()

        # Only asset2 should be loaded
        assert asset1_id not in graph.registry
        assert asset2_id in graph.registry
        assert len(graph.registry) == 1

    async def test_load_after_external_update(self, graph):
        """Test that load() picks up external updates."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Hold reference to original object
        original = asset

        # Update directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '500.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # Original object still has old value
        assert original.price == 100.0

        # Load creates new instance with updated value
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 500.0
        assert reloaded is not original

    async def test_multiple_loads_are_idempotent(self, graph):
        """Test that multiple load() calls don't cause issues."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()
        asset_id = asset.id

        # Load multiple times
        graph.registry.clear()
        await graph.load()
        instance1 = graph.registry[asset_id]

        graph.registry.clear()
        await graph.load()
        instance2 = graph.registry[asset_id]

        graph.registry.clear()
        await graph.load()
        instance3 = graph.registry[asset_id]

        # Each should have correct data
        assert instance1.symbol == "BTC"
        assert instance2.symbol == "BTC"
        assert instance3.symbol == "BTC"

        # But be different instances
        assert instance1 is not instance2
        assert instance2 is not instance3

    async def test_load_with_relationships(self, graph):
        """Test that load() correctly restores relationships."""

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

        # Clear and reload
        graph.registry.clear()
        await graph.load()

        # Relationships should work
        book_reloaded = graph.registry[book.id]
        author_reloaded = graph.registry[author.id]

        assert book_reloaded.author_id == author_reloaded.id
        assert book_reloaded.author is author_reloaded


@pytest.mark.asyncio
class TestStaleDataHandling:
    """Test handling of stale in-memory data vs database."""

    async def test_in_memory_object_can_become_stale(self, graph):
        """Test that in-memory object can have stale data."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Modify directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # In-memory object is now stale
        assert asset.price == 100.0  # Stale value

        # Database has different value
        graph.registry.clear()
        await graph.load()
        fresh = graph.registry[asset_id]
        assert fresh.price == 200.0

    async def test_updating_stale_object_overwrites_database(self, graph):
        """Test that updating a stale object overwrites database (last write wins)."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Simulate external modification
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # Update stale in-memory object
        asset.price = 150.0
        await asset.update()

        # Database should have 150.0 (last write wins)
        graph.registry.clear()
        await graph.load()
        reloaded = graph.registry[asset_id]
        assert reloaded.price == 150.0  # Not 200.0

    async def test_load_refreshes_stale_relationships(self, graph):
        """Test that loading refreshes stale relationship data."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        author1 = Author(source="test", name="Author 1")
        author2 = Author(source="test", name="Author 2")
        await author1.insert()
        await author2.insert()

        book = Book(source="test", title="Book", author=author1)
        await book.insert()

        # Change relationship directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{author_id}}', %(author_id)s) WHERE id = %(book_id)s",
            author_id=str(author2.id),
            book_id=book.id
        )

        # In-memory book still points to author1 (stale)
        assert book.author_id == author1.id

        # Load refreshes
        graph.registry.clear()
        await graph.load()

        book_reloaded = graph.registry[book.id]
        assert book_reloaded.author_id == author2.id


@pytest.mark.asyncio
class TestConsistencyScenarios:
    """Test consistency in various scenarios."""

    async def test_consistency_after_insert_update_delete_sequence(self, graph):
        """Test consistency through full CRUD lifecycle."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Insert
        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Verify in database
        graph.registry.clear()
        await graph.load()
        assert asset_id in graph.registry

        # Update
        asset_from_db = graph.registry[asset_id]
        asset_from_db.price = 200.0
        await asset_from_db.update()

        # Verify update
        graph.registry.clear()
        await graph.load()
        assert graph.registry[asset_id].price == 200.0

        # Delete
        asset_to_delete = graph.registry[asset_id]
        await asset_to_delete.delete()

        # Verify deletion
        graph.registry.clear()
        await graph.load()
        assert asset_id not in graph.registry

    async def test_consistency_with_mixed_operations(self, graph):
        """Test consistency when mixing in-memory and database operations."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Create multiple assets
        btc = Asset(source="test", symbol="BTC", price=100.0)
        eth = Asset(source="test", symbol="ETH", price=50.0)
        await btc.insert()
        await eth.insert()

        # Modify BTC in-memory and persist
        btc.price = 200.0
        await btc.update()

        # Modify ETH directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '75.0') WHERE id = %(eth_id)s",
            eth_id=eth.id
        )

        # Load and verify
        graph.registry.clear()
        await graph.load()

        btc_reloaded = graph.registry[btc.id]
        eth_reloaded = graph.registry[eth.id]

        assert btc_reloaded.price == 200.0
        assert eth_reloaded.price == 75.0

    async def test_consistency_with_relationship_changes(self, graph):
        """Test consistency when relationships change."""

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

        author1 = Author(source="test", name="Author 1")
        author2 = Author(source="test", name="Author 2")
        await author1.insert()
        await author2.insert()

        book = Book(source="test", title="Book", author=author1)
        await book.insert()

        # Change relationship
        book.author = author2
        await book.update()

        # Verify consistency
        graph.registry.clear()
        await graph.load()

        book_reloaded = graph.registry[book.id]
        author1_reloaded = graph.registry[author1.id]
        author2_reloaded = graph.registry[author2.id]

        assert book_reloaded.author_id == author2_reloaded.id
        assert len(author1_reloaded.books) == 0
        assert len(author2_reloaded.books) == 1

    async def test_consistency_after_cascading_deletes(self, graph):
        """Test consistency after deleting objects with relationships."""

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

        # Delete book
        await book.delete()

        # Load and verify author's backlinks are empty
        graph.registry.clear()
        await graph.load()

        author_reloaded = graph.registry[author.id]
        assert len(author_reloaded.books) == 0


@pytest.mark.asyncio
class TestOrphanedReferences:
    """Test handling of orphaned relationship references."""

    async def test_orphaned_forward_reference(self, graph):
        """Test accessing relationship when referenced object was deleted."""

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

        # Delete author directly from database
        await graph._conn.execute(
            f"DELETE FROM {graph._name}.object WHERE id = %(author_id)s",
            author_id=author.id
        )

        # Reload
        graph.registry.clear()
        await graph.load()

        book_reloaded = graph.registry[book.id]

        # Book still has author_id but author is not in registry
        assert book_reloaded.author_id == author.id
        assert book_reloaded.author is None

    async def test_orphaned_backlink_reference(self, graph):
        """Test backlinks when referenced objects were deleted."""

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

        # Delete book1 directly from database
        await graph._conn.execute(
            f"DELETE FROM {graph._name}.object WHERE id = %(book1_id)s",
            book1_id=book1.id
        )

        # Reload
        graph.registry.clear()
        await graph.load()

        author_reloaded = graph.registry[author.id]

        # Should only see book2
        assert len(author_reloaded.books) == 1
        assert author_reloaded.books[0].id == book2.id

    async def test_nullable_orphaned_reference(self, graph):
        """Test nullable relationship when referenced object is deleted."""

        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category] | None = None

        cat = Category(source="test", name="Electronics")
        await cat.insert()

        item = Item(source="test", name="Laptop", category_obj=cat)
        await item.insert()

        # Delete category directly from database
        await graph._conn.execute(
            f"DELETE FROM {graph._name}.object WHERE id = %(cat_id)s",
            cat_id=cat.id
        )

        # Reload
        graph.registry.clear()
        await graph.load()

        item_reloaded = graph.registry[item.id]

        # Item still has category_obj_id but category is not in registry
        assert item_reloaded.category_obj_id == cat.id
        assert item_reloaded.category_obj is None


@pytest.mark.asyncio
class TestConcurrentModifications:
    """Test behavior with concurrent-style modifications."""

    async def test_last_write_wins(self, graph):
        """Test that last write wins in simple conflict scenario."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Simulate two "concurrent" updates (sequential but both from original state)
        # First update
        asset.price = 200.0
        await asset.update()

        # Simulate another session loaded original state and modified
        # (We simulate by updating DB directly)
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '300.0') WHERE id = %(asset_id)s",
            asset_id=asset.id
        )

        # Now in-memory object updates again
        asset.price = 250.0
        await asset.update()

        # Last write (250.0) should win
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 250.0

    async def test_independent_field_updates(self, graph):
        """Test that updates to different fields work independently."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            volume: float

        asset = Asset(source="test", symbol="BTC", price=100.0, volume=1000.0)
        await asset.insert()

        # Update price
        asset.price = 200.0
        await asset.update()

        # Simulate external update to volume
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{volume}}', '2000.0') WHERE id = %(asset_id)s",
            asset_id=asset.id
        )

        # Load to see both changes
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0
        assert reloaded.volume == 2000.0
