"""
Tests for state management: in-memory changes vs database persistence.

These tests validate the core ORM pattern:
- Database is the single source of truth for persistent state
- Objects can be modified in-memory without affecting the database
- Changes must be explicitly written back via update()
- Unsaved in-memory changes are visible immediately in code
- Database state remains unchanged until update() is called

Tests cover:
- In-memory modification behavior
- Database persistence via update()
- Dirty state detection
- Partial updates
- Multiple in-memory changes before persistence
- Rollback via reload from database
- Field-level change tracking
"""
import pytest
from m3_graph.link import Link


@pytest.mark.asyncio
class TestInMemoryModification:
    """Test in-memory object modification without database writes."""

    async def test_in_memory_changes_visible_immediately(self, graph):
        """Test that in-memory changes are immediately visible."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Modify in-memory
        asset.price = 200.0

        # Should be visible immediately
        assert asset.price == 200.0

    async def test_in_memory_changes_not_persisted_without_update(self, graph):
        """Test that in-memory changes don't persist without update()."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Modify in-memory but don't update
        asset.price = 200.0

        # Reload from database
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 100.0  # Should be original value

    async def test_multiple_in_memory_changes_before_persist(self, graph):
        """Test multiple in-memory modifications before calling update()."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            name: str

        asset = Asset(source="test", symbol="BTC", price=100.0, name="Bitcoin")
        await asset.insert()

        # Multiple in-memory changes
        asset.price = 200.0
        asset.price = 300.0
        asset.name = "Bitcoin Core"
        asset.price = 400.0

        # All changes visible in-memory
        assert asset.price == 400.0
        assert asset.name == "Bitcoin Core"

        # But not in database yet
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 100.0
        assert reloaded.name == "Bitcoin"

    async def test_in_memory_relationship_change_visibility(self, graph):
        """Test that in-memory relationship changes are immediately visible."""

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

        # Change relationship in-memory
        book.author = author2

        # Should be visible immediately
        assert book.author_id == author2.id
        assert book.author.name == "Author 2"

    async def test_nullable_field_in_memory_changes(self, graph):
        """Test in-memory changes to nullable fields."""

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            description: str | None = None

        item = Item(source="test", name="Item", description="Original")
        await item.insert()

        # Change to None in-memory
        item.description = None
        assert item.description is None

        # Not persisted yet
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.description == "Original"

    async def test_complex_type_in_memory_mutation(self, graph):
        """Test in-memory mutation of complex types (lists, dicts)."""

        class Config(graph.DBObject):
            category = "system"
            type = "config"
            settings: dict[str, int]
            tags: list[str]

        config = Config(
            source="test",
            settings={"timeout": 30},
            tags=["prod"]
        )
        await config.insert()

        # Mutate in-memory
        config.settings["timeout"] = 60
        config.settings["retries"] = 3
        config.tags.append("critical")

        # Changes visible in-memory
        assert config.settings == {"timeout": 60, "retries": 3}
        assert config.tags == ["prod", "critical"]

        # Not persisted yet
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[config.id]
        assert reloaded.settings == {"timeout": 30}
        assert reloaded.tags == ["prod"]


@pytest.mark.asyncio
class TestDatabasePersistence:
    """Test explicit database persistence via update()."""

    async def test_update_persists_in_memory_changes(self, graph):
        """Test that update() persists in-memory changes to database."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Modify and persist
        asset.price = 200.0
        await asset.update()

        # Verify persisted
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0

    async def test_partial_update_preserves_other_fields(self, graph):
        """Test that updating one field doesn't affect others."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            name: str

        asset = Asset(source="test", symbol="BTC", price=100.0, name="Bitcoin")
        await asset.insert()

        # Modify only price
        asset.price = 200.0
        await asset.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0
        assert reloaded.name == "Bitcoin"  # Unchanged
        assert reloaded.symbol == "BTC"  # Unchanged

    async def test_multiple_sequential_updates(self, graph):
        """Test multiple sequential update() calls."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # First update
        asset.price = 200.0
        await asset.update()

        # Second update
        asset.price = 300.0
        await asset.update()

        # Third update
        asset.price = 400.0
        await asset.update()

        # Verify final state
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 400.0

    async def test_update_relationship_persistence(self, graph):
        """Test that relationship changes persist correctly."""

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

        # Change and persist
        book.author = author2
        await book.update()

        # Verify persisted
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[book.id]
        assert reloaded.author_id == author2.id

    async def test_update_complex_types_persistence(self, graph):
        """Test that complex type mutations are persisted correctly."""

        class Config(graph.DBObject):
            category = "system"
            type = "config"
            settings: dict[str, int]
            tags: list[str]

        config = Config(
            source="test",
            settings={"timeout": 30},
            tags=["prod"]
        )
        await config.insert()

        # Mutate and persist
        config.settings["timeout"] = 60
        config.tags.append("critical")
        await config.update()

        # Verify persisted
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[config.id]
        assert reloaded.settings["timeout"] == 60
        assert "critical" in reloaded.tags


@pytest.mark.asyncio
class TestReloadAndRollback:
    """Test reloading from database to discard in-memory changes."""

    async def test_reload_discards_in_memory_changes(self, graph):
        """Test that reloading discards unsaved in-memory changes."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Make in-memory changes
        asset.price = 200.0
        assert asset.price == 200.0

        # Reload from database (discards changes)
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 100.0

    async def test_reload_after_partial_changes(self, graph):
        """Test reload after making partial changes to multiple fields."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            name: str

        asset = Asset(source="test", symbol="BTC", price=100.0, name="Bitcoin")
        await asset.insert()
        asset_id = asset.id

        # Change multiple fields in-memory
        asset.price = 200.0
        asset.name = "Bitcoin Core"

        # Reload discards all changes
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 100.0
        assert reloaded.name == "Bitcoin"

    async def test_reload_after_relationship_change(self, graph):
        """Test that reload discards unsaved relationship changes."""

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
        book_id = book.id

        # Change relationship in-memory
        book.author = author2

        # Reload discards change
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[book_id]
        assert reloaded.author_id == author1.id

    async def test_save_then_modify_then_reload(self, graph):
        """Test saving, modifying in-memory, then reloading."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # First update
        asset.price = 200.0
        await asset.update()

        # More in-memory changes (not saved)
        asset.price = 300.0

        # Reload should get the last saved state
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0  # Last saved, not in-memory value


@pytest.mark.asyncio
class TestStateBoundaries:
    """Test boundaries between in-memory state and database state."""

    async def test_insert_creates_database_state(self, graph):
        """Test that insert() creates the database state."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Before insert: no database state
        asset = Asset(source="test", symbol="BTC", price=100.0)
        assert asset.id is None

        # After insert: database state exists
        await asset.insert()
        assert asset.id is not None

        # Verify in database
        graph.registry.clear()
        await graph.load()
        assert asset.id in graph.registry

    async def test_delete_removes_database_state(self, graph):
        """Test that delete() removes database state."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()
        asset_id = asset.id

        # Delete removes database state
        await asset.delete()

        # Verify removed from database
        graph.registry.clear()
        await graph.load()
        assert asset_id not in graph.registry

    async def test_upsert_behavior_with_modifications(self, graph):
        """Test upsert() with various modification scenarios."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # First upsert (insert path)
        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.upsert()
        assert asset.id is not None

        # Modify and upsert again (update path)
        asset.price = 200.0
        await asset.upsert()

        # Verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0

    async def test_object_state_across_multiple_sessions(self, graph):
        """Test that object state is consistent across multiple load cycles."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Create and save
        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        original_id = asset.id

        # Session 1: Load and modify, then save
        graph.registry.clear()
        await graph.load()
        obj1 = graph.registry[original_id]
        obj1.price = 200.0
        await obj1.update()

        # Session 2: Load and verify
        graph.registry.clear()
        await graph.load()
        obj2 = graph.registry[original_id]
        assert obj2.price == 200.0

        # Session 3: Load, modify but don't save
        graph.registry.clear()
        await graph.load()
        obj3 = graph.registry[original_id]
        obj3.price = 300.0
        # Don't update

        # Session 4: Load and verify unsaved changes were lost
        graph.registry.clear()
        await graph.load()
        obj4 = graph.registry[original_id]
        assert obj4.price == 200.0  # Not 300.0

    async def test_in_memory_state_independence_per_object(self, graph):
        """Test that in-memory state is independent per object instance."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Create two different assets
        btc = Asset(source="test", symbol="BTC", price=100.0)
        eth = Asset(source="test", symbol="ETH", price=50.0)
        await btc.insert()
        await eth.insert()

        # Modify both in-memory
        btc.price = 200.0
        eth.price = 100.0

        # Update only btc
        await btc.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        btc_reloaded = graph.registry[btc.id]
        eth_reloaded = graph.registry[eth.id]

        assert btc_reloaded.price == 200.0  # Persisted
        assert eth_reloaded.price == 50.0  # Not persisted
