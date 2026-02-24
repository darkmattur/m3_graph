"""
Tests for CRUD operations (Create, Read, Update, Delete).

Tests cover:
- insert() operation
- update() operation
- upsert() operation
- delete() operation
- Error handling for invalid states
- Registry management during CRUD
- Database persistence
"""
import pytest
from decimal import Decimal


@pytest.mark.asyncio
class TestCRUD:
    """Test CRUD operations on DBObject."""

    async def test_insert_basic(self, graph):
        """Test basic insert operation."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str


        asset = Asset(source="test", symbol="BTC", name="Bitcoin")

        # Before insert
        assert asset.id is None
        
        # Insert
        await asset.insert()

        # After insert
        assert asset.id is not None
        assert isinstance(asset.id, int)
        assert asset.id > 0

        # Should be in registry
        assert asset.id in graph.registry
        assert graph.registry[asset.id] is asset

    async def test_insert_with_relationships(self, graph):
        """Test insert with foreign key relationships."""
        from m3_graph.link import Link

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

        book = Book(source="test", title="Test Book", author=author)
        await book.insert()

        # Verify both have IDs
        assert book.id is not None
        assert book.author_id == author.id

    async def test_insert_already_inserted(self, graph):
        """Test that inserting an already-inserted object raises error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Try to insert again
        with pytest.raises(ValueError, match="existing id"):
            await asset.insert()

    async def test_update_basic(self, graph):
        """Test basic update operation."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str


        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        print(asset.id)

        original_id = asset.id

        # Update name
        asset.name = "Bitcoin Core"
        await asset.update()
        
        # ID should remain the same
        assert asset.id == original_id

        # Verify update persisted (reload from DB)
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        reloaded = graph.registry[original_id]
        assert reloaded.name == "Bitcoin Core"

    async def test_update_without_id(self, graph):
        """Test that updating object without ID raises error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")

        # Try to update without insert
        with pytest.raises(ValueError, match="without id"):
            await asset.update()

    async def test_update_relationships(self, graph):
        """Test updating relationship fields."""
        from m3_graph.link import Link

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

        book = Book(source="test", title="Test Book", author=author1)
        await book.insert()

        # Change author
        book.author = author2
        await book.update()

        # Verify update persisted
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        reloaded_book = graph.registry[book.id]
        assert reloaded_book.author_id == author2.id

    async def test_upsert_insert_path(self, graph):
        """Test upsert when object doesn't have ID (insert path)."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")
        assert asset.id is None

        await asset.upsert()

        # Should have inserted
        assert asset.id is not None
        assert asset.id in graph.registry

    async def test_upsert_update_path(self, graph):
        """Test upsert when object has ID (update path)."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str


        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        original_id = asset.id

        # Modify and upsert
        asset.name = "Bitcoin Core"
        await asset.upsert()

        # Should have updated, not created new
        assert asset.id == original_id

        # Verify
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        reloaded = graph.registry[original_id]
        assert reloaded.name == "Bitcoin Core"

    async def test_delete_basic(self, graph):
        """Test basic delete operation."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id
        assert asset_id in graph.registry

        # Delete
        await asset.delete()

        # Should be removed from registry
        assert asset_id not in graph.registry
        assert asset.id is None

        # Verify deleted from database
        graph.registry.clear()
        await graph.load()
        assert asset_id not in graph.registry

    async def test_delete_without_id(self, graph):
        """Test that deleting object without ID raises error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")

        with pytest.raises(ValueError, match="without id"):
            await asset.delete()

    async def test_crud_with_decimal(self, graph):
        """Test CRUD operations preserve Decimal precision."""

        class Account(graph.DBObject):
            category = "financial"
            type = "account"
            balance: Decimal


        account = Account(source="test", balance=Decimal("123.45"))
        await account.insert()

        # Reload and verify precision
        graph.registry.clear()
        await graph.load()

        reloaded = list(graph.registry.values())[0]
        assert isinstance(reloaded.balance, Decimal)
        assert reloaded.balance == Decimal("123.45")

    async def test_crud_with_complex_types(self, graph):
        """Test CRUD with complex types (lists, dicts)."""

        class Config(graph.DBObject):
            category = "system"
            type = "config"
            settings: dict[str, int]
            tags: list[str]


        config = Config(
            source="test",
            settings={"timeout": 30, "retries": 3},
            tags=["production", "critical"]
        )
        await config.insert()

        # Update
        config.settings["timeout"] = 60
        config.tags.append("monitored")
        await config.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = list(graph.registry.values())[0]
        assert reloaded.settings == {"timeout": 60, "retries": 3}
        assert reloaded.tags == ["production", "critical", "monitored"]

    async def test_insert_sets_registry_type(self, graph):
        """Test that insert registers object in type-specific registry."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Should be in type registry
        assert "asset" in graph.registry_type
        assert asset.id in graph.registry_type["asset"]
        assert graph.registry_type["asset"][asset.id] is asset

    async def test_delete_removes_from_type_registry(self, graph):
        """Test that delete removes object from type-specific registry."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id
        assert asset_id in graph.registry_type["asset"]

        await asset.delete()

        assert asset_id not in graph.registry_type.get("asset", {})

    async def test_multiple_inserts_same_type(self, graph):
        """Test inserting multiple objects of same type."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        btc = Asset(source="test", symbol="BTC")
        eth = Asset(source="test", symbol="ETH")

        await btc.insert()
        await eth.insert()

        # Both should be in registry
        assert btc.id in graph.registry
        assert eth.id in graph.registry
        assert btc.id != eth.id

        # Both should be in type registry
        assert btc.id in graph.registry_type["asset"]
        assert eth.id in graph.registry_type["asset"]

    async def test_update_nullable_field_to_none(self, graph):
        """Test updating a field to None."""

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            description: str | None = None


        item = Item(source="test", name="Item", description="Original")
        await item.insert()

        item.description = None
        await item.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = list(graph.registry.values())[0]
        assert reloaded.description is None

    async def test_source_field_persistence(self, graph):
        """Test that source field is persisted and retrieved."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str


        asset = Asset(source="manual_entry", symbol="BTC")
        await asset.insert()

        # Reload
        graph.registry.clear()
        await graph.load()

        reloaded = list(graph.registry.values())[0]
        assert reloaded.source == "manual_entry"
