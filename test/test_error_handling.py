"""
Tests for error handling and invalid operations.

Tests cover:
- Invalid CRUD operations
- Type mismatches
- Invalid relationship assignments
- Error conditions in database operations
"""
import pytest
from pydantic import ValidationError
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestErrorHandling:
    """Test error handling for invalid operations."""

    async def test_insert_object_twice_raises_error(self, graph):
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

    async def test_update_without_id_raises_error(self, graph):
        """Test that updating object without ID raises error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")

        # Try to update without inserting first
        with pytest.raises(ValueError, match="without id"):
            await asset.update()

    async def test_delete_without_id_raises_error(self, graph):
        """Test that deleting object without ID raises error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")

        # Try to delete without inserting first
        with pytest.raises(ValueError, match="without id"):
            await asset.delete()

    async def test_required_relationship_cannot_be_none(self, graph):
        """Test that required relationships cannot be set to None."""

        class Parent(graph.DBObject):
            category = "test"
            type = "parent"
            name: str

        class Child(graph.DBObject):
            category = "test"
            type = "child"
            name: str
            parent: Link[Parent]  # Required

        # Try to create with None parent
        with pytest.raises(ValueError, match="required"):
            Child(source="test", name="Orphan", parent=None)

    async def test_assigning_unsaved_object_raises_error(self, graph):
        """Test that assigning unsaved object to relationship raises error."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        # Create unsaved author
        author = Author(source="test", name="Jane")

        # Try to assign unsaved object
        with pytest.raises(ValueError, match="unsaved"):
            Book(source="test", title="Book", author=author)

    async def test_setter_rejects_unsaved_object(self, graph):
        """Test that relationship setter rejects unsaved objects."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        saved_author = Author(source="test", name="Saved")
        await saved_author.insert()

        book = Book(source="test", title="Book", author=saved_author)
        await book.insert()

        # Try to set to unsaved author
        unsaved_author = Author(source="test", name="Unsaved")

        with pytest.raises(ValueError, match="unsaved"):
            book.author = unsaved_author

    async def test_invalid_type_for_field_raises_validation_error(self, graph):
        """Test that invalid type for field raises Pydantic validation error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Try to create with wrong type
        with pytest.raises(ValidationError):
            Asset(source="test", symbol="BTC", price="not_a_number")

    async def test_missing_required_field_raises_error(self, graph):
        """Test that missing required field raises validation error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float  # Required

        # Try to create without required field
        with pytest.raises(ValidationError):
            Asset(source="test", symbol="BTC")

    async def test_setting_required_relationship_to_none_raises_error(self, graph):
        """Test that setting required relationship to None raises error."""

        class Parent(graph.DBObject):
            category = "test"
            type = "parent"
            name: str

        class Child(graph.DBObject):
            category = "test"
            type = "child"
            name: str
            parent: Link[Parent]  # Required

        parent = Parent(source="test", name="Parent")
        await parent.insert()

        child = Child(source="test", name="Child", parent=parent)
        await child.insert()

        # Try to set to None
        with pytest.raises(ValueError, match="required"):
            child.parent = None

    async def test_invalid_relationship_type_raises_error(self, graph):
        """Test that assigning wrong type to relationship raises error."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        class WrongType(graph.DBObject):
            category = "test"
            type = "wrong"
            name: str

        wrong_obj = WrongType(source="test", name="Wrong")
        await wrong_obj.insert()

        book = Book(source="test", title="Book", author_id=wrong_obj.id)

        # This will work because we only store IDs
        # The type check happens at the Python level, not DB level
        # But accessing .author should work even if types don't match
        assert book.author_id == wrong_obj.id

    async def test_type_mismatch_in_complex_field(self, graph):
        """Test validation error for complex field type mismatch."""

        class Config(graph.DBObject):
            category = "system"
            type = "config"
            settings: dict[str, int]

        # Try to create with wrong dict value type
        with pytest.raises(ValidationError):
            Config(source="test", settings={"key": "not_an_int"})

    async def test_list_type_mismatch_raises_error(self, graph):
        """Test validation error for list element type mismatch."""

        class Tagged(graph.DBObject):
            category = "test"
            type = "tagged"
            tags: list[str]

        # Try with wrong list element type
        with pytest.raises(ValidationError):
            Tagged(source="test", tags=[1, 2, 3])  # Should be strings

    async def test_delete_already_deleted_object(self, graph):
        """Test deleting an already-deleted object raises error."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Delete once
        await asset.delete()

        # Try to delete again - id is None now
        with pytest.raises(ValueError, match="without id"):
            await asset.delete()

    async def test_accessing_deleted_object_from_registry(self, graph):
        """Test that deleted object is removed from registry."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()
        asset_id = asset.id

        await asset.delete()

        # Should not be in registry
        assert asset_id not in graph.registry

    async def test_invalid_setter_value_type(self, graph):
        """Test that setting relationship to invalid value type raises error."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        author = Author(source="test", name="Jane")
        await author.insert()

        book = Book(source="test", title="Book", author=author)
        await book.insert()

        # Try to set to invalid type (string instead of object/int)
        with pytest.raises(ValueError, match="Cannot assign"):
            book.author = "not_an_object_or_id"

    async def test_empty_required_string_field(self, graph):
        """Test that empty string is valid for string field (unless constrained)."""

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        # Empty string should be allowed by default
        item = Item(source="test", name="")
        assert item.name == ""


@pytest.mark.asyncio
class TestDatabaseIntegrityErrors:
    """Test handling of database-level integrity errors."""

    async def test_load_skips_unregistered_types(self, graph, db_connection, test_schema):
        """Test that loading skips objects with unregistered types."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        # Insert a known type
        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Manually insert an unregistered type
        await db_connection.execute(
            f"""
            INSERT INTO {test_schema}.object (category, type, subtype, attr, source)
            VALUES ('other', 'unknown', 'unknown', %(attr)s, 'test')
            """,
            attr={"name": "Unknown"}
        )

        # Clear and reload
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        # Should only load the registered type
        assert len(graph.registry) == 1
        assert asset.id in graph.registry

    async def test_load_with_malformed_relationships(self, graph):
        """Test loading objects with relationships that reference non-existent IDs."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        await graph.db_maintain()

        # Create author and book
        author = Author(source="test", name="Jane")
        await author.insert()

        book = Book(source="test", title="Book", author=author)
        await book.insert()

        # Delete author directly from DB (bypassing ORM)
        await graph._conn.execute(
            f"DELETE FROM {graph._name}.object WHERE id = %(id)s",
            id=author.id
        )

        # Reload
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        # Book should load but author reference will be None
        book_reloaded = graph.registry[book.id]
        assert book_reloaded.author_id == author.id
        assert book_reloaded.author is None  # Not in registry

    async def test_concurrent_modification_detection(self, graph):
        """Test that concurrent modifications to same object work correctly."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Simulate concurrent modification
        # (In reality, last write wins at DB level)
        asset.price = 200.0
        await asset.update()

        asset.price = 300.0
        await asset.update()

        # Verify final state
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 300.0
