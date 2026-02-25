"""
Database-level functionality tests.

Tests cover:
- Database triggers for backlinks and history
- History tracking on insert/update/delete
- Error handling for database operations
- Type validation and constraints
- Database integrity errors
"""
import pytest
from decimal import Decimal
from pydantic import ValidationError
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestDatabaseTriggers:
    """Test database trigger functionality for backlinks and history."""

    async def test_backlink_sync_on_insert(self, graph):
        """Test that backlinks are synchronized when objects are inserted."""
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

        # Register relationship metadata
        await graph._conn.execute(
            f"""
            INSERT INTO {graph._name}.meta_relationship (category, type, subtype, forward, back)
            VALUES ('test', 'book', 'book', %(forward)s, %(back)s)
            ON CONFLICT (category, type, subtype) DO UPDATE SET forward = EXCLUDED.forward, back = EXCLUDED.back
            """,
            forward={"author_id": "books_ids"},
            back=[]
        )

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Test Book", author=author)
        await book.insert()

        # Reload to get trigger-updated backlinks
        graph.registry.clear()
        await graph.load()

        author_reloaded = graph.registry[author.id]

        # Backlink should be populated by trigger
        assert len(author_reloaded.books_ids) >= 1
        assert book.id in author_reloaded.books_ids

    async def test_backlink_sync_on_update(self, graph):
        """Test that backlinks are synchronized when relationships are updated."""
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

        # Register relationship metadata
        await graph._conn.execute(
            f"""
            INSERT INTO {graph._name}.meta_relationship (category, type, subtype, forward, back)
            VALUES ('test', 'book', 'book', %(forward)s, %(back)s)
            ON CONFLICT (category, type, subtype) DO UPDATE SET forward = EXCLUDED.forward, back = EXCLUDED.back
            """,
            forward={"author_id": "books_ids"},
            back=[]
        )

        author1 = Author(source="test", name="Author 1")
        author2 = Author(source="test", name="Author 2")
        await author1.insert()
        await author2.insert()

        book = Book(source="test", title="Test Book", author=author1)
        await book.insert()

        # Change author
        book.author = author2
        await book.update()

        # Reload
        graph.registry.clear()
        await graph.load()

        author1_reloaded = graph.registry[author1.id]
        author2_reloaded = graph.registry[author2.id]

        # Author1 should no longer have the book
        assert book.id not in author1_reloaded.books_ids

        # Author2 should have the book
        assert book.id in author2_reloaded.books_ids

    async def test_backlink_sync_on_delete(self, graph):
        """Test that backlinks are cleaned up when objects are deleted."""
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

        # Register relationship metadata
        await graph._conn.execute(
            f"""
            INSERT INTO {graph._name}.meta_relationship (category, type, subtype, forward, back)
            VALUES ('test', 'book', 'book', %(forward)s, %(back)s)
            ON CONFLICT (category, type, subtype) DO UPDATE SET forward = EXCLUDED.forward, back = EXCLUDED.back
            """,
            forward={"author_id": "books_ids"},
            back=[]
        )

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Test Book", author=author)
        await book.insert()

        # Delete book
        book_id = book.id
        await book.delete()

        # Reload
        graph.registry.clear()
        await graph.load()

        author_reloaded = graph.registry[author.id]

        # Book should be removed from backlinks
        assert book_id not in author_reloaded.books_ids


@pytest.mark.asyncio
class TestHistoryTracking:
    """Test history tracking functionality."""

    async def test_history_on_insert(self, graph):
        """Test that history entry is created on insert."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Check history table
        history = await graph._conn.query(
            f"SELECT * FROM {graph._name}.history WHERE id = %(id)s",
            id=asset.id
        )

        assert len(history) == 1
        assert history[0]['id'] == asset.id
        assert history[0]['attr']['symbol'] == "BTC"
        assert history[0]['deleted'] is False

    async def test_history_on_update(self, graph):
        """Test that history is tracked on update."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        # Update
        asset.name = "Bitcoin Core"
        await asset.update()

        # Check history - should have 2 entries
        history = await graph._conn.query(
            f"SELECT * FROM {graph._name}.history WHERE id = %(id)s ORDER BY validity",
            id=asset.id
        )

        assert len(history) == 2

        # First entry should have old value with closed validity
        assert history[0]['attr']['name'] == "Bitcoin"
        assert history[0]['validity'].upper != 'infinity'

        # Second entry should have new value with open validity
        assert history[1]['attr']['name'] == "Bitcoin Core"
        assert history[1]['validity'].upper == 'infinity'

    async def test_history_on_delete(self, graph):
        """Test that history is updated on delete."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id

        # Delete
        await asset.delete()

        # Check history - validity should be closed
        history = await graph._conn.query(
            f"SELECT * FROM {graph._name}.history WHERE id = %(id)s",
            id=asset_id
        )

        assert len(history) == 1
        assert history[0]['validity'].upper != 'infinity'

    async def test_history_no_change_no_entry(self, graph):
        """Test that history doesn't create new entry if nothing changed."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        # Update with same values
        await asset.update()

        # Should still have only 1 history entry
        history = await graph._conn.query(
            f"SELECT * FROM {graph._name}.history WHERE id = %(id)s",
            id=asset.id
        )

        assert len(history) == 1

    async def test_history_preserves_all_attributes(self, graph):
        """Test that history preserves all object attributes."""
        class ComplexObj(graph.DBObject):
            category = "test"
            type = "complex"
            name: str
            amount: Decimal
            tags: list[str]
            metadata: dict[str, int]

        obj = ComplexObj(
            source="test",
            name="Test",
            amount=Decimal("123.45"),
            tags=["a", "b"],
            metadata={"x": 1}
        )
        await obj.insert()

        # Check history preserves all types
        history = await graph._conn.query(
            f"SELECT * FROM {graph._name}.history WHERE id = %(id)s",
            id=obj.id
        )

        assert history[0]['attr']['name'] == "Test"
        assert Decimal(str(history[0]['attr']['amount'])) == Decimal("123.45")
        assert history[0]['attr']['tags'] == ["a", "b"]
        assert history[0]['attr']['metadata'] == {"x": 1}


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


@pytest.mark.asyncio
class TestDatabaseIntegrity:
    """Test database-level integrity errors and edge cases."""

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
        await graph.load()

        # Book should load but author reference will be None
        book_reloaded = graph.registry[book.id]
        assert book_reloaded.author_id == author.id
        assert book_reloaded.author is None  # Not in registry

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
