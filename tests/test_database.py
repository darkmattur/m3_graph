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
        with pytest.raises(ValueError, match="'parent' is required"):
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
        with pytest.raises(ValueError, match="parent is required"):
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


@pytest.mark.asyncio
class TestDataIntegrity:
    """Test data integrity constraints and edge cases."""

    async def test_none_vs_missing_field_in_jsonb(self, graph):
        """Test difference between field=None and field not present in JSONB."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            description: str | None = None

        # Create with None description
        item1 = Item(source="test", name="Item1", description=None)
        await item1.insert()

        # Create without specifying description
        item2 = Item(source="test", name="Item2")
        await item2.insert()

        # Check database representation
        rows = await graph._conn.query(
            f"SELECT id, attr FROM {graph._name}.object WHERE id IN (%(id1)s, %(id2)s) ORDER BY id",
            id1=item1.id,
            id2=item2.id
        )

        # Both should have the same representation (None fields excluded from JSONB)
        assert 'description' not in rows[0]['attr']
        assert 'description' not in rows[1]['attr']

    async def test_update_field_to_none_removes_from_jsonb(self, graph):
        """Test that updating a field to None removes it from JSONB."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            description: str | None = None

        item = Item(source="test", name="Item", description="Has description")
        await item.insert()

        # Verify description is in JSONB
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._name}.object WHERE id = %(id)s",
            id=item.id
        )
        assert 'description' in row[0]['attr']

        # Update to None
        item.description = None
        await item.update()

        # Should be removed from JSONB
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._name}.object WHERE id = %(id)s",
            id=item.id
        )
        assert 'description' not in row[0]['attr']

    async def test_relationship_id_persists_as_none(self, graph):
        """Test that nullable relationship IDs are stored as None in JSONB."""
        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category] | None = None

        item = Item(source="test", name="Item")
        await item.insert()

        # Check JSONB - category_obj_id should be present with null value
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._name}.object WHERE id = %(id)s",
            id=item.id
        )
        # According to _get_attr, None values are filtered out unless it's a _id field
        assert 'category_obj_id' in row[0]['attr']
        assert row[0]['attr']['category_obj_id'] is None

    async def test_excluded_attrs_not_in_database(self, graph):
        """Test that excluded attributes are never stored in database."""
        class Cache(graph.DBObject):
            category = "test"
            type = "cache"
            key: str
            value: str
            _cached_at: str = "temp"
            excluded_attrs = {"_cached_at"}

        cache = Cache(source="test", key="key1", value="value1", _cached_at="2024-01-01")
        await cache.insert()

        # Verify excluded attr not in database
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._name}.object WHERE id = %(id)s",
            id=cache.id
        )
        assert '_cached_at' not in row[0]['attr']
        assert 'key' in row[0]['attr']
        assert 'value' in row[0]['attr']

    async def test_decimal_precision_preserved(self, graph):
        """Test that Decimal precision is preserved through database round-trip."""
        from decimal import Decimal

        class Account(graph.DBObject):
            category = "financial"
            type = "account"
            balance: Decimal

        # Test various decimal precisions
        test_values = [
            Decimal("123.45"),
            Decimal("0.001"),
            Decimal("999999999.999999999"),
            Decimal("0.00000001"),
        ]

        accounts = []
        for val in test_values:
            acc = Account(source="test", balance=val)
            await acc.insert()
            accounts.append((acc.id, val))

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        for acc_id, expected_val in accounts:
            acc = graph.registry[acc_id]
            assert acc.balance == expected_val
            assert isinstance(acc.balance, Decimal)

    async def test_list_and_dict_deep_equality(self, graph):
        """Test that complex nested structures are preserved."""
        class Config(graph.DBObject):
            category = "test"
            type = "config"
            settings: dict
            tags: list

        complex_dict = {
            "nested": {
                "level2": {
                    "level3": ["a", "b", "c"]
                }
            },
            "numbers": [1, 2, 3]
        }

        complex_list = [
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second", "sub": [1, 2, 3]}
        ]

        config = Config(source="test", settings=complex_dict, tags=complex_list)
        await config.insert()

        # Reload and verify deep equality
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[config.id]
        assert reloaded.settings == complex_dict
        assert reloaded.tags == complex_list

    async def test_empty_collections_preserved(self, graph):
        """Test that empty lists and dicts are preserved."""
        class Container(graph.DBObject):
            category = "test"
            type = "container"
            items: list
            metadata: dict

        container = Container(source="test", items=[], metadata={})
        await container.insert()

        # Verify in database
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._name}.object WHERE id = %(id)s",
            id=container.id
        )
        assert row[0]['attr']['items'] == []
        assert row[0]['attr']['metadata'] == {}

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[container.id]
        assert reloaded.items == []
        assert reloaded.metadata == {}

    async def test_attribute_with_underscore_prefix(self, graph):
        """Test that attributes with underscore prefix are NOT stored (Pydantic private fields)."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            _internal_id: int = 0  # Private field, not persisted

        item = Item(source="test", name="Item", _internal_id=42)
        await item.insert()

        # Private fields (starting with _) are excluded by Pydantic model_dump
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._name}.object WHERE id = %(id)s",
            id=item.id
        )
        assert '_internal_id' not in row[0]['attr']  # Not persisted
        assert 'name' in row[0]['attr']

        # Reload and verify - private field not restored from DB
        graph.registry.clear()
        await graph.load()
        reloaded = graph.registry[item.id]
        assert reloaded._internal_id == 0  # Default value, not 42

    async def test_backlink_ids_not_stored_in_database(self, graph):
        """Test that backlink arrays (_ids fields) are not stored in attr JSONB."""
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

        # Check that books_ids is not in JSONB (managed by triggers)
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._name}.object WHERE id = %(id)s",
            id=author.id
        )
        assert 'books_ids' not in row[0]['attr']

    async def test_large_jsonb_document(self, graph):
        """Test handling of large JSONB documents."""
        class LargeDoc(graph.DBObject):
            category = "test"
            type = "large_doc"
            name: str
            data: dict

        # Create a large nested structure
        large_data = {
            f"key_{i}": {
                "nested": [j for j in range(100)]
            }
            for i in range(100)
        }

        doc = LargeDoc(source="test", name="Large", data=large_data)
        await doc.insert()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[doc.id]
        assert reloaded.data == large_data

    async def test_special_json_values(self, graph):
        """Test handling of special JSON values (null, boolean, numeric edge cases)."""
        class Special(graph.DBObject):
            category = "test"
            type = "special"
            values: list

        special_values = [
            None,
            True,
            False,
            0,
            -1,
            1.5,
            -1.5,
            "",
            "null",
            "true",
            "false",
        ]

        obj = Special(source="test", values=special_values)
        await obj.insert()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[obj.id]
        assert reloaded.values == special_values

    async def test_unicode_in_jsonb(self, graph):
        """Test that Unicode characters are properly stored and retrieved."""
        class Text(graph.DBObject):
            category = "test"
            type = "text"
            content: str
            languages: list[str]

        obj = Text(
            source="test",
            content="Hello: 你好, Здравствуй, مرحبا, שלום",
            languages=["中文", "Русский", "العربية", "עברית"]
        )
        await obj.insert()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[obj.id]
        assert reloaded.content == "Hello: 你好, Здравствуй, مرحبا, שלום"
        assert reloaded.languages == ["中文", "Русский", "العربية", "עברית"]

    async def test_history_preserves_all_data_types(self, graph):
        """Test that history table preserves all data types correctly."""
        from decimal import Decimal

        class ComplexObj(graph.DBObject):
            category = "test"
            type = "complex"
            name: str
            amount: Decimal
            tags: list[str]
            metadata: dict
            active: bool

        obj = ComplexObj(
            source="test",
            name="Test",
            amount=Decimal("123.45"),
            tags=["a", "b"],
            metadata={"key": "value"},
            active=True
        )
        await obj.insert()

        # Check history
        history = await graph._conn.query(
            f"SELECT * FROM {graph._name}.history WHERE id = %(id)s",
            id=obj.id
        )

        assert len(history) == 1
        hist = history[0]
        assert hist['attr']['name'] == "Test"
        assert Decimal(str(hist['attr']['amount'])) == Decimal("123.45")
        assert hist['attr']['tags'] == ["a", "b"]
        assert hist['attr']['metadata'] == {"key": "value"}
        assert hist['attr']['active'] is True
