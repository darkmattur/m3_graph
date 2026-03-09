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
            INSERT INTO {graph._schema}.meta (category, type, subtype, forward, back)
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
            INSERT INTO {graph._schema}.meta (category, type, subtype, forward, back)
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
            INSERT INTO {graph._schema}.meta (category, type, subtype, forward, back)
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
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
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
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s ORDER BY validity",
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
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
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
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
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
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
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

    async def test_assigning_unsaved_object_allowed(self, graph):
        """Test that assigning unsaved object to relationship is allowed and auto-saved on upsert."""
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

        # Should allow unsaved object
        book = Book(source="test", title="Book", author=author)

        # Author should be accessible
        assert book.author.name == "Jane"
        assert author.id is None  # Not yet saved

        # Insert with unsaved refs should fail
        with pytest.raises(ValueError, match="unsaved references"):
            await book.insert()

        # Upsert should auto-save author
        await book.upsert()

        assert author.id is not None  # Author was auto-saved
        assert book.author_id == author.id

    async def test_insert_rejects_unsaved_references(self, graph):
        """Test that insert() explicitly rejects objects with unsaved references."""
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
        book = Book(source="test", title="Book", author=author)

        # Should explicitly reject
        with pytest.raises(ValueError, match="Cannot insert object with unsaved references: author"):
            await book.insert()

    async def test_update_rejects_unsaved_references(self, graph):
        """Test that update() explicitly rejects objects with unsaved references."""
        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        # Create and save with valid author
        author1 = Author(source="test", name="Author 1")
        await author1.insert()

        book = Book(source="test", title="Book", author=author1)
        await book.insert()

        # Change to unsaved author
        author2 = Author(source="test", name="Author 2")
        book.author = author2

        # Update should reject
        with pytest.raises(ValueError, match="Cannot update object with unsaved references: author"):
            await book.update()

    async def test_upsert_cascading_save_multi_level(self, graph):
        """Test that upsert() cascades through multiple levels of unsaved references."""
        class Publisher(graph.DBObject):
            category = "test"
            type = "publisher"
            name: str

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str
            publisher: Link[Publisher]

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        # Build entire graph in memory
        publisher = Publisher(source="test", name="BigPub")
        author = Author(source="test", name="Jane", publisher=publisher)
        book = Book(source="test", title="Book", author=author)

        # All unsaved
        assert publisher.id is None
        assert author.id is None
        assert book.id is None

        # Single upsert should cascade through all levels
        await book.upsert()

        # All should now be saved
        assert publisher.id is not None
        assert author.id is not None
        assert book.id is not None
        assert book.author_id == author.id
        assert author.publisher_id == publisher.id

    async def test_upsert_with_mix_of_saved_and_unsaved(self, graph):
        """Test that upsert() handles mix of saved and unsaved references."""
        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category]

        # Save one category
        saved_cat = Category(source="test", name="Saved")
        await saved_cat.insert()

        # Create item with saved reference
        item = Item(source="test", name="Item", category_obj=saved_cat)
        await item.insert()

        # Change to unsaved reference
        unsaved_cat = Category(source="test", name="Unsaved")
        item.category_obj = unsaved_cat

        # Upsert should save the unsaved category
        await item.upsert()

        assert unsaved_cat.id is not None
        assert item.category_obj_id == unsaved_cat.id

    async def test_unsaved_refs_cleared_after_upsert(self, graph):
        """Test that _unsaved_refs is properly cleared after upsert."""
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
        book = Book(source="test", title="Book", author=author)

        # Should have unsaved ref
        assert hasattr(book, '_unsaved_refs')
        assert 'author' in book._unsaved_refs

        await book.upsert()

        # Should be cleared after upsert
        assert len(book._unsaved_refs) == 0

        # Second upsert should work without issues
        book.title = "Updated Book"
        await book.upsert()  # Should not fail

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
            f"DELETE FROM {graph._schema}.object WHERE id = %(id)s",
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
            f"SELECT id, attr FROM {graph._schema}.object WHERE id IN (%(id1)s, %(id2)s) ORDER BY id",
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
            f"SELECT attr FROM {graph._schema}.object WHERE id = %(id)s",
            id=item.id
        )
        assert 'description' in row[0]['attr']

        # Update to None
        item.description = None
        await item.update()

        # Should be removed from JSONB
        row = await graph._conn.query(
            f"SELECT attr FROM {graph._schema}.object WHERE id = %(id)s",
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
            f"SELECT attr FROM {graph._schema}.object WHERE id = %(id)s",
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
            f"SELECT attr FROM {graph._schema}.object WHERE id = %(id)s",
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
            f"SELECT attr FROM {graph._schema}.object WHERE id = %(id)s",
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
            f"SELECT attr FROM {graph._schema}.object WHERE id = %(id)s",
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
            f"SELECT attr FROM {graph._schema}.object WHERE id = %(id)s",
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
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
            id=obj.id
        )

        assert len(history) == 1
        hist = history[0]
        assert hist['attr']['name'] == "Test"
        assert Decimal(str(hist['attr']['amount'])) == Decimal("123.45")
        assert hist['attr']['tags'] == ["a", "b"]
        assert hist['attr']['metadata'] == {"key": "value"}
        assert hist['attr']['active'] is True


@pytest.mark.asyncio
class TestTypeInheritance:
    """Test type inheritance metadata tracking."""

    async def test_register_type_with_no_parents(self, graph):
        """Test registering a type with no parent types."""
        class BaseType(graph.DBObject):
            category = "test"
            type = "base_unique_type"
            name: str

        await BaseType.maintain()

        # Check meta table
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'base_unique_type'"
        )

        assert len(result) == 1
        assert result[0]['type'] == "base_unique_type"
        assert result[0]['subtype'] == "base_unique_type"
        assert result[0]['parent_types'] == []
        assert result[0]['descendant_types'] == ["base_unique_type"]

    async def test_register_type_with_single_parent(self, graph):
        """Test registering a type that inherits from another."""
        class Animal(graph.DBObject):
            category = "biology"
            type = "animal"
            name: str

        class Dog(Animal):
            type = "dog"
            breed: str

        await Animal.maintain()
        await Dog.maintain()

        # Check parent type — dog should have been cascaded into animal's descendants
        animal_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'animal'"
        )
        assert animal_meta[0]['parent_types'] == []
        assert set(animal_meta[0]['descendant_types']) == {"animal", "dog"}

        # Check child type
        dog_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'dog'"
        )
        assert dog_meta[0]['parent_types'] == ["animal"]
        assert dog_meta[0]['descendant_types'] == ["dog"]

    async def test_register_type_with_multiple_inheritance_levels(self, graph):
        """Test registering types with multiple inheritance levels."""
        class Vehicle(graph.DBObject):
            category = "transport"
            type = "vehicle"
            name: str

        class Car(Vehicle):
            type = "car"
            doors: int

        class SportsCar(Car):
            type = "sports_car"
            top_speed: int

        await Vehicle.maintain()
        await Car.maintain()
        await SportsCar.maintain()

        # Check grandchild type
        sports_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'sports_car'"
        )
        assert set(sports_meta[0]['parent_types']) == {"car", "vehicle"}
        assert sports_meta[0]['descendant_types'] == ["sports_car"]

        # Check that ancestors accumulated their descendants
        car_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'car'"
        )
        assert set(car_meta[0]['descendant_types']) == {"car", "sports_car"}

        vehicle_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'vehicle'"
        )
        assert set(vehicle_meta[0]['descendant_types']) == {"vehicle", "car", "sports_car"}

    async def test_update_type_inheritance_on_maintain(self, graph):
        """Test that calling maintain() updates inheritance metadata."""
        class Base(graph.DBObject):
            category = "test"
            type = "base"
            name: str

        await Base.maintain()

        # First check
        result1 = await graph._conn.query(
            f"SELECT parent_types FROM {graph._schema}.meta WHERE type = 'base'"
        )
        assert result1[0]['parent_types'] == []

        # Call maintain again (should not error, just update)
        await Base.maintain()

        result2 = await graph._conn.query(
            f"SELECT parent_types FROM {graph._schema}.meta WHERE type = 'base'"
        )
        assert result2[0]['parent_types'] == []

    async def test_inheritance_with_different_categories(self, graph):
        """Test that inheritance tracking works with different categories."""
        class Entity(graph.DBObject):
            category = "core"
            type = "entity"
            name: str

        class Person(Entity):
            category = "people"
            type = "person"
            age: int

        await Entity.maintain()
        await Person.maintain()

        # Check that person has entity as parent despite different category
        person_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'person' AND category = 'people'"
        )
        assert "entity" in person_meta[0]['parent_types']

    async def test_inheritance_metadata_separate_from_relationships(self, graph):
        """Test that inheritance metadata doesn't interfere with relationship metadata."""
        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        await Author.maintain()
        await Book.maintain()

        # Check that both relationship and inheritance data are present
        book_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'book'"
        )

        assert book_meta[0]['forward'] == {"author_id": None}
        assert book_meta[0]['parent_types'] == []
        assert book_meta[0]['descendant_types'] == ["book"]

    async def test_subtype_differs_from_type_inheritance(self, graph):
        """Test inheritance when subtype differs from type."""
        class Animal(graph.DBObject):
            category = "biology"
            type = "animal"
            subtype = "base_animal"
            name: str

        class Dog(Animal):
            type = "dog"
            subtype = "canine"
            breed: str

        await Animal.maintain()
        await Dog.maintain()

        dog_meta = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.meta WHERE type = 'dog' AND subtype = 'canine'"
        )

        assert dog_meta[0]['type'] == "dog"
        assert dog_meta[0]['subtype'] == "canine"
        # parent_types contains subtypes of parents, not types
        # Animal has subtype='base_animal', so that's what appears
        assert "base_animal" in dog_meta[0]['parent_types']

    async def test_six_level_linear_inheritance_chain(self, graph):
        """Test very deep linear chain: L1 -> L2 -> L3 -> L4 -> L5 -> L6."""
        class L1(graph.DBObject):
            category = "deep"
            type = "l1"
            name: str

        class L2(L1):
            type = "l2"

        class L3(L2):
            type = "l3"

        class L4(L3):
            type = "l4"

        class L5(L4):
            type = "l5"

        class L6(L5):
            type = "l6"

        await L1.maintain()
        await L2.maintain()
        await L3.maintain()
        await L4.maintain()
        await L5.maintain()
        await L6.maintain()

        metas = {}
        for level in ['l1', 'l2', 'l3', 'l4', 'l5', 'l6']:
            result = await graph._conn.query(
                f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = '{level}'"
            )
            metas[level] = result[0]

        # L1: root, all 6 as descendants
        assert metas['l1']['parent_types'] == []
        assert set(metas['l1']['descendant_types']) == {'l1', 'l2', 'l3', 'l4', 'l5', 'l6'}

        # L2: 1 parent, 5 descendants (l2-l6)
        assert set(metas['l2']['parent_types']) == {'l1'}
        assert set(metas['l2']['descendant_types']) == {'l2', 'l3', 'l4', 'l5', 'l6'}

        # L3: 2 parents, 4 descendants
        assert set(metas['l3']['parent_types']) == {'l1', 'l2'}
        assert set(metas['l3']['descendant_types']) == {'l3', 'l4', 'l5', 'l6'}

        # L4: 3 parents, 3 descendants
        assert set(metas['l4']['parent_types']) == {'l1', 'l2', 'l3'}
        assert set(metas['l4']['descendant_types']) == {'l4', 'l5', 'l6'}

        # L5: 4 parents, 2 descendants
        assert set(metas['l5']['parent_types']) == {'l1', 'l2', 'l3', 'l4'}
        assert set(metas['l5']['descendant_types']) == {'l5', 'l6'}

        # L6: 5 parents, only itself
        assert set(metas['l6']['parent_types']) == {'l1', 'l2', 'l3', 'l4', 'l5'}
        assert metas['l6']['descendant_types'] == ['l6']

    async def test_wide_tree_with_ten_siblings(self, graph):
        """Test wide inheritance: one parent with 10 children."""
        class Root(graph.DBObject):
            category = "wide"
            type = "root_wide"
            name: str

        children_classes = []
        for i in range(10):
            class_name = f"Child{i}"
            child_cls = type(class_name, (Root,), {
                'type': f'child_{i}',
                '__module__': Root.__module__
            })
            children_classes.append(child_cls)

        await Root.maintain()
        for child_cls in children_classes:
            await child_cls.maintain()

        root_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'root_wide'"
        )

        expected_descendants = {'root_wide'}
        for i in range(10):
            expected_descendants.add(f'child_{i}')

        assert set(root_meta[0]['descendant_types']) == expected_descendants

        # Verify each child has only itself as descendant
        for i in range(10):
            child_meta = await graph._conn.query(
                f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'child_{i}'"
            )
            assert child_meta[0]['parent_types'] == ['root_wide']
            assert child_meta[0]['descendant_types'] == [f'child_{i}']

    async def test_diamond_inheritance_pattern(self, graph):
        """Test diamond: Base -> [Left, Right] -> Bottom."""
        class Base(graph.DBObject):
            category = "diamond"
            type = "base_d"
            name: str

        class Left(Base):
            type = "left"
            left_attr: str

        class Right(Base):
            type = "right"
            right_attr: str

        class Bottom(Left):  # Only inherits from Left in Python
            type = "bottom"
            bottom_attr: str

        await Base.maintain()
        await Left.maintain()
        await Right.maintain()
        await Bottom.maintain()

        base_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'base_d'"
        )
        left_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'left'"
        )
        right_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'right'"
        )

        # Base should have all 4
        assert set(base_meta[0]['descendant_types']) == {'base_d', 'left', 'right', 'bottom'}

        # Left should have left + bottom (bottom inherits from left)
        assert set(left_meta[0]['descendant_types']) == {'left', 'bottom'}

        # Right should have only itself (bottom doesn't inherit from right)
        assert right_meta[0]['descendant_types'] == ['right']

    async def test_binary_tree_structure(self, graph):
        """Test complete binary tree: Root -> [L,R], L->[LL,LR], R->[RL,RR]."""
        class Root(graph.DBObject):
            category = "binary"
            type = "root_bin"
            name: str

        class L(Root):
            type = "l"

        class R(Root):
            type = "r"

        class LL(L):
            type = "ll"

        class LR(L):
            type = "lr"

        class RL(R):
            type = "rl"

        class RR(R):
            type = "rr"

        await Root.maintain()
        await L.maintain()
        await R.maintain()
        await LL.maintain()
        await LR.maintain()
        await RL.maintain()
        await RR.maintain()

        root_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'root_bin'"
        )
        l_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'l'"
        )
        r_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'r'"
        )

        # Root has all 7 nodes
        assert set(root_meta[0]['descendant_types']) == {'root_bin', 'l', 'r', 'll', 'lr', 'rl', 'rr'}

        # L branch has l, ll, lr
        assert set(l_meta[0]['descendant_types']) == {'l', 'll', 'lr'}

        # R branch has r, rl, rr
        assert set(r_meta[0]['descendant_types']) == {'r', 'rl', 'rr'}

    async def test_eight_level_deep_chain(self, graph):
        """Test extreme depth: 8-level inheritance chain."""
        classes = []
        prev_cls = graph.DBObject

        for i in range(1, 9):
            cls_dict = {
                'type': f'depth_{i}',
                '__module__': graph.DBObject.__module__,
                '__annotations__': {}
            }
            if i == 1:
                cls_dict['category'] = 'extreme_depth'
                cls_dict['__annotations__']['name'] = str
            cls = type(f'Depth{i}', (prev_cls,), cls_dict)
            classes.append(cls)
            prev_cls = cls

        for cls in classes:
            await cls.maintain()

        # Check depth_1 (root) has all 8
        d1_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'depth_1'"
        )
        assert set(d1_meta[0]['descendant_types']) == {f'depth_{i}' for i in range(1, 9)}

        # Check depth_4 (middle) has 5 descendants (4-8)
        d4_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'depth_4'"
        )
        assert set(d4_meta[0]['descendant_types']) == {f'depth_{i}' for i in range(4, 9)}

        # Check depth_8 (leaf) has only itself
        d8_meta = await graph._conn.query(
            f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'depth_8'"
        )
        assert set(d8_meta[0]['parent_types']) == {f'depth_{i}' for i in range(1, 8)}
        assert d8_meta[0]['descendant_types'] == ['depth_8']

    async def test_mixed_depth_forest(self, graph):
        """Test multiple independent trees with varying depths."""
        # Tree 1: 3 levels
        class T1_Root(graph.DBObject):
            category = "forest"
            type = "t1_root"
            name: str

        class T1_L2(T1_Root):
            type = "t1_l2"

        class T1_L3(T1_L2):
            type = "t1_l3"

        # Tree 2: 5 levels
        class T2_Root(graph.DBObject):
            category = "forest"
            type = "t2_root"
            name: str

        class T2_L2(T2_Root):
            type = "t2_l2"

        class T2_L3(T2_L2):
            type = "t2_l3"

        class T2_L4(T2_L3):
            type = "t2_l4"

        class T2_L5(T2_L4):
            type = "t2_l5"

        # Tree 3: 1 level (standalone)
        class T3_Root(graph.DBObject):
            category = "forest"
            type = "t3_root"
            name: str

        # Register all
        for cls in [T1_Root, T1_L2, T1_L3, T2_Root, T2_L2, T2_L3, T2_L4, T2_L5, T3_Root]:
            await cls.maintain()

        t1_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 't1_root'"
        )
        t2_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 't2_root'"
        )
        t3_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 't3_root'"
        )

        # Tree 1: 3 descendants
        assert set(t1_meta[0]['descendant_types']) == {'t1_root', 't1_l2', 't1_l3'}

        # Tree 2: 5 descendants
        assert set(t2_meta[0]['descendant_types']) == {'t2_root', 't2_l2', 't2_l3', 't2_l4', 't2_l5'}

        # Tree 3: standalone
        assert t3_meta[0]['descendant_types'] == ['t3_root']

        # Verify no cross-contamination
        assert all(d.startswith('t1_') for d in t1_meta[0]['descendant_types'])
        assert all(d.startswith('t2_') for d in t2_meta[0]['descendant_types'])

    async def test_asymmetric_branching_tree(self, graph):
        """Test tree with uneven branching at each level."""
        #             Root
        #          /    |    \
        #         A     B     C
        #        / \    |
        #      A1  A2   B1
        #      |       / | \
        #     A1a    B1a B1b B1c

        class Root(graph.DBObject):
            category = "asymmetric"
            type = "root_asym"
            name: str

        class A(Root):
            type = "a"

        class B(Root):
            type = "b"

        class C(Root):
            type = "c"

        class A1(A):
            type = "a1"

        class A2(A):
            type = "a2"

        class B1(B):
            type = "b1"

        class A1a(A1):
            type = "a1a"

        class B1a(B1):
            type = "b1a"

        class B1b(B1):
            type = "b1b"

        class B1c(B1):
            type = "b1c"

        for cls in [Root, A, B, C, A1, A2, B1, A1a, B1a, B1b, B1c]:
            await cls.maintain()

        root_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'root_asym'"
        )
        a_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'a'"
        )
        b_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'b'"
        )
        c_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'c'"
        )
        b1_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'b1'"
        )

        # Root has all 11 types
        assert set(root_meta[0]['descendant_types']) == {
            'root_asym', 'a', 'b', 'c', 'a1', 'a2', 'b1', 'a1a', 'b1a', 'b1b', 'b1c'
        }

        # A branch: a, a1, a2, a1a
        assert set(a_meta[0]['descendant_types']) == {'a', 'a1', 'a2', 'a1a'}

        # B branch: b, b1, b1a, b1b, b1c
        assert set(b_meta[0]['descendant_types']) == {'b', 'b1', 'b1a', 'b1b', 'b1c'}

        # C is a leaf
        assert c_meta[0]['descendant_types'] == ['c']

        # B1 has itself + 3 children
        assert set(b1_meta[0]['descendant_types']) == {'b1', 'b1a', 'b1b', 'b1c'}

    async def test_wide_and_deep_combined(self, graph):
        """Test combination of wide branching and deep nesting."""
        #                  Root
        #           /   |   |   |   \
        #          L1  L2  L3  L4  L5  (5 children)
        #          |
        #         L1a
        #          |
        #        L1a1
        #          |
        #        L1a1i
        #          |
        #       L1a1i_

        class Root(graph.DBObject):
            category = "wide_deep"
            type = "wd_root"
            name: str

        # Wide branching
        class L1(Root):
            type = "wd_l1"

        class L2(Root):
            type = "wd_l2"

        class L3(Root):
            type = "wd_l3"

        class L4(Root):
            type = "wd_l4"

        class L5(Root):
            type = "wd_l5"

        # Deep chain on L1 branch
        class L1a(L1):
            type = "wd_l1a"

        class L1a1(L1a):
            type = "wd_l1a1"

        class L1a1i(L1a1):
            type = "wd_l1a1i"

        class L1a1i_(L1a1i):
            type = "wd_l1a1i_"

        for cls in [Root, L1, L2, L3, L4, L5, L1a, L1a1, L1a1i, L1a1i_]:
            await cls.maintain()

        root_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'wd_root'"
        )
        l1_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'wd_l1'"
        )
        l2_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'wd_l2'"
        )

        # Root has all 10 types
        assert set(root_meta[0]['descendant_types']) == {
            'wd_root', 'wd_l1', 'wd_l2', 'wd_l3', 'wd_l4', 'wd_l5',
            'wd_l1a', 'wd_l1a1', 'wd_l1a1i', 'wd_l1a1i_'
        }

        # L1 has deep chain
        assert set(l1_meta[0]['descendant_types']) == {
            'wd_l1', 'wd_l1a', 'wd_l1a1', 'wd_l1a1i', 'wd_l1a1i_'
        }

        # L2-L5 are leaves
        assert l2_meta[0]['descendant_types'] == ['wd_l2']

    async def test_multiple_independent_chains_same_category(self, graph):
        """Test 5 independent linear chains in the same category."""
        chains = []
        for chain_id in range(5):
            # Each chain is 4 levels deep
            class Root(graph.DBObject):
                category = "multi_chain"
                type = f"chain{chain_id}_l1"
                name: str

            class L2(Root):
                type = f"chain{chain_id}_l2"

            class L3(L2):
                type = f"chain{chain_id}_l3"

            class L4(L3):
                type = f"chain{chain_id}_l4"

            chains.append([Root, L2, L3, L4])

        # Register all
        for chain in chains:
            for cls in chain:
                await cls.maintain()

        # Verify each chain is independent
        for chain_id in range(5):
            root_meta = await graph._conn.query(
                f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'chain{chain_id}_l1'"
            )

            expected = {f"chain{chain_id}_l{level}" for level in range(1, 5)}
            assert set(root_meta[0]['descendant_types']) == expected

            # Ensure no cross-contamination from other chains
            for other_id in range(5):
                if other_id != chain_id:
                    assert not any(f"chain{other_id}_" in d for d in root_meta[0]['descendant_types'])

    async def test_recombining_branches_multiple_inheritance(self, graph):
        """
        Test complex pattern where branches recombine via multiple inheritance.

        Structure mirrors real EVM blockchain types:
                Origin                SmartContract
                  |                         |
                Chain               EVMSmartContract
                  |                         |
             ChainListing                  /
                  |   \\                  /
                  |    \\                /
                  |     \\              /
                  |      \\            /
             ERC1155    ERC20 (inherits from both ChainListing and EVMSmartContract)
        """
        class Origin(graph.DBObject):
            category = "listing"
            type = "origin"
            name: str

        class Chain(Origin):
            type = "chain"
            chain_name: str

        class ChainListing(graph.DBObject):
            category = "listing"
            type = "on_chain"
            asset: str | None = None

        class SmartContract(graph.DBObject):
            category = "smart_contract"
            type = "smart_contract"
            name: str

        class EVMSmartContract(SmartContract):
            type = "evm_smart_contract"
            address: str | None = None

        # ERC20 inherits from BOTH ChainListing and EVMSmartContract
        class ERC20(ChainListing, EVMSmartContract):
            subtype = "erc_20"
            decimals: int | None = None

        # ERC1155 only inherits from ChainListing
        class ERC1155(ChainListing):
            type = "erc_1155"
            token_id: int | None = None

        # Register all types
        await Origin.maintain()
        await Chain.maintain()
        await ChainListing.maintain()
        await SmartContract.maintain()
        await EVMSmartContract.maintain()
        await ERC20.maintain()
        await ERC1155.maintain()

        # Fetch metadata
        origin_meta = await graph._conn.query(
            f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'origin'"
        )
        chain_meta = await graph._conn.query(
            f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'chain'"
        )
        on_chain_meta = await graph._conn.query(
            f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'on_chain'"
        )
        smart_contract_meta = await graph._conn.query(
            f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'smart_contract'"
        )
        evm_sc_meta = await graph._conn.query(
            f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'evm_smart_contract'"
        )
        erc20_meta = await graph._conn.query(
            f"SELECT type, subtype, parent_types, descendant_types FROM {graph._schema}.meta WHERE category = 'listing' AND subtype = 'erc_20'"
        )
        erc1155_meta = await graph._conn.query(
            f"SELECT parent_types, descendant_types FROM {graph._schema}.meta WHERE type = 'erc_1155'"
        )

        # Verify parent chains
        assert origin_meta[0]['parent_types'] == []
        assert set(chain_meta[0]['parent_types']) == {'origin'}
        assert on_chain_meta[0]['parent_types'] == []
        assert smart_contract_meta[0]['parent_types'] == []
        assert set(evm_sc_meta[0]['parent_types']) == {'smart_contract'}

        # ERC20 inherits from both ChainListing (type='on_chain') and EVMSmartContract
        # Since ERC20 doesn't override 'type', it inherits type='on_chain' from ChainListing
        # parent_types only includes ancestors with DIFFERENT type values, so 'on_chain' is excluded
        # Therefore parent_types = {'evm_smart_contract', 'smart_contract'}
        assert erc20_meta[0]['type'] == 'on_chain'
        assert erc20_meta[0]['subtype'] == 'erc_20'
        assert set(erc20_meta[0]['parent_types']) == {'evm_smart_contract', 'smart_contract', 'on_chain'}

        # ERC1155 only has on_chain as parent
        assert set(erc1155_meta[0]['parent_types']) == {'on_chain'}

        # Verify descendant chains
        # Origin branch: origin -> chain (no ERC types since they inherit from ChainListing, not Chain)
        assert set(origin_meta[0]['descendant_types']) == {'origin', 'chain'}
        assert chain_meta[0]['descendant_types'] == ['chain']

        # ChainListing branch: on_chain -> erc_20, erc_1155
        # descendant_types contains SUBTYPES, so 'erc_20' appears (not just 'on_chain')
        assert set(on_chain_meta[0]['descendant_types']) == {'on_chain', 'erc_20', 'erc_1155'}

        # SmartContract branch: smart_contract -> evm_smart_contract -> erc_20 (via multiple inheritance)
        # ERC20 (subtype='erc_20') inherits from EVMSmartContract, so it appears in descendants
        assert set(smart_contract_meta[0]['descendant_types']) == {'smart_contract', 'evm_smart_contract', 'erc_20'}
        assert set(evm_sc_meta[0]['descendant_types']) == {'evm_smart_contract', 'erc_20'}

        # Leaf nodes have only themselves
        assert erc1155_meta[0]['descendant_types'] == ['erc_1155']

    async def test_complex_dag_with_cross_branch_inheritance(self, graph):
        """
        Test DAG (directed acyclic graph) pattern where types inherit from multiple branches.

                    A         B         C
                   / \\      / \\      /
                  A1  A2    B1  B2    C1
                   \\   \\  /   /     /
                    \\   \\/   /     /
                     \\   X   /     /
                      \\ / \\/     /
                       Y   Z     /
                        \\   \\  /
                         \\   \\/
                          Final
        """
        class A(graph.DBObject):
            category = "dag"
            type = "a_root"
            name: str

        class B(graph.DBObject):
            category = "dag"
            type = "b_root"
            name: str

        class C(graph.DBObject):
            category = "dag"
            type = "c_root"
            name: str

        class A1(A):
            type = "a1"

        class A2(A):
            type = "a2"

        class B1(B):
            type = "b1"

        class B2(B):
            type = "b2"

        class C1(C):
            type = "c1"

        # X inherits from A2 and B1
        class X(A2, B1):
            type = "x"

        # Y inherits from A1 and X
        class Y(A1, X):
            type = "y"

        # Z inherits from B2 and X
        class Z(B2, X):
            type = "z"

        # Final inherits from Y, Z, and C1
        class Final(Y, Z, C1):
            type = "final"

        # Register all
        for cls in [A, B, C, A1, A2, B1, B2, C1, X, Y, Z, Final]:
            await cls.maintain()

        # Check root descendants
        a_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'a_root'"
        )
        b_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'b_root'"
        )
        c_meta = await graph._conn.query(
            f"SELECT descendant_types FROM {graph._schema}.meta WHERE type = 'c_root'"
        )

        # A root should have: a_root, a1, a2, x, y, z, final
        # (anything that has A in its MRO)
        assert 'a_root' in a_meta[0]['descendant_types']
        assert 'a1' in a_meta[0]['descendant_types']
        assert 'a2' in a_meta[0]['descendant_types']
        assert 'x' in a_meta[0]['descendant_types']
        assert 'y' in a_meta[0]['descendant_types']
        assert 'final' in a_meta[0]['descendant_types']

        # B root should have: b_root, b1, b2, x, y, z, final
        assert 'b_root' in b_meta[0]['descendant_types']
        assert 'b1' in b_meta[0]['descendant_types']
        assert 'b2' in b_meta[0]['descendant_types']
        assert 'x' in b_meta[0]['descendant_types']
        assert 'z' in b_meta[0]['descendant_types']
        assert 'final' in b_meta[0]['descendant_types']

        # C root should have: c_root, c1, final
        assert set(c_meta[0]['descendant_types']) == {'c_root', 'c1', 'final'}


@pytest.mark.asyncio
class TestSQLFunctions:
    """Test SQL functions including fetch_object."""

    async def test_fetch_object_single_root(self, graph):
        """Test fetching a single object with no relationships."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        item = Item(source="test", name="Widget")
        await item.insert()

        # Use SQL function to fetch
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=item.id
        )

        assert len(result) == 1
        assert result[0]['id'] == item.id
        assert result[0]['attr']['name'] == "Widget"

    async def test_fetch_object_with_forward_relationship(self, graph):
        """Test fetch_object traverses forward relationships."""
        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        await Author.maintain()
        await Book.maintain()

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Test Book", author=author)
        await book.insert()

        # Fetch starting from book, should get both book and author
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=book.id
        )

        ids = {r['id'] for r in result}
        assert book.id in ids
        assert author.id in ids
        assert len(result) == 2

    async def test_fetch_object_with_backlink_relationship(self, graph):
        """Test fetch_object traverses backlink relationships."""
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

        await Author.maintain()
        await Book.maintain()

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book1 = Book(source="test", title="Book 1", author=author)
        book2 = Book(source="test", title="Book 2", author=author)
        await book1.insert()
        await book2.insert()

        # Fetch starting from author, should get author and both books
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=author.id
        )

        ids = {r['id'] for r in result}
        assert author.id in ids
        assert book1.id in ids
        assert book2.id in ids
        assert len(result) == 3

    async def test_fetch_object_multiple_roots(self, graph):
        """Test fetching multiple root objects."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        item1 = Item(source="test", name="Item1")
        item2 = Item(source="test", name="Item2")
        await item1.insert()
        await item2.insert()

        # Fetch both items
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id1)s, %(id2)s]::bigint[])",
            id1=item1.id,
            id2=item2.id
        )

        ids = {r['id'] for r in result}
        assert item1.id in ids
        assert item2.id in ids
        assert len(result) == 2

    async def test_fetch_object_deep_graph_traversal(self, graph):
        """Test fetch_object traverses multi-level relationships."""
        class Company(graph.DBObject):
            category = "test"
            type = "company"
            name: str

        class Department(graph.DBObject):
            category = "test"
            type = "department"
            name: str
            company: Link[Company]

        class Employee(graph.DBObject):
            category = "test"
            type = "employee"
            name: str
            department: Link[Department]

        await Company.maintain()
        await Department.maintain()
        await Employee.maintain()

        company = Company(source="test", name="ACME Corp")
        await company.insert()

        dept = Department(source="test", name="Engineering", company=company)
        await dept.insert()

        emp = Employee(source="test", name="Alice", department=dept)
        await emp.insert()

        # Fetch from employee, should traverse to department and company
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=emp.id
        )

        ids = {r['id'] for r in result}
        assert emp.id in ids
        assert dept.id in ids
        assert company.id in ids
        assert len(result) == 3

    async def test_fetch_object_circular_relationships(self, graph):
        """Test fetch_object handles circular relationships without infinite loop."""
        class PersonA(graph.DBObject):
            category = "test"
            type = "person_a"
            name: str

        class PersonB(graph.DBObject):
            category = "test"
            type = "person_b"
            name: str
            friend: Link[PersonA] | None = None

        await PersonA.maintain()
        await PersonB.maintain()

        alice = PersonA(source="test", name="Alice")
        bob = PersonB(source="test", name="Bob", friend=alice)

        # Use upsert for cascading save
        await bob.upsert()

        # Fetch from bob, should get both alice and bob
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=bob.id
        )

        ids = {r['id'] for r in result}
        assert alice.id in ids
        assert bob.id in ids
        assert len(result) == 2

    async def test_fetch_object_with_null_relationships(self, graph):
        """Test fetch_object handles null/optional relationships."""
        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author] | None = None

        await Book.maintain()

        book = Book(source="test", title="Anonymous Book")
        await book.insert()

        # Should only fetch the book, no author
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=book.id
        )

        assert len(result) == 1
        assert result[0]['id'] == book.id

    async def test_fetch_object_excludes_disconnected_objects(self, graph):
        """Test fetch_object only returns connected objects."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        item1 = Item(source="test", name="Connected")
        item2 = Item(source="test", name="Disconnected")
        await item1.insert()
        await item2.insert()

        # Fetch only item1
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=item1.id
        )

        ids = {r['id'] for r in result}
        assert item1.id in ids
        assert item2.id not in ids
        assert len(result) == 1

    async def test_fetch_object_empty_array(self, graph):
        """Test fetch_object with empty array returns nothing."""
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[]::bigint[])"
        )

        assert len(result) == 0

    async def test_fetch_object_nonexistent_id(self, graph):
        """Test fetch_object with non-existent ID returns empty."""
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[999999]::bigint[])"
        )

        assert len(result) == 0

    async def test_fetch_object_preserves_all_attributes(self, graph):
        """Test that fetch_object returns all object attributes correctly."""
        from decimal import Decimal

        class Product(graph.DBObject):
            category = "test"
            type = "product"
            name: str
            price: Decimal
            tags: list[str]
            metadata: dict

        product = Product(
            source="test",
            name="Widget",
            price=Decimal("19.99"),
            tags=["new", "featured"],
            metadata={"color": "blue", "size": "large"}
        )
        await product.insert()

        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=product.id
        )

        assert len(result) == 1
        attr = result[0]['attr']
        assert attr['name'] == "Widget"
        assert Decimal(str(attr['price'])) == Decimal("19.99")
        assert attr['tags'] == ["new", "featured"]
        assert attr['metadata'] == {"color": "blue", "size": "large"}

    async def test_fetch_object_with_complex_graph(self, graph):
        """Test fetch_object on a more complex object graph."""
        class Team(graph.DBObject):
            category = "test"
            type = "team"
            name: str
            members: Backlink['Member']

        class Member(graph.DBObject):
            category = "test"
            type = "member"
            name: str
            team: Link[Team, "members"]
            tasks: Backlink['Task']

        class Task(graph.DBObject):
            category = "test"
            type = "task"
            title: str
            assignee: Link[Member, "tasks"]

        await Team.maintain()
        await Member.maintain()
        await Task.maintain()

        team = Team(source="test", name="DevTeam")
        await team.insert()

        member1 = Member(source="test", name="Alice", team=team)
        member2 = Member(source="test", name="Bob", team=team)
        await member1.insert()
        await member2.insert()

        task1 = Task(source="test", title="Task 1", assignee=member1)
        task2 = Task(source="test", title="Task 2", assignee=member1)
        task3 = Task(source="test", title="Task 3", assignee=member2)
        await task1.insert()
        await task2.insert()
        await task3.insert()

        # Fetch from team root, should get everything
        result = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.fetch_object(ARRAY[%(id)s]::bigint[])",
            id=team.id
        )

        ids = {r['id'] for r in result}
        assert team.id in ids
        assert member1.id in ids
        assert member2.id in ids
        assert task1.id in ids
        assert task2.id in ids
        assert task3.id in ids
        assert len(result) == 6


@pytest.mark.asyncio
class TestObjectLoading:
    """Test the DBObject.load() method for type-based loading."""

    async def test_load_simple_type(self, graph):
        """Test loading objects of a simple type."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        await Item.maintain()

        item1 = Item(source="test", name="Item1")
        item2 = Item(source="test", name="Item2")
        await item1.insert()
        await item2.insert()

        # Clear registry and load
        graph.registry.clear()
        await Item.load()
        loaded = list(graph.registry.values())

        assert len(loaded) == 2
        assert {obj.name for obj in loaded} == {"Item1", "Item2"}

    async def test_load_with_inherited_types(self, graph):
        """Test that load() includes objects of inherited subtypes."""
        class Animal(graph.DBObject):
            category = "biology"
            type = "animal"
            name: str

        class Dog(Animal):
            type = "dog"
            breed: str

        class Cat(Animal):
            type = "cat"
            color: str

        await Animal.maintain()
        await Dog.maintain()
        await Cat.maintain()

        dog = Dog(source="test", name="Buddy", breed="Labrador")
        cat = Cat(source="test", name="Whiskers", color="Orange")
        await dog.insert()
        await cat.insert()

        # Load from Animal (parent type) should get both dog and cat
        graph.registry.clear()
        await Animal.load()
        loaded = list(graph.registry.values())

        assert len(loaded) == 2
        assert {obj.name for obj in loaded} == {"Buddy", "Whiskers"}
        assert any(isinstance(obj, Dog) for obj in loaded)
        assert any(isinstance(obj, Cat) for obj in loaded)

    async def test_load_includes_forward_refs(self, graph):
        """Test that load includes objects connected via forward relationships."""
        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        await Author.maintain()
        await Book.maintain()

        author1 = Author(source="test", name="Author1")
        author2 = Author(source="test", name="Author2")
        await author1.insert()
        await author2.insert()

        book1 = Book(source="test", title="Book1", author=author1)
        book2 = Book(source="test", title="Book2", author=author1)
        await book1.insert()
        await book2.insert()

        # Load books should also load related authors
        graph.registry.clear()
        await Book.load()
        loaded_ids = set(graph.registry.keys())

        assert book1.id in loaded_ids
        assert book2.id in loaded_ids
        assert author1.id in loaded_ids
        # author2 should NOT be loaded (no book references it)
        assert author2.id not in loaded_ids

    async def test_load_includes_backlinks(self, graph):
        """Test that load includes objects connected via backlink relationships."""
        class Author(graph.DBObject):
            category = "test"
            type = "author_load_test"
            name: str
            books: Backlink['Book']

        class Book(graph.DBObject):
            category = "test"
            type = "book_load_test"
            title: str
            author: Link[Author, "books"]

        await Author.maintain()
        await Book.maintain()

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book1 = Book(source="test", title="Book1", author=author)
        book2 = Book(source="test", title="Book2", author=author)
        await book1.insert()
        await book2.insert()

        # Load authors should also load related books
        graph.registry.clear()
        await Author.load()
        loaded_ids = set(graph.registry.keys())

        assert author.id in loaded_ids
        assert book1.id in loaded_ids
        assert book2.id in loaded_ids
        assert len(graph.registry) == 3

    async def test_load_multilevel_inheritance(self, graph):
        """Test loading with multiple levels of inheritance."""
        class Vehicle(graph.DBObject):
            category = "transport"
            type = "vehicle"
            name: str

        class Car(Vehicle):
            type = "car"
            doors: int

        class SportsCar(Car):
            type = "sports_car"
            top_speed: int

        await Vehicle.maintain()
        await Car.maintain()
        await SportsCar.maintain()

        sports_car = SportsCar(source="test", name="Ferrari", doors=2, top_speed=200)
        regular_car = Car(source="test", name="Sedan", doors=4)
        await sports_car.insert()
        await regular_car.insert()

        # Load from Vehicle should get all descendants
        graph.registry.clear()
        await Vehicle.load()

        assert len(graph.registry) == 2
        assert {obj.name for obj in graph.registry.values()} == {"Ferrari", "Sedan"}

        # Load from Car should get Car and SportsCar
        graph.registry.clear()
        await Car.load()

        assert len(graph.registry) == 2

    async def test_load_returns_already_loaded_objects(self, graph):
        """Test that load() reuses already-loaded objects from registry."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        await Item.maintain()

        item = Item(source="test", name="Item1")
        await item.insert()

        # First load
        await Item.load()
        obj1 = graph.registry[item.id]

        # Second load should return same object instance
        await Item.load()
        obj2 = graph.registry[item.id]

        assert obj1 is obj2  # Same instance

    async def test_load_empty_result(self, graph):
        """Test loading when no objects exist."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        await Item.maintain()

        await Item.load()

        assert len(graph.registry) == 0

    async def test_load_complex_graph(self, graph):
        """Test loading a complex graph with multiple relationship levels."""
        class Company(graph.DBObject):
            category = "test"
            type = "company"
            name: str

        class Department(graph.DBObject):
            category = "test"
            type = "department"
            name: str
            company: Link[Company]

        class Employee(graph.DBObject):
            category = "test"
            type = "employee"
            name: str
            department: Link[Department]

        await Company.maintain()
        await Department.maintain()
        await Employee.maintain()

        company = Company(source="test", name="ACME Corp")
        await company.insert()

        dept = Department(source="test", name="Engineering", company=company)
        await dept.insert()

        emp1 = Employee(source="test", name="Alice", department=dept)
        emp2 = Employee(source="test", name="Bob", department=dept)
        await emp1.insert()
        await emp2.insert()

        # Load employees should also load related dept and company
        graph.registry.clear()
        await Employee.load()
        loaded_ids = set(graph.registry.keys())

        assert emp1.id in loaded_ids
        assert emp2.id in loaded_ids
        assert dept.id in loaded_ids
        assert company.id in loaded_ids
        assert len(graph.registry) == 4

    async def test_load_without_type_raises_error(self, graph):
        """Test that calling load() on a class without type raises error."""
        class NoType(graph.DBObject):
            category = "test"
            name: str

        # Should raise ValueError
        with pytest.raises(ValueError, match="without a type attribute"):
            await NoType.load()

    async def test_load_preserves_all_attributes(self, graph):
        """Test that loaded objects have all their attributes correctly restored."""
        from decimal import Decimal

        class Product(graph.DBObject):
            category = "test"
            type = "product"
            name: str
            price: Decimal
            tags: list[str]

        await Product.maintain()

        product = Product(
            source="test",
            name="Widget",
            price=Decimal("19.99"),
            tags=["new", "featured"]
        )
        await product.insert()

        graph.registry.clear()
        await Product.load()

        assert len(graph.registry) == 1
        obj = list(graph.registry.values())[0]
        assert obj.name == "Widget"
        assert obj.price == Decimal("19.99")
        assert obj.tags == ["new", "featured"]
        assert obj.source == "test"


@pytest.mark.asyncio
class TestObjectQuerying:
    """Test the DBObject.all(), .get(), and .filter() methods for querying in-memory objects."""

    async def test_all_returns_all_objects_of_type(self, graph):
        """Test that all() returns all objects of a type from the registry."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        await Item.maintain()

        item1 = Item(source="test", name="Item1")
        item2 = Item(source="test", name="Item2")
        await item1.insert()
        await item2.insert()

        all_items = Item.all()

        assert len(all_items) == 2
        assert {obj.name for obj in all_items} == {"Item1", "Item2"}

    async def test_all_empty_when_no_objects(self, graph):
        """Test that all() returns empty list when no objects exist."""
        class Item(graph.DBObject):
            category = "test"
            type = "item_all_test"
            name: str

        await Item.maintain()

        assert Item.all() == []

    async def test_get_by_unique_constraint(self, graph):
        """Test get() with a unique constraint."""
        class User(graph.DBObject):
            category = "test"
            type = "user"
            email: str
            name: str
            type_unique_attr = ["email"]

        await User.maintain()

        user = User(source="test", email="user@example.com", name="John")
        await user.insert()

        # Get by unique email
        found = User.get(email="user@example.com")

        assert found is user
        assert found.name == "John"

    async def test_get_by_computed_property(self, graph):
        """Test get() with a computed property index."""
        class Asset(graph.DBObject):
            category = "test"
            type = "asset"
            symbol: str
            name: str
            computed_unique_attr = ["full_name"]

            @property
            def full_name(self):
                return f"{self.symbol}:{self.name}"

        await Asset.maintain()

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        # Get by computed property
        found = Asset.get(full_name="BTC:Bitcoin")

        assert found is asset

    async def test_get_raises_keyerror_when_not_found(self, graph):
        """Test that get() raises KeyError when object not found."""
        class User(graph.DBObject):
            category = "test"
            type = "user_get_test"
            email: str
            type_unique_attr = ["email"]

        await User.maintain()

        with pytest.raises(KeyError, match="No User found"):
            User.get(email="nonexistent@example.com")

    async def test_get_raises_valueerror_no_unique_constraint(self, graph):
        """Test that get() raises ValueError when no unique constraint matches."""
        class Item(graph.DBObject):
            category = "test"
            type = "item_get_test"
            name: str
            description: str
            type_unique_attr = ["name"]

        await Item.maintain()

        item = Item(source="test", name="Item", description="Desc")
        await item.insert()

        # Try to get by non-unique attribute
        with pytest.raises(ValueError, match="No unique constraint"):
            Item.get(description="Desc")

    async def test_get_raises_valueerror_no_kwargs(self, graph):
        """Test that get() raises ValueError when called without arguments."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str

        with pytest.raises(ValueError, match="At least one keyword argument required"):
            Item.get()

    async def test_get_with_multi_column_constraint(self, graph):
        """Test get() with a multi-column unique constraint."""
        class Location(graph.DBObject):
            category = "test"
            type = "location"
            country: str
            city: str
            name: str
            type_unique_attr = [("country", "city")]

        await Location.maintain()

        loc = Location(source="test", country="USA", city="NYC", name="Times Square")
        await loc.insert()

        # Get by composite key
        found = Location.get(country="USA", city="NYC")

        assert found is loc
        assert found.name == "Times Square"

    async def test_filter_by_single_attribute(self, graph):
        """Test filter() with a single attribute."""
        class Task(graph.DBObject):
            category = "test"
            type = "task"
            title: str
            status: str

        await Task.maintain()

        task1 = Task(source="test", title="Task1", status="pending")
        task2 = Task(source="test", title="Task2", status="completed")
        task3 = Task(source="test", title="Task3", status="pending")
        await task1.insert()
        await task2.insert()
        await task3.insert()

        # Filter by status
        pending = Task.filter(status="pending")

        assert len(pending) == 2
        assert {t.title for t in pending} == {"Task1", "Task3"}

    async def test_filter_by_multiple_attributes(self, graph):
        """Test filter() with multiple attributes."""
        class Product(graph.DBObject):
            category = "test"
            type = "product_filter_test"
            name: str
            category_name: str
            price: int

        await Product.maintain()

        p1 = Product(source="test", name="P1", category_name="electronics", price=100)
        p2 = Product(source="test", name="P2", category_name="electronics", price=200)
        p3 = Product(source="test", name="P3", category_name="books", price=100)
        await p1.insert()
        await p2.insert()
        await p3.insert()

        # Filter by category and price
        results = Product.filter(category_name="electronics", price=100)

        assert len(results) == 1
        assert results[0].name == "P1"

    async def test_filter_returns_empty_list_no_matches(self, graph):
        """Test that filter() returns empty list when no matches."""
        class Item(graph.DBObject):
            category = "test"
            type = "item_filter_test"
            name: str
            status: str

        await Item.maintain()

        item = Item(source="test", name="Item", status="active")
        await item.insert()

        results = Item.filter(status="deleted")

        assert results == []

    async def test_filter_no_kwargs_returns_all(self, graph):
        """Test that filter() with no kwargs returns all objects."""
        class Item(graph.DBObject):
            category = "test"
            type = "item_filter_all_test"
            name: str

        await Item.maintain()

        item1 = Item(source="test", name="Item1")
        item2 = Item(source="test", name="Item2")
        await item1.insert()
        await item2.insert()

        results = Item.filter()

        assert len(results) == 2
        assert {obj.id for obj in results} == {item1.id, item2.id}

    async def test_all_get_filter_work_together(self, graph):
        """Test that all(), get(), and filter() work together correctly."""
        class User(graph.DBObject):
            category = "test"
            type = "user_combined_test"
            email: str
            name: str
            role: str
            type_unique_attr = ["email"]

        await User.maintain()

        admin = User(source="test", email="admin@test.com", name="Admin", role="admin")
        user1 = User(source="test", email="user1@test.com", name="User1", role="user")
        user2 = User(source="test", email="user2@test.com", name="User2", role="user")
        await admin.insert()
        await user1.insert()
        await user2.insert()

        # Test all()
        all_users = User.all()
        assert len(all_users) == 3

        # Test get()
        found_admin = User.get(email="admin@test.com")
        assert found_admin is admin

        # Test filter()
        regular_users = User.filter(role="user")
        assert len(regular_users) == 2
        assert {u.name for u in regular_users} == {"User1", "User2"}
