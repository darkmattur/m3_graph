"""
Tests for database trigger functionality.

Tests cover:
- Backlink synchronization via triggers
- Relationship metadata management
- History tracking on insert/update/delete
- Cascade cleanup on deletion
- Trigger behavior with multiple relationships
"""
import pytest
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
        graph.registry_type.clear()
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
        graph.registry_type.clear()
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
        graph.registry_type.clear()
        await graph.load()

        author_reloaded = graph.registry[author.id]

        # Book should be removed from backlinks
        assert book_id not in author_reloaded.books_ids

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

    async def test_multiple_backlinks_same_object(self, graph):
        """Test object with multiple different backlink relationships."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str
            books: Backlink['Book']
            articles: Backlink['Article']

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author, "books"]

        class Article(graph.DBObject):
            category = "test"
            type = "article"
            title: str
            author: Link[Author, "articles"]


        # Register relationship metadata
        await graph._conn.execute_many(
            f"""
            INSERT INTO {graph._name}.meta_relationship (category, type, subtype, forward, back)
            VALUES ('test', 'book', 'book', %(forward)s, %(back)s)
            ON CONFLICT (category, type, subtype) DO UPDATE SET forward = EXCLUDED.forward, back = EXCLUDED.back
            """,
            [
                {"forward": {"author_id": "books_ids"}, "back": []},
            ]
        )
        await graph._conn.execute_many(
            f"""
            INSERT INTO {graph._name}.meta_relationship (category, type, subtype, forward, back)
            VALUES ('test', 'article', 'article', %(forward)s, %(back)s)
            ON CONFLICT (category, type, subtype) DO UPDATE SET forward = EXCLUDED.forward, back = EXCLUDED.back
            """,
            [
                {"forward": {"author_id": "articles_ids"}, "back": []},
            ]
        )

        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Book", author=author)
        article = Article(source="test", title="Article", author=author)

        await book.insert()
        await article.insert()

        # Reload
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        author_reloaded = graph.registry[author.id]

        # Both backlinks should be populated
        assert book.id in author_reloaded.books_ids
        assert article.id in author_reloaded.articles_ids

    async def test_nullable_relationship_backlink_handling(self, graph):
        """Test backlink handling with nullable relationships."""

        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str
            items: Backlink['Item']

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category, "items"] | None = None


        # Register relationship metadata
        await graph._conn.execute(
            f"""
            INSERT INTO {graph._name}.meta_relationship (category, type, subtype, forward, back)
            VALUES ('test', 'item', 'item', %(forward)s, %(back)s)
            ON CONFLICT (category, type, subtype) DO UPDATE SET forward = EXCLUDED.forward, back = EXCLUDED.back
            """,
            forward={"category_obj_id": "items_ids"},
            back=[]
        )

        cat = Category(source="test", name="Electronics")
        await cat.insert()

        # Item with category
        item1 = Item(source="test", name="Laptop", category_obj=cat)
        await item1.insert()

        # Item without category
        item2 = Item(source="test", name="Other")
        await item2.insert()

        # Reload
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        cat_reloaded = graph.registry[cat.id]
        item2_reloaded = graph.registry[item2.id]

        # Only item1 should be in backlinks
        assert item1.id in cat_reloaded.items_ids
        assert item2.id not in cat_reloaded.items_ids
        assert item2_reloaded.category_obj_id is None

    async def test_relationship_change_backlink_migration(self, graph):
        """Test that changing relationships properly migrates backlinks."""

        class Tag(graph.DBObject):
            category = "test"
            type = "tag"
            name: str
            items: Backlink['Item']

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            tag: Link[Tag, "items"]


        # Register relationship metadata
        await graph._conn.execute(
            f"""
            INSERT INTO {graph._name}.meta_relationship (category, type, subtype, forward, back)
            VALUES ('test', 'item', 'item', %(forward)s, %(back)s)
            ON CONFLICT (category, type, subtype) DO UPDATE SET forward = EXCLUDED.forward, back = EXCLUDED.back
            """,
            forward={"tag_id": "items_ids"},
            back=[]
        )

        tag1 = Tag(source="test", name="Tag1")
        tag2 = Tag(source="test", name="Tag2")
        await tag1.insert()
        await tag2.insert()

        item = Item(source="test", name="Item", tag=tag1)
        await item.insert()

        # Change tag
        item.tag = tag2
        await item.update()

        # Reload
        graph.registry.clear()
        graph.registry_type.clear()
        await graph.load()

        tag1_reloaded = graph.registry[tag1.id]
        tag2_reloaded = graph.registry[tag2.id]

        # Backlinks should have migrated
        assert item.id not in tag1_reloaded.items_ids
        assert item.id in tag2_reloaded.items_ids

    async def test_history_preserves_all_attributes(self, graph):
        """Test that history preserves all object attributes."""
        from decimal import Decimal

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
