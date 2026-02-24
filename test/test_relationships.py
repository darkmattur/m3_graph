"""
Tests for relationship management (Link and Backlink).

Tests cover:
- Forward link creation and access
- Backlink creation and access
- Assignment via object and ID
- Nullable and required relationships
- Backlink synchronization via database triggers
- Relationship property behavior
"""
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestRelationships:
    """Test Link and Backlink relationship functionality."""

    async def test_forward_link_basic(self, graph):
        """Test basic forward link creation and access."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]

        # Initialize indexes

        # Create and save author
        author = Author(source="test", name="Jane Doe")
        await author.insert()

        # Create book with author
        book = Book(source="test", title="Test Book", author=author)
        assert book.author_id == author.id
        assert book.author.id == author.id
        assert book.author.name == "Jane Doe"

    async def test_forward_link_assignment_via_id(self, graph):
        """Test forward link assignment using ID."""

        class Author(graph.DBObject):
            category = "test"
            type = "author"
            name: str

        class Book(graph.DBObject):
            category = "test"
            type = "book"
            title: str
            author: Link[Author]


        author = Author(source="test", name="John Smith")
        await author.insert()

        # Assign via ID
        book = Book(source="test", title="Test Book", author_id=author.id)
        assert book.author_id == author.id
        assert book.author.name == "John Smith"

    async def test_forward_link_nullable(self, graph):
        """Test nullable forward links."""

        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category] | None = None


        # Create item without category
        item1 = Item(source="test", name="No Category")
        assert item1.category_obj is None
        assert item1.category_obj_id is None

        # Create item with category
        cat = Category(source="test", name="Electronics")
        await cat.insert()

        item2 = Item(source="test", name="Laptop", category_obj=cat)
        assert item2.category_obj_id == cat.id
        assert item2.category_obj.name == "Electronics"

    async def test_forward_link_required(self, graph):
        """Test that required links raise errors when None."""

        class Parent(graph.DBObject):
            category = "test"
            type = "parent"
            name: str

        class Child(graph.DBObject):
            category = "test"
            type = "child"
            name: str
            parent: Link[Parent]  # Required


        # Should raise error when trying to create without parent
        with pytest.raises(ValueError, match="parent.*required"):
            Child(source="test", name="Orphan", parent=None)

    async def test_forward_link_unsaved_object(self, graph):
        """Test that assigning unsaved object raises error."""

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
        author = Author(source="test", name="Jane Doe")

        # Should raise error
        with pytest.raises(ValueError, match="unsaved"):
            Book(source="test", title="Test Book", author=author)

    async def test_forward_link_setter(self, graph):
        """Test forward link property setter."""

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
        assert book.author.name == "Author 1"

        # Change author
        book.author = author2
        assert book.author_id == author2.id
        assert book.author.name == "Author 2"

    async def test_backlink_basic(self, graph):
        """Test basic backlink creation and access."""

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

        # Backlinks should be populated by database trigger
        # We need to reload from database to see them
        await graph.load()

        author_reloaded = graph.registry[author.id]
        books = author_reloaded.books

        assert len(books) == 2
        assert {b.title for b in books} == {"Book 1", "Book 2"}

    async def test_backlink_empty(self, graph):
        """Test that backlinks return empty list when no relationships exist."""

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

        # No books yet
        assert author.books == []

    async def test_relationship_with_explicit_backlink_name(self, graph):
        """Test Link with explicit backlink name parameter."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            positions: Backlink['Position']

        class Position(graph.DBObject):
            category = "financial"
            type = "position"
            quantity: float
            asset: Link[Asset, "positions"]

        await graph.db_maintain()

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        pos1 = Position(source="test", quantity=1.5, asset=asset)
        pos2 = Position(source="test", quantity=2.5, asset=asset)
        await pos1.insert()
        await pos2.insert()

        # Reload to get backlinks
        await graph.load()

        asset_reloaded = graph.registry[asset.id]
        positions = asset_reloaded.positions

        assert len(positions) == 2
        assert sum(p.quantity for p in positions) == 4.0

    async def test_relationship_update(self, graph):
        """Test updating relationships and backlink synchronization."""

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

        book = Book(source="test", title="Test Book", author=author1)
        await book.insert()

        # Change author
        book.author = author2
        await book.update()

        # Reload and verify backlinks updated
        await graph.load()

        author1_reloaded = graph.registry[author1.id]
        author2_reloaded = graph.registry[author2.id]

        assert len(author1_reloaded.books) == 0
        assert len(author2_reloaded.books) == 1
        assert author2_reloaded.books[0].title == "Test Book"

    async def test_multiple_forward_links(self, graph):
        """Test object with multiple forward links."""

        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str

        class Tag(graph.DBObject):
            category = "test"
            type = "tag"
            name: str

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category]
            tag: Link[Tag] | None = None

        await graph.db_maintain()

        cat = Category(source="test", name="Electronics")
        tag = Tag(source="test", name="Featured")
        await cat.insert()
        await tag.insert()

        item = Item(source="test", name="Laptop", category_obj=cat, tag=tag)
        assert item.category_obj.name == "Electronics"
        assert item.tag.name == "Featured"

        await item.insert()
        await graph.load()

        item_reloaded = graph.registry[item.id]
        assert item_reloaded.category_obj_id == cat.id
        assert item_reloaded.tag_id == tag.id
        assert item_reloaded.category_obj.name == "Electronics"
        assert item_reloaded.tag.name == "Featured"

    async def test_relationship_deletion_backlink_cleanup(self, graph):
        """Test that backlinks are cleaned up when related object is deleted."""

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

        book = Book(source="test", title="Test Book", author=author)
        await book.insert()

        # Delete the book
        await book.delete()

        # Reload and verify backlink is gone
        await graph.load()

        author_reloaded = graph.registry[author.id]
        assert len(author_reloaded.books) == 0

    async def test_nullable_link_set_to_none(self, graph):
        """Test setting nullable link to None."""

        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category] | None = None

        await graph.db_maintain()

        cat = Category(source="test", name="Electronics")
        await cat.insert()

        item = Item(source="test", name="Laptop", category_obj=cat)
        await item.insert()

        # Set to None
        item.category_obj = None
        await item.update()

        assert item.category_obj is None
        assert item.category_obj_id is None
