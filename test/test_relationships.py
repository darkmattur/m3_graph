"""
Relationship management tests (Link and Backlink).

Tests cover:
- Forward link (Link) creation, assignment, and traversal
- Backward link (Backlink) behavior and synchronization
- Nullable and required relationships
- Relationship state management (in-memory vs database)
- Bidirectional relationship consistency
- Multiple relationships and complex graphs
"""
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestForwardLinks:
    """Test forward link (Link) functionality."""

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

    async def test_forward_link_required_error(self, graph):
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

    async def test_forward_link_unsaved_object_error(self, graph):
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

    async def test_forward_link_setter_rejects_unsaved(self, graph):
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

    async def test_forward_link_traversal_preserves_identity(self, graph):
        """Test that traversing forward links returns same Python instance."""
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

        # Both traverse to same instance
        assert book1.author is book2.author
        assert book1.author is author

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

        cat = Category(source="test", name="Electronics")
        await cat.insert()

        item = Item(source="test", name="Laptop", category_obj=cat)
        await item.insert()

        # Set to None
        item.category_obj = None
        await item.update()

        assert item.category_obj is None
        assert item.category_obj_id is None


@pytest.mark.asyncio
class TestBacklinks:
    """Test backward link (Backlink) functionality."""

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

    async def test_backlink_requires_database_triggers(self, graph):
        """Test that backlinks are populated by database triggers, not in-memory."""
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

        # Before inserting books, backlinks are empty
        assert author.books == []

        # Create books
        book1 = Book(source="test", title="Book 1", author=author)
        book2 = Book(source="test", title="Book 2", author=author)
        await book1.insert()
        await book2.insert()

        # Backlinks still empty in-memory (not updated automatically)
        assert author.books == []

        # Must reload to see backlinks
        await graph.load()
        author_reloaded = graph.registry[author.id]
        assert len(author_reloaded.books) == 2

    async def test_backlink_returns_registry_instances(self, graph):
        """Test that backlinks return instances from registry."""
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

        # Backlink should return same instance as in registry
        backlink_books = author_reloaded.books
        assert len(backlink_books) == 1
        assert backlink_books[0] is book_reloaded

    async def test_backlink_updates_after_relationship_change(self, graph):
        """Test that backlinks reflect relationship changes after reload."""
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

        # Reload to populate backlinks
        await graph.load()
        author1_loaded = graph.registry[author1.id]
        author2_loaded = graph.registry[author2.id]
        assert len(author1_loaded.books) == 1
        assert len(author2_loaded.books) == 0

        # Change relationship
        book_loaded = graph.registry[book.id]
        book_loaded.author = author2_loaded
        await book_loaded.update()

        # Reload again
        graph.registry.clear()
        await graph.load()

        author1_reloaded = graph.registry[author1.id]
        author2_reloaded = graph.registry[author2.id]

        # Backlinks should be updated
        assert len(author1_reloaded.books) == 0
        assert len(author2_reloaded.books) == 1

    async def test_backlink_cleanup_on_deletion(self, graph):
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


@pytest.mark.asyncio
class TestRelationshipState:
    """Test relationship state management (in-memory vs database)."""

    async def test_relationship_change_visible_immediately(self, graph):
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

    async def test_relationship_change_not_persisted_without_update(self, graph):
        """Test that in-memory relationship changes don't persist without update()."""
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

        # Change in-memory but don't update
        book.author = author2

        # Reload from database
        graph.registry.clear()
        await graph.load()

        book_reloaded = graph.registry[book.id]
        assert book_reloaded.author_id == author1.id  # Original value

    async def test_relationship_update_persistence(self, graph):
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

    async def test_reload_discards_relationship_changes(self, graph):
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


@pytest.mark.asyncio
class TestBidirectionalRelationships:
    """Test bidirectional relationship consistency."""

    async def test_forward_and_backward_consistency(self, graph):
        """Test that forward and backward links are consistent after reload."""
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

        # Forward and backward should be consistent
        assert book_reloaded.author is author_reloaded
        assert author_reloaded.books[0] is book_reloaded

    async def test_multiple_forward_links_to_same_object(self, graph):
        """Test multiple objects linking to same target."""
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
        book3 = Book(source="test", title="Book 3", author=author)
        await book1.insert()
        await book2.insert()
        await book3.insert()

        # Reload
        await graph.load()

        author_reloaded = graph.registry[author.id]
        books = author_reloaded.books

        # Should have all three books
        assert len(books) == 3
        assert {b.title for b in books} == {"Book 1", "Book 2", "Book 3"}

    async def test_relationship_change_updates_both_sides(self, graph):
        """Test that changing relationship updates both forward and backward links."""
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

        # Reload
        await graph.load()

        book_loaded = graph.registry[book.id]
        author1_loaded = graph.registry[author1.id]
        author2_loaded = graph.registry[author2.id]

        # Initial state
        assert len(author1_loaded.books) == 1
        assert len(author2_loaded.books) == 0

        # Change relationship
        book_loaded.author = author2_loaded
        await book_loaded.update()

        # Reload to see updated backlinks
        graph.registry.clear()
        await graph.load()

        author1_final = graph.registry[author1.id]
        author2_final = graph.registry[author2.id]
        book_final = graph.registry[book.id]

        # Both sides should be updated
        assert book_final.author_id == author2.id
        assert len(author1_final.books) == 0
        assert len(author2_final.books) == 1
        assert author2_final.books[0].id == book.id


@pytest.mark.asyncio
class TestComplexRelationships:
    """Test complex relationship scenarios."""

    async def test_multiple_forward_links_same_class(self, graph):
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

    async def test_relationship_through_multiple_sessions(self, graph):
        """Test relationship consistency across multiple load cycles."""
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

        # Session 1: Create
        author = Author(source="test", name="Jane Doe")
        await author.insert()

        book = Book(source="test", title="Book", author=author)
        await book.insert()

        # Session 2: Load and verify forward link
        graph.registry.clear()
        await graph.load()
        book_s2 = graph.registry[book.id]
        assert book_s2.author_id == author.id

        # Session 3: Load and verify backward link
        graph.registry.clear()
        await graph.load()
        author_s3 = graph.registry[author.id]
        assert len(author_s3.books) == 1

        # Session 4: Modify relationship
        graph.registry.clear()
        await graph.load()
        author2 = Author(source="test", name="Another Author")
        await author2.insert()
        book_s4 = graph.registry[book.id]
        book_s4.author = author2
        await book_s4.update()

        # Session 5: Verify modification
        graph.registry.clear()
        await graph.load()
        book_s5 = graph.registry[book.id]
        author1_s5 = graph.registry[author.id]
        author2_s5 = graph.registry[author2.id]

        assert book_s5.author_id == author2.id
        assert len(author1_s5.books) == 0
        assert len(author2_s5.books) == 1
