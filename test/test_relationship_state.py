"""
Tests for relationship state management in-memory.

These tests validate how relationships behave in-memory:
- Forward link (Link) in-memory behavior
- Backward link (Backlink) in-memory behavior
- Relationship changes are immediate in code
- Relationship changes persist only via update()
- Bidirectional relationship consistency
- Cascade behavior

Tests cover:
- In-memory relationship assignment
- Relationship traversal without database
- Relationship change visibility
- Multiple relationships to same object
- Relationship persistence patterns
- Backlink array management
"""
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestForwardLinkInMemory:
    """Test forward link (Link) in-memory behavior."""

    async def test_forward_link_assignment_immediate(self, graph):
        """Test that forward link assignment is immediately visible."""

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

        # Assignment immediately visible
        assert book.author_id == author.id
        assert book.author is author
        assert book.author.name == "Jane Doe"

    async def test_forward_link_change_immediate(self, graph):
        """Test that changing forward link is immediately visible."""

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

        # Change in-memory
        book.author = author2

        # Immediately visible
        assert book.author_id == author2.id
        assert book.author is author2
        assert book.author.name == "Author 2"

    async def test_forward_link_change_not_persisted_without_update(self, graph):
        """Test that forward link changes don't persist without update()."""

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

    async def test_forward_link_traversal_returns_same_instance(self, graph):
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

    async def test_nullable_forward_link_in_memory(self, graph):
        """Test nullable forward link in-memory behavior."""

        class Category(graph.DBObject):
            category = "test"
            type = "category"
            name: str

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_obj: Link[Category] | None = None

        # Create without category
        item = Item(source="test", name="Item")
        assert item.category_obj is None
        assert item.category_obj_id is None

        # Set category in-memory
        cat = Category(source="test", name="Electronics")
        await cat.insert()

        item.category_obj = cat
        assert item.category_obj is cat
        assert item.category_obj_id == cat.id

        # Set back to None in-memory
        item.category_obj = None
        assert item.category_obj is None
        assert item.category_obj_id is None

    async def test_forward_link_by_id_assignment(self, graph):
        """Test assigning forward link by ID."""

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

        # Assign by ID
        book = Book(source="test", title="Book", author_id=author.id)

        # Should work
        assert book.author_id == author.id
        assert book.author is author

    async def test_forward_link_multiple_changes_before_persist(self, graph):
        """Test multiple forward link changes before persisting."""

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
        author3 = Author(source="test", name="Author 3")
        await author1.insert()
        await author2.insert()
        await author3.insert()

        book = Book(source="test", title="Book", author=author1)
        await book.insert()

        # Multiple changes in-memory
        book.author = author2
        book.author = author3
        book.author = author1

        # Only last change matters
        assert book.author is author1

        # Persist
        await book.update()

        # Verify
        graph.registry.clear()
        await graph.load()

        book_reloaded = graph.registry[book.id]
        assert book_reloaded.author_id == author1.id


@pytest.mark.asyncio
class TestBacklinkInMemory:
    """Test backward link (Backlink) in-memory behavior."""

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

    async def test_backlink_array_from_ids(self, graph):
        """Test that backlink accesses objects from _ids field."""

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

        # Reload
        await graph.load()

        author_reloaded = graph.registry[author.id]

        # Check internal _ids array
        assert hasattr(author_reloaded, 'books_ids')
        assert len(author_reloaded.books_ids) == 2
        assert book1.id in author_reloaded.books_ids
        assert book2.id in author_reloaded.books_ids

        # Backlink property accesses via _ids
        books = author_reloaded.books
        assert len(books) == 2
        assert all(isinstance(b, Book) for b in books)

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

    async def test_backlink_empty_when_no_relationships(self, graph):
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

        # No books created
        assert author.books == []

        # Even after reload
        await graph.load()
        author_reloaded = graph.registry[author.id]
        assert author_reloaded.books == []

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


@pytest.mark.asyncio
class TestBidirectionalRelationships:
    """Test bidirectional relationship consistency."""

    async def test_forward_and_backward_consistency_after_reload(self, graph):
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

    async def test_deletion_updates_backlinks(self, graph):
        """Test that deleting object updates backlinks."""

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

        # Reload
        await graph.load()
        author_loaded = graph.registry[author.id]
        assert len(author_loaded.books) == 2

        # Delete one book
        book1_loaded = graph.registry[book1.id]
        await book1_loaded.delete()

        # Reload
        graph.registry.clear()
        await graph.load()

        author_reloaded = graph.registry[author.id]

        # Should only have one book now
        assert len(author_reloaded.books) == 1
        assert author_reloaded.books[0].id == book2.id


@pytest.mark.asyncio
class TestRelationshipPersistence:
    """Test relationship persistence patterns."""

    async def test_create_with_relationship_then_persist(self, graph):
        """Test creating object with relationship, then persisting."""

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

        # Create with relationship
        book = Book(source="test", title="Book", author=author)

        # Relationship visible in-memory
        assert book.author is author

        # Persist
        await book.insert()

        # Verify persisted
        graph.registry.clear()
        await graph.load()

        book_reloaded = graph.registry[book.id]
        assert book_reloaded.author_id == author.id

    async def test_modify_relationship_then_persist(self, graph):
        """Test modifying relationship in-memory, then persisting."""

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

        # Modify in-memory
        book.author = author2

        # Persist
        await book.update()

        # Verify
        graph.registry.clear()
        await graph.load()

        book_reloaded = graph.registry[book.id]
        assert book_reloaded.author_id == author2.id

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

    async def test_complex_relationship_graph(self, graph):
        """Test complex graph of relationships."""

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

        # Create multiple authors and books
        author1 = Author(source="test", name="Author 1")
        author2 = Author(source="test", name="Author 2")
        author3 = Author(source="test", name="Author 3")
        await author1.insert()
        await author2.insert()
        await author3.insert()

        book1 = Book(source="test", title="Book 1", author=author1)
        book2 = Book(source="test", title="Book 2", author=author1)
        book3 = Book(source="test", title="Book 3", author=author2)
        book4 = Book(source="test", title="Book 4", author=author3)
        await book1.insert()
        await book2.insert()
        await book3.insert()
        await book4.insert()

        # Reload
        await graph.load()

        # Verify the graph
        author1_loaded = graph.registry[author1.id]
        author2_loaded = graph.registry[author2.id]
        author3_loaded = graph.registry[author3.id]

        assert len(author1_loaded.books) == 2
        assert len(author2_loaded.books) == 1
        assert len(author3_loaded.books) == 1

        # All books should point to correct authors
        for book_id in [book1.id, book2.id, book3.id, book4.id]:
            book_loaded = graph.registry[book_id]
            assert book_loaded.author is not None
