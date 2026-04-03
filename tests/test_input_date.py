"""
Tests for the input_date feature (bitemporal history support).

Tests cover:
- Insert with input_date sets custom validity timestamp
- Update with input_date closes/opens at custom timestamp
- Delete with input_date closes at custom timestamp
- recorded_at always reflects wall-clock time (now()), never input_date
- input_date resets after transaction exits
- Validation: input_date before current validity start is rejected
- Bulk operations work with input_date
- Timezone-naive datetime is rejected
"""
import datetime as dt
import pytest
from m3_graph.link import Link, Backlink


UTC = dt.timezone.utc


@pytest.mark.asyncio
class TestInputDateInsert:
    """Test insert operations with input_date."""

    async def test_insert_with_input_date(self, graph):
        """History entry should use the provided input_date, not now()."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        past = dt.datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)

        async with graph.transaction(input_date=past):
            item = Item(source="backfill", code="ABC")
            await item.insert()

        history = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
            id=item.id
        )

        assert len(history) == 1
        assert history[0]['validity'].lower == past
        assert history[0]['validity'].upper == dt.datetime.max  # infinity

    async def test_insert_recorded_at_is_wall_clock(self, graph):
        """recorded_at should always be now(), regardless of input_date."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        past = dt.datetime(2020, 6, 1, tzinfo=UTC)
        before = dt.datetime.now(UTC)

        async with graph.transaction(input_date=past):
            item = Item(source="backfill", code="ABC")
            await item.insert()

        after = dt.datetime.now(UTC)

        history = await graph._conn.query(
            f"SELECT recorded_at FROM {graph._schema}.history WHERE id = %(id)s",
            id=item.id
        )

        assert len(history) == 1
        recorded = history[0]['recorded_at']
        # recorded_at should be between before and after (wall clock), not near 2020
        assert before <= recorded <= after

    async def test_insert_future_input_date(self, graph):
        """Inserting with a future input_date should work."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        future = dt.datetime(2030, 12, 31, tzinfo=UTC)

        async with graph.transaction(input_date=future):
            item = Item(source="test", code="FUT")
            await item.insert()

        history = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
            id=item.id
        )

        assert history[0]['validity'].lower == future

    async def test_bulk_insert_with_input_date(self, graph):
        """Bulk inserts should all use the input_date."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        past = dt.datetime(2024, 3, 15, tzinfo=UTC)

        async with graph.transaction(input_date=past):
            items = [Item(source="backfill", code=f"B{i}") for i in range(5)]
            await Item.bulk_insert(items)

        for item in items:
            history = await graph._conn.query(
                f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
                id=item.id
            )
            assert len(history) == 1
            assert history[0]['validity'].lower == past


@pytest.mark.asyncio
class TestInputDateUpdate:
    """Test update operations with input_date."""

    async def test_update_with_input_date(self, graph):
        """Update should close old period and open new at input_date."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            name: str

        # Insert at T1
        t1 = dt.datetime(2025, 1, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t1):
            item = Item(source="test", code="ABC", name="Alpha")
            await item.insert()

        # Update at T2
        t2 = dt.datetime(2025, 6, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t2):
            item.name = "Alpha Core"
            await item.update()

        history = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s ORDER BY validity",
            id=item.id
        )

        assert len(history) == 2

        # First entry: (T1, T2] with old value
        assert history[0]['validity'].lower == t1
        assert history[0]['validity'].upper == t2
        assert history[0]['attr']['name'] == "Alpha"

        # Second entry: (T2, infinity] with new value
        assert history[1]['validity'].lower == t2
        assert history[1]['validity'].upper == dt.datetime.max
        assert history[1]['attr']['name'] == "Alpha Core"

    async def test_update_no_change_no_history(self, graph):
        """No-op update with input_date should not create history entries."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        t1 = dt.datetime(2025, 1, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t1):
            item = Item(source="test", code="ABC")
            await item.insert()

        # Update with same values at T2
        t2 = dt.datetime(2025, 6, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t2):
            await item.update()

        history = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
            id=item.id
        )

        # Still only 1 entry — no change detected
        assert len(history) == 1


@pytest.mark.asyncio
class TestInputDateDelete:
    """Test delete operations with input_date."""

    async def test_delete_with_input_date(self, graph):
        """Delete should close validity at input_date."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        t1 = dt.datetime(2025, 1, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t1):
            item = Item(source="test", code="ABC")
            await item.insert()

        item_id = item.id

        t2 = dt.datetime(2025, 3, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t2):
            await item.delete()

        history = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
            id=item_id
        )

        assert len(history) == 1
        assert history[0]['validity'].lower == t1
        assert history[0]['validity'].upper == t2


@pytest.mark.asyncio
class TestInputDateValidation:
    """Test validation and rejection of invalid input_date usage."""

    async def test_reject_timezone_naive_datetime(self, graph):
        """Timezone-naive datetime should raise ValueError."""
        naive = dt.datetime(2025, 1, 1, 12, 0, 0)
        with pytest.raises(ValueError, match="timezone-aware"):
            async with graph.transaction(input_date=naive):
                pass

    async def test_reject_non_datetime(self, graph):
        """Non-datetime values should raise ValueError."""
        with pytest.raises(ValueError, match="timezone-aware"):
            async with graph.transaction(input_date="2025-01-01"):
                pass

    async def test_reject_update_before_validity_start(self, graph):
        """Updating with input_date before current validity start should fail."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            name: str

        t_insert = dt.datetime(2025, 6, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t_insert):
            item = Item(source="test", code="ABC", name="Alpha")
            await item.insert()

        # Try to update at a time BEFORE the insert — should fail
        t_before = dt.datetime(2025, 1, 1, tzinfo=UTC)
        with pytest.raises(Exception, match="input_date"):
            async with graph.transaction(input_date=t_before):
                item.name = "Changed"
                await item.update()

    async def test_reject_delete_before_validity_start(self, graph):
        """Deleting with input_date before current validity start should fail."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        t_insert = dt.datetime(2025, 6, 1, tzinfo=UTC)
        async with graph.transaction(input_date=t_insert):
            item = Item(source="test", code="ABC")
            await item.insert()

        t_before = dt.datetime(2025, 1, 1, tzinfo=UTC)
        with pytest.raises(Exception, match="input_date"):
            async with graph.transaction(input_date=t_before):
                await item.delete()


@pytest.mark.asyncio
class TestInputDateIsolation:
    """Test that input_date does not leak across transactions."""

    async def test_input_date_does_not_leak(self, graph):
        """After the context exits, subsequent inserts should use now()."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        past = dt.datetime(2020, 1, 1, tzinfo=UTC)

        async with graph.transaction(input_date=past):
            item1 = Item(source="test", code="PAST")
            await item1.insert()

        # Insert outside the context — should use now()
        before = dt.datetime.now(UTC)
        item2 = Item(source="test", code="NOW")
        await item2.insert()
        after = dt.datetime.now(UTC)

        h1 = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
            id=item1.id
        )
        h2 = await graph._conn.query(
            f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
            id=item2.id
        )

        # item1 used the backdated timestamp
        assert h1[0]['validity'].lower == past

        # item2 used wall-clock time (should be recent, not 2020)
        assert h2[0]['validity'].lower >= before
        assert h2[0]['validity'].lower <= after

    async def test_multiple_operations_same_context(self, graph):
        """Multiple operations in one input_date context all share the timestamp."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        ts = dt.datetime(2024, 7, 4, tzinfo=UTC)

        async with graph.transaction(input_date=ts):
            a = Item(source="test", code="A")
            await a.insert()
            b = Item(source="test", code="B")
            await b.insert()

        for obj in [a, b]:
            history = await graph._conn.query(
                f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
                id=obj.id
            )
            assert history[0]['validity'].lower == ts


@pytest.mark.asyncio
class TestInputDateWithRelationships:
    """Test input_date with forward links and backlinks."""

    async def test_insert_with_relationships(self, graph):
        """Objects with relationships should work correctly under input_date."""
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

        await graph.maintain()

        ts = dt.datetime(2025, 3, 1, tzinfo=UTC)

        async with graph.transaction(input_date=ts):
            author = Author(source="test", name="Jane")
            await author.insert()

            book = Book(source="test", title="Chapter One", author=author)
            await book.insert()

        # Both objects should have history at ts
        for obj in [author, book]:
            history = await graph._conn.query(
                f"SELECT * FROM {graph._schema}.history WHERE id = %(id)s",
                id=obj.id
            )
            assert len(history) == 1
            assert history[0]['validity'].lower == ts
