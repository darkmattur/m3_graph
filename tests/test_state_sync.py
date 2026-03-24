"""
State management and database synchronization tests.

Tests cover:
- In-memory object state vs database persistence
- Update/reload patterns and data freshness
- Database as source of truth
- Synchronization after external changes
- Stale data handling
- Consistency across operations
"""
import pytest
from m3_graph.link import Link


@pytest.mark.asyncio
class TestInMemoryState:
    """Test in-memory object state management."""

    async def test_in_memory_changes_visible_immediately(self, graph):
        """Test that in-memory changes are immediately visible."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()

        # Modify in-memory
        item.price = 200.0

        # Should be visible immediately
        assert item.price == 200.0

    async def test_in_memory_changes_not_persisted_without_update(self, graph):
        """Test that in-memory changes don't persist without update()."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()

        # Modify in-memory but don't update
        item.price = 200.0

        # Reload from database
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.price == 100.0  # Should be original value

    async def test_multiple_in_memory_changes_before_persist(self, graph):
        """Test multiple in-memory modifications before calling update()."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float
            name: str

        item = Item(source="test", code="ABC", price=100.0, name="Alpha")
        await item.insert()

        # Multiple in-memory changes
        item.price = 200.0
        item.price = 300.0
        item.name = "Alpha Core"
        item.price = 400.0

        # All changes visible in-memory
        assert item.price == 400.0
        assert item.name == "Alpha Core"

        # But not in database yet
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.price == 100.0
        assert reloaded.name == "Alpha"

    async def test_complex_type_in_memory_mutation(self, graph):
        """Test in-memory mutation of complex types (lists, dicts)."""
        class Config(graph.DBObject):
            category = "system"
            type = "config"
            settings: dict[str, int]
            tags: list[str]

        config = Config(
            source="test",
            settings={"timeout": 30},
            tags=["prod"]
        )
        await config.insert()

        # Mutate in-memory
        config.settings["timeout"] = 60
        config.settings["retries"] = 3
        config.tags.append("critical")

        # Changes visible in-memory
        assert config.settings == {"timeout": 60, "retries": 3}
        assert config.tags == ["prod", "critical"]

        # Not persisted yet
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[config.id]
        assert reloaded.settings == {"timeout": 30}
        assert reloaded.tags == ["prod"]


@pytest.mark.asyncio
class TestDatabasePersistence:
    """Test explicit database persistence via update()."""

    async def test_update_persists_in_memory_changes(self, graph):
        """Test that update() persists in-memory changes to database."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()

        # Modify and persist
        item.price = 200.0
        await item.update()

        # Verify persisted
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.price == 200.0

    async def test_multiple_sequential_updates(self, graph):
        """Test multiple sequential update() calls."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()

        # First update
        item.price = 200.0
        await item.update()

        # Second update
        item.price = 300.0
        await item.update()

        # Third update
        item.price = 400.0
        await item.update()

        # Verify final state
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.price == 400.0

    async def test_save_then_modify_then_reload(self, graph):
        """Test saving, modifying in-memory, then reloading."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()

        # First update
        item.price = 200.0
        await item.update()

        # More in-memory changes (not saved)
        item.price = 300.0

        # Reload should get the last saved state
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.price == 200.0  # Last saved, not in-memory value


@pytest.mark.asyncio
class TestReloadAndRollback:
    """Test reloading from database to discard in-memory changes."""

    async def test_reload_discards_in_memory_changes(self, graph):
        """Test that reloading discards unsaved in-memory changes."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Make in-memory changes
        item.price = 200.0
        assert item.price == 200.0

        # Reload from database (discards changes)
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item_id]
        assert reloaded.price == 100.0

    async def test_reload_after_partial_changes(self, graph):
        """Test reload after making partial changes to multiple fields."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float
            name: str

        item = Item(source="test", code="ABC", price=100.0, name="Alpha")
        await item.insert()
        item_id = item.id

        # Change multiple fields in-memory
        item.price = 200.0
        item.name = "Alpha Core"

        # Reload discards all changes
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item_id]
        assert reloaded.price == 100.0
        assert reloaded.name == "Alpha"


@pytest.mark.asyncio
class TestDatabaseAsSourceOfTruth:
    """Test that database is the authoritative source of truth."""

    async def test_load_reflects_database_state(self, graph):
        """Test that load() loads current database state."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Modify directly in database
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(item_id)s",
            item_id=item_id
        )

        # Load should reflect database change
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item_id]
        assert reloaded.price == 200.0

    async def test_load_after_external_insert(self, graph):
        """Test loading objects inserted externally to current session."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        # Insert via ORM
        asset1 = Item(source="test", code="ABC")
        await asset1.insert()

        # Insert directly via database
        result = await graph._conn.query(
            f"""
            INSERT INTO {graph._schema}.object (category, type, subtype, attr, source)
            VALUES ('catalog', 'item', 'item', %(attr)s, 'test')
            RETURNING id
            """,
            attr={"code": "XYZ"}
        )
        asset2_id = result[0]['id']

        # Clear and reload
        graph.registry.clear()
        await graph.load()

        # Should load both
        assert asset1.id in graph.registry
        assert asset2_id in graph.registry
        assert len(graph.registry) == 2

    async def test_load_after_external_delete(self, graph):
        """Test that load() handles externally deleted objects."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        asset1 = Item(source="test", code="ABC")
        asset2 = Item(source="test", code="XYZ")
        await asset1.insert()
        await asset2.insert()

        asset1_id = asset1.id
        asset2_id = asset2.id

        # Delete asset1 directly from database
        await graph._conn.execute(
            f"DELETE FROM {graph._schema}.object WHERE id = %(asset1_id)s",
            asset1_id=asset1_id
        )

        # Reload
        graph.registry.clear()
        await graph.load()

        # Only asset2 should be loaded
        assert asset1_id not in graph.registry
        assert asset2_id in graph.registry
        assert len(graph.registry) == 1

    async def test_load_after_external_update(self, graph):
        """Test that load() picks up external updates."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Hold reference to original object
        original = item

        # Update directly in database
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '500.0') WHERE id = %(item_id)s",
            item_id=item_id
        )

        # Original object still has old value
        assert original.price == 100.0

        # Load creates new instance with updated value
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item_id]
        assert reloaded.price == 500.0
        assert reloaded is not original


@pytest.mark.asyncio
class TestStaleData:
    """Test handling of stale in-memory data."""

    async def test_in_memory_object_can_become_stale(self, graph):
        """Test that in-memory object can have stale data."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Modify directly in database
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(item_id)s",
            item_id=item_id
        )

        # In-memory object is now stale
        assert item.price == 100.0  # Stale value

        # Database has different value
        graph.registry.clear()
        await graph.load()
        fresh = graph.registry[item_id]
        assert fresh.price == 200.0

    async def test_updating_stale_object_overwrites_database(self, graph):
        """Test that updating a stale object overwrites database (last write wins)."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Simulate external modification
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(item_id)s",
            item_id=item_id
        )

        # Update stale in-memory object
        item.price = 150.0
        await item.update()

        # Database should have 150.0 (last write wins)
        graph.registry.clear()
        await graph.load()
        reloaded = graph.registry[item_id]
        assert reloaded.price == 150.0  # Not 200.0


@pytest.mark.asyncio
class TestConsistency:
    """Test consistency in various scenarios."""

    async def test_consistency_after_full_crud_lifecycle(self, graph):
        """Test consistency through full CRUD lifecycle."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        # Insert
        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Verify in database
        graph.registry.clear()
        await graph.load()
        assert item_id in graph.registry

        # Update
        item_from_db = graph.registry[item_id]
        item_from_db.price = 200.0
        await item_from_db.update()

        # Verify update
        graph.registry.clear()
        await graph.load()
        assert graph.registry[item_id].price == 200.0

        # Delete
        item_to_delete = graph.registry[item_id]
        await item_to_delete.delete()

        # Verify deletion
        graph.registry.clear()
        await graph.load()
        assert item_id not in graph.registry

    async def test_consistency_with_mixed_operations(self, graph):
        """Test consistency when mixing in-memory and database operations."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        # Create multiple items
        abc = Item(source="test", code="ABC", price=100.0)
        xyz = Item(source="test", code="XYZ", price=50.0)
        await abc.insert()
        await xyz.insert()

        # Modify BTC in-memory and persist
        abc.price = 200.0
        await abc.update()

        # Modify ETH directly in database
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '75.0') WHERE id = %(xyz_id)s",
            xyz_id=xyz.id
        )

        # Load and verify
        graph.registry.clear()
        await graph.load()

        abc_reloaded = graph.registry[abc.id]
        xyz_reloaded = graph.registry[xyz.id]

        assert abc_reloaded.price == 200.0
        assert xyz_reloaded.price == 75.0

    async def test_object_state_across_multiple_sessions(self, graph):
        """Test that object state is consistent across multiple load cycles."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        # Create and save
        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        original_id = item.id

        # Session 1: Load and modify, then save
        graph.registry.clear()
        await graph.load()
        obj1 = graph.registry[original_id]
        obj1.price = 200.0
        await obj1.update()

        # Session 2: Load and verify
        graph.registry.clear()
        await graph.load()
        obj2 = graph.registry[original_id]
        assert obj2.price == 200.0

        # Session 3: Load, modify but don't save
        graph.registry.clear()
        await graph.load()
        obj3 = graph.registry[original_id]
        obj3.price = 300.0
        # Don't update

        # Session 4: Load and verify unsaved changes were lost
        graph.registry.clear()
        await graph.load()
        obj4 = graph.registry[original_id]
        assert obj4.price == 200.0  # Not 300.0

    async def test_in_memory_state_independence_per_object(self, graph):
        """Test that in-memory state is independent per object instance."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        # Create two different items
        abc = Item(source="test", code="ABC", price=100.0)
        xyz = Item(source="test", code="XYZ", price=50.0)
        await abc.insert()
        await xyz.insert()

        # Modify both in-memory
        abc.price = 200.0
        xyz.price = 100.0

        # Update only abc
        await abc.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        abc_reloaded = graph.registry[abc.id]
        xyz_reloaded = graph.registry[xyz.id]

        assert abc_reloaded.price == 200.0  # Persisted
        assert xyz_reloaded.price == 50.0  # Not persisted


@pytest.mark.asyncio
class TestConcurrentModifications:
    """Test behavior with concurrent-style modifications."""

    async def test_last_write_wins(self, graph):
        """Test that last write wins in simple conflict scenario."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()

        # Simulate two "concurrent" updates (sequential but both from original state)
        # First update
        item.price = 200.0
        await item.update()

        # Simulate another session loaded original state and modified
        # (We simulate by updating DB directly)
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '300.0') WHERE id = %(item_id)s",
            item_id=item.id
        )

        # Now in-memory object updates again
        item.price = 250.0
        await item.update()

        # Last write (250.0) should win
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.price == 250.0

    async def test_independent_field_updates(self, graph):
        """Test that updates to different fields work independently."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float
            volume: float

        item = Item(source="test", code="ABC", price=100.0, volume=1000.0)
        await item.insert()

        # Update price
        item.price = 200.0
        await item.update()

        # Simulate external update to volume
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{volume}}', '2000.0') WHERE id = %(item_id)s",
            item_id=item.id
        )

        # Load to see both changes
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item.id]
        assert reloaded.price == 200.0
        assert reloaded.volume == 2000.0


@pytest.mark.asyncio
class TestConcurrentModificationDetection:
    """Test detection and handling of concurrent modifications."""

    async def test_concurrent_update_last_write_wins(self, graph):
        """Test that concurrent updates follow last-write-wins semantics."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Simulate two concurrent sessions
        # Session 1: Load object
        session1_item = item

        # Session 2: Load same object (simulate by reloading)
        graph.registry.clear()
        await graph.load()
        session2_item = graph.registry[item_id]

        # Session 1 modifies and saves
        session1_item.price = 200.0
        await session1_item.update()

        # Session 2 also modifies and saves (unaware of session 1's change)
        session2_item.price = 300.0
        await session2_item.update()

        # Last write (session 2) should win
        graph.registry.clear()
        await graph.load()
        final = graph.registry[item_id]
        assert final.price == 300.0  # Session 2's value

    async def test_stale_read_detection_not_implemented(self, graph):
        """Document that stale read detection is not currently implemented."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Hold reference to original object
        original = item

        # External modification
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '500.0') WHERE id = %(id)s",
            id=item_id
        )

        # Original object can still update with stale data (no version check)
        original.price = 150.0
        await original.update()  # Succeeds, overwrites external change

        # Verify last write won
        graph.registry.clear()
        await graph.load()
        reloaded = graph.registry[item_id]
        assert reloaded.price == 150.0

    async def test_concurrent_delete_and_update(self, graph):
        """Test updating object that was deleted by another session."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Session 1: Hold reference
        session1_item = item

        # Session 2: Delete the object (simulate by direct DB delete)
        await graph._conn.execute(
            f"DELETE FROM {graph._schema}.object WHERE id = %(id)s",
            id=item_id
        )

        # Session 1: Try to update (object doesn't exist anymore)
        session1_item.price = 200.0
        await session1_item.update()  # Silently succeeds but updates 0 rows

        # Verify object is still deleted
        graph.registry.clear()
        await graph.load()
        assert item_id not in graph.registry

    async def test_concurrent_relationship_changes(self, graph):
        """Test concurrent modifications to relationships."""
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
        book_id = book.id

        # Session 1: Hold reference
        session1_book = book

        # Session 2: Change author
        graph.registry.clear()
        await graph.load()
        session2_book = graph.registry[book_id]
        session2_book.author = graph.registry[author2.id]
        await session2_book.update()

        # Session 1: Change author (unaware of session 2's change)
        session1_book.author = author3
        await session1_book.update()

        # Last write wins
        graph.registry.clear()
        await graph.load()
        final = graph.registry[book_id]
        assert final.author_id == author3.id

    async def test_interleaved_modifications(self, graph):
        """Test interleaved updates to different fields."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float
            volume: float
            name: str

        item = Item(source="test", code="ABC", price=100.0, volume=1000.0, name="Alpha")
        await item.insert()
        item_id = item.id

        # Session 1: Load and modify price
        session1 = item
        session1.price = 200.0

        # Session 2: Load and modify volume
        graph.registry.clear()
        await graph.load()
        session2 = graph.registry[item_id]
        session2.volume = 2000.0

        # Session 2 saves first
        await session2.update()

        # Session 1 saves (will overwrite session 2's volume change)
        await session1.update()

        # Session 1's entire state overwrites, losing session 2's volume change
        graph.registry.clear()
        await graph.load()
        final = graph.registry[item_id]
        assert final.price == 200.0  # Session 1's change
        assert final.volume == 1000.0  # Session 2's change was lost!

    async def test_registry_state_vs_database_state(self, graph):
        """Test that registry can become out of sync with database."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()
        item_id = item.id

        # Keep reference to in-memory object
        in_memory = item

        # Modify database directly
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '500.0') WHERE id = %(id)s",
            id=item_id
        )

        # Registry still has old value
        assert in_memory.price == 100.0
        assert graph.registry[item_id].price == 100.0

        # Database has new value (can verify by fresh load)
        graph.registry.clear()
        await graph.load()
        fresh = graph.registry[item_id]
        assert fresh.price == 500.0


@pytest.mark.asyncio
class TestReloadPatterns:
    """Test various reload patterns and their effects."""

    async def test_selective_reload_not_supported(self, graph):
        """Document that selective object reload is not supported."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float

        asset1 = Item(source="test", code="ABC", price=100.0)
        asset2 = Item(source="test", code="XYZ", price=50.0)
        await asset1.insert()
        await asset2.insert()

        # Modify asset1 in database
        await graph._conn.execute(
            f"UPDATE {graph._schema}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(id)s",
            id=asset1.id
        )

        # No way to reload just asset1 - must reload all or nothing
        # This documents the limitation
        assert asset1.price == 100.0  # Still stale

        # Only option is full reload
        graph.registry.clear()
        await graph.load()
        assert graph.registry[asset1.id].price == 200.0

    async def test_partial_registry_clear(self, graph):
        """Test that you can manually remove objects from registry."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        asset1 = Item(source="test", code="ABC")
        asset2 = Item(source="test", code="XYZ")
        await asset1.insert()
        await asset2.insert()

        id1 = asset1.id
        id2 = asset2.id

        # Manually remove one object
        graph.registry.pop(id1)

        # Other object still there
        assert id1 not in graph.registry
        assert id2 in graph.registry

        # But this creates inconsistent state - type registry still has it
        assert id1 in graph.registry_type["item"]

    async def test_reload_after_mass_delete(self, graph):
        """Test reloading after many objects are deleted."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        items = [Item(source="test", code=f"CODE{i}") for i in range(10)]
        for a in items:
            await a.insert()

        # Delete half of them
        for i in range(5):
            await items[i].delete()

        # Reload
        graph.registry.clear()
        await graph.load()

        # Only 5 should remain
        assert len([a for a in graph.registry.values() if isinstance(a, Item)]) == 5

    async def test_reload_with_pending_changes(self, graph):
        """Test that reload discards all pending changes."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float
            volume: float

        item = Item(source="test", code="ABC", price=100.0, volume=1000.0)
        await item.insert()
        item_id = item.id

        # Make multiple pending changes
        item.price = 200.0
        item.volume = 2000.0
        item.code = "ALPHA"

        # Reload without saving
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[item_id]
        assert reloaded.price == 100.0
        assert reloaded.volume == 1000.0
        assert reloaded.code == "ABC"
