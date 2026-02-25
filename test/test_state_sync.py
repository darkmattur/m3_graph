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


@pytest.mark.asyncio
class TestInMemoryState:
    """Test in-memory object state management."""

    async def test_in_memory_changes_visible_immediately(self, graph):
        """Test that in-memory changes are immediately visible."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Modify in-memory
        asset.price = 200.0

        # Should be visible immediately
        assert asset.price == 200.0

    async def test_in_memory_changes_not_persisted_without_update(self, graph):
        """Test that in-memory changes don't persist without update()."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Modify in-memory but don't update
        asset.price = 200.0

        # Reload from database
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 100.0  # Should be original value

    async def test_multiple_in_memory_changes_before_persist(self, graph):
        """Test multiple in-memory modifications before calling update()."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            name: str

        asset = Asset(source="test", symbol="BTC", price=100.0, name="Bitcoin")
        await asset.insert()

        # Multiple in-memory changes
        asset.price = 200.0
        asset.price = 300.0
        asset.name = "Bitcoin Core"
        asset.price = 400.0

        # All changes visible in-memory
        assert asset.price == 400.0
        assert asset.name == "Bitcoin Core"

        # But not in database yet
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 100.0
        assert reloaded.name == "Bitcoin"

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
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Modify and persist
        asset.price = 200.0
        await asset.update()

        # Verify persisted
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0

    async def test_multiple_sequential_updates(self, graph):
        """Test multiple sequential update() calls."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # First update
        asset.price = 200.0
        await asset.update()

        # Second update
        asset.price = 300.0
        await asset.update()

        # Third update
        asset.price = 400.0
        await asset.update()

        # Verify final state
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 400.0

    async def test_save_then_modify_then_reload(self, graph):
        """Test saving, modifying in-memory, then reloading."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # First update
        asset.price = 200.0
        await asset.update()

        # More in-memory changes (not saved)
        asset.price = 300.0

        # Reload should get the last saved state
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0  # Last saved, not in-memory value


@pytest.mark.asyncio
class TestReloadAndRollback:
    """Test reloading from database to discard in-memory changes."""

    async def test_reload_discards_in_memory_changes(self, graph):
        """Test that reloading discards unsaved in-memory changes."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Make in-memory changes
        asset.price = 200.0
        assert asset.price == 200.0

        # Reload from database (discards changes)
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 100.0

    async def test_reload_after_partial_changes(self, graph):
        """Test reload after making partial changes to multiple fields."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            name: str

        asset = Asset(source="test", symbol="BTC", price=100.0, name="Bitcoin")
        await asset.insert()
        asset_id = asset.id

        # Change multiple fields in-memory
        asset.price = 200.0
        asset.name = "Bitcoin Core"

        # Reload discards all changes
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 100.0
        assert reloaded.name == "Bitcoin"


@pytest.mark.asyncio
class TestDatabaseAsSourceOfTruth:
    """Test that database is the authoritative source of truth."""

    async def test_load_reflects_database_state(self, graph):
        """Test that load() loads current database state."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Modify directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # Load should reflect database change
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 200.0

    async def test_load_after_external_insert(self, graph):
        """Test loading objects inserted externally to current session."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        # Insert via ORM
        asset1 = Asset(source="test", symbol="BTC")
        await asset1.insert()

        # Insert directly via database
        result = await graph._conn.query(
            f"""
            INSERT INTO {graph._name}.object (category, type, subtype, attr, source)
            VALUES ('financial', 'asset', 'asset', %(attr)s, 'test')
            RETURNING id
            """,
            attr={"symbol": "ETH"}
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
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset1 = Asset(source="test", symbol="BTC")
        asset2 = Asset(source="test", symbol="ETH")
        await asset1.insert()
        await asset2.insert()

        asset1_id = asset1.id
        asset2_id = asset2.id

        # Delete asset1 directly from database
        await graph._conn.execute(
            f"DELETE FROM {graph._name}.object WHERE id = %(asset1_id)s",
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
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Hold reference to original object
        original = asset

        # Update directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '500.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # Original object still has old value
        assert original.price == 100.0

        # Load creates new instance with updated value
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset_id]
        assert reloaded.price == 500.0
        assert reloaded is not original


@pytest.mark.asyncio
class TestStaleData:
    """Test handling of stale in-memory data."""

    async def test_in_memory_object_can_become_stale(self, graph):
        """Test that in-memory object can have stale data."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Modify directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # In-memory object is now stale
        assert asset.price == 100.0  # Stale value

        # Database has different value
        graph.registry.clear()
        await graph.load()
        fresh = graph.registry[asset_id]
        assert fresh.price == 200.0

    async def test_updating_stale_object_overwrites_database(self, graph):
        """Test that updating a stale object overwrites database (last write wins)."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Simulate external modification
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '200.0') WHERE id = %(asset_id)s",
            asset_id=asset_id
        )

        # Update stale in-memory object
        asset.price = 150.0
        await asset.update()

        # Database should have 150.0 (last write wins)
        graph.registry.clear()
        await graph.load()
        reloaded = graph.registry[asset_id]
        assert reloaded.price == 150.0  # Not 200.0


@pytest.mark.asyncio
class TestConsistency:
    """Test consistency in various scenarios."""

    async def test_consistency_after_full_crud_lifecycle(self, graph):
        """Test consistency through full CRUD lifecycle."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Insert
        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        asset_id = asset.id

        # Verify in database
        graph.registry.clear()
        await graph.load()
        assert asset_id in graph.registry

        # Update
        asset_from_db = graph.registry[asset_id]
        asset_from_db.price = 200.0
        await asset_from_db.update()

        # Verify update
        graph.registry.clear()
        await graph.load()
        assert graph.registry[asset_id].price == 200.0

        # Delete
        asset_to_delete = graph.registry[asset_id]
        await asset_to_delete.delete()

        # Verify deletion
        graph.registry.clear()
        await graph.load()
        assert asset_id not in graph.registry

    async def test_consistency_with_mixed_operations(self, graph):
        """Test consistency when mixing in-memory and database operations."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Create multiple assets
        btc = Asset(source="test", symbol="BTC", price=100.0)
        eth = Asset(source="test", symbol="ETH", price=50.0)
        await btc.insert()
        await eth.insert()

        # Modify BTC in-memory and persist
        btc.price = 200.0
        await btc.update()

        # Modify ETH directly in database
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '75.0') WHERE id = %(eth_id)s",
            eth_id=eth.id
        )

        # Load and verify
        graph.registry.clear()
        await graph.load()

        btc_reloaded = graph.registry[btc.id]
        eth_reloaded = graph.registry[eth.id]

        assert btc_reloaded.price == 200.0
        assert eth_reloaded.price == 75.0

    async def test_object_state_across_multiple_sessions(self, graph):
        """Test that object state is consistent across multiple load cycles."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Create and save
        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()
        original_id = asset.id

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
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        # Create two different assets
        btc = Asset(source="test", symbol="BTC", price=100.0)
        eth = Asset(source="test", symbol="ETH", price=50.0)
        await btc.insert()
        await eth.insert()

        # Modify both in-memory
        btc.price = 200.0
        eth.price = 100.0

        # Update only btc
        await btc.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        btc_reloaded = graph.registry[btc.id]
        eth_reloaded = graph.registry[eth.id]

        assert btc_reloaded.price == 200.0  # Persisted
        assert eth_reloaded.price == 50.0  # Not persisted


@pytest.mark.asyncio
class TestConcurrentModifications:
    """Test behavior with concurrent-style modifications."""

    async def test_last_write_wins(self, graph):
        """Test that last write wins in simple conflict scenario."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        # Simulate two "concurrent" updates (sequential but both from original state)
        # First update
        asset.price = 200.0
        await asset.update()

        # Simulate another session loaded original state and modified
        # (We simulate by updating DB directly)
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{price}}', '300.0') WHERE id = %(asset_id)s",
            asset_id=asset.id
        )

        # Now in-memory object updates again
        asset.price = 250.0
        await asset.update()

        # Last write (250.0) should win
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 250.0

    async def test_independent_field_updates(self, graph):
        """Test that updates to different fields work independently."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            volume: float

        asset = Asset(source="test", symbol="BTC", price=100.0, volume=1000.0)
        await asset.insert()

        # Update price
        asset.price = 200.0
        await asset.update()

        # Simulate external update to volume
        await graph._conn.execute(
            f"UPDATE {graph._name}.object SET attr = jsonb_set(attr, '{{volume}}', '2000.0') WHERE id = %(asset_id)s",
            asset_id=asset.id
        )

        # Load to see both changes
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0
        assert reloaded.volume == 2000.0
