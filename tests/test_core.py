"""
Core DBObject and CRUD functionality tests.

Tests cover:
- Basic DBObject creation and attributes
- Pydantic validation and field types
- CRUD operations (Create, Read, Update, Delete)
- Object identity and registry management
- Type/subtype classification
"""
import pytest
from decimal import Decimal
from pydantic import ValidationError


class TestDBObjectBasics:
    """Test basic DBObject creation, attributes, and validation."""

    def test_simple_object_creation(self, graph):
        """Test creating a simple DBObject with basic attributes."""
        class SimpleObj(graph.DBObject):
            category = "test"
            type = "simple"
            name: str
            value: int

        obj = SimpleObj(source="test", name="test_obj", value=42)

        assert obj.id is None  # Not yet inserted
        assert obj.source == "test"
        assert obj.category == "test"
        assert obj.type == "simple"
        assert obj.name == "test_obj"
        assert obj.value == 42

    def test_subtype_defaults_to_type(self, graph):
        """Test that subtype defaults to type when not specified."""
        class TypedObj(graph.DBObject):
            category = "test"
            type = "base"
            name: str

        obj = TypedObj(source="test", name="obj")
        assert obj.subtype == "base"

    def test_explicit_subtype(self, graph):
        """Test explicit subtype specification."""
        class SubtypedObj(graph.DBObject):
            category = "test"
            type = "base"
            subtype = "specialized"
            name: str

        obj = SubtypedObj(source="test", name="obj")
        assert obj.type == "base"
        assert obj.subtype == "specialized"

    def test_pydantic_validation(self, graph):
        """Test that Pydantic validation works on DBObject."""
        class ValidatedObj(graph.DBObject):
            category = "test"
            type = "validated"
            name: str
            count: int

        # Valid creation
        obj = ValidatedObj(source="test", name="valid", count=10)
        assert obj.count == 10

        # Invalid type
        with pytest.raises(ValidationError):
            ValidatedObj(source="test", name="invalid", count="not_a_number")

        # Missing required field
        with pytest.raises(ValidationError):
            ValidatedObj(source="test", name="missing")

    def test_optional_and_nullable_fields(self, graph):
        """Test optional and nullable field handling."""
        class OptionalObj(graph.DBObject):
            category = "test"
            type = "optional"
            required: str
            optional: str | None = None

        obj1 = OptionalObj(source="test", required="value")
        assert obj1.required == "value"
        assert obj1.optional is None

        obj2 = OptionalObj(source="test", required="value", optional="provided")
        assert obj2.optional == "provided"

    def test_complex_types(self, graph):
        """Test complex field types (lists, dicts, Decimal)."""
        class ComplexObj(graph.DBObject):
            category = "test"
            type = "complex"
            amount: Decimal
            tags: list[str]
            metadata: dict[str, int]

        obj = ComplexObj(
            source="test",
            amount=Decimal("123.45"),
            tags=["tag1", "tag2"],
            metadata={"key1": 1, "key2": 2}
        )

        assert isinstance(obj.amount, Decimal)
        assert obj.amount == Decimal("123.45")
        assert obj.tags == ["tag1", "tag2"]
        assert obj.metadata == {"key1": 1, "key2": 2}

    def test_excluded_attributes(self, graph):
        """Test that excluded attributes don't get stored."""
        class ExcludedObj(graph.DBObject):
            category = "test"
            type = "excluded"
            name: str
            temp_data: str = "temporary"
            excluded_attrs = {"temp_data"}

        obj = ExcludedObj(source="test", name="obj", temp_data="should_not_persist")

        # The attribute exists on the object
        assert obj.temp_data == "should_not_persist"

        # But it's not included in _get_attr()
        attrs = obj._get_attr()
        assert "name" in attrs
        assert "temp_data" not in attrs

    def test_all_excluded_attrs_inheritance(self, graph):
        """Test that _all_excluded_attrs collects excluded_attrs from parent classes."""
        class BaseObj(graph.DBObject):
            category = "test"
            type = "base"
            name: str
            base_temp: str = "temp1"
            excluded_attrs = {"base_temp"}

        class MiddleObj(BaseObj):
            type = "middle"
            middle_data: str = "data"
            middle_temp: str = "temp2"
            excluded_attrs = {"middle_temp"}

        class DerivedObj(MiddleObj):
            type = "derived"
            derived_data: str = "data"
            derived_temp: str = "temp3"
            excluded_attrs = {"derived_temp"}

        # Test that _all_excluded_attrs includes all excluded attrs from the inheritance chain
        assert "base_temp" in DerivedObj._all_excluded_attrs
        assert "middle_temp" in DerivedObj._all_excluded_attrs
        assert "derived_temp" in DerivedObj._all_excluded_attrs

        # Create an instance and verify all excluded attrs are excluded from _get_attr()
        obj = DerivedObj(
            source="test",
            name="test",
            base_temp="b",
            middle_data="m",
            middle_temp="mt",
            derived_data="d",
            derived_temp="dt"
        )

        attrs = obj._get_attr()
        assert "name" in attrs
        assert "middle_data" in attrs
        assert "derived_data" in attrs
        assert "base_temp" not in attrs
        assert "middle_temp" not in attrs
        assert "derived_temp" not in attrs

    def test_inheritance(self, graph):
        """Test that DBObject inheritance works correctly."""
        class BaseModel(graph.DBObject):
            category = "test"
            type = "base"
            base_field: str

        class DerivedModel(BaseModel):
            type = "derived"
            derived_field: int

        derived = DerivedModel(source="test", base_field="base", derived_field=42)

        assert derived.category == "test"
        assert derived.type == "derived"
        assert derived.base_field == "base"
        assert derived.derived_field == 42


@pytest.mark.asyncio
class TestCRUD:
    """Test CRUD operations (Create, Read, Update, Delete)."""

    async def test_insert_basic(self, graph):
        """Test basic insert operation."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")

        # Before insert
        assert asset.id is None

        # Insert
        await asset.insert()

        # After insert
        assert asset.id is not None
        assert isinstance(asset.id, int)
        assert asset.id > 0

        # Should be in registry
        assert asset.id in graph.registry
        assert graph.registry[asset.id] is asset

    async def test_insert_already_inserted_raises_error(self, graph):
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

    async def test_update_basic(self, graph):
        """Test basic update operation."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        original_id = asset.id

        # Update name
        asset.name = "Bitcoin Core"
        await asset.update()

        # ID should remain the same
        assert asset.id == original_id

        # Verify update persisted (reload from DB)
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[original_id]
        assert reloaded.name == "Bitcoin Core"

    async def test_update_without_id_raises_error(self, graph):
        """Test that updating object without ID raises error."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")

        # Try to update without insert
        with pytest.raises(ValueError, match="without id"):
            await asset.update()

    async def test_update_preserves_other_fields(self, graph):
        """Test that updating one field doesn't affect others."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float
            name: str

        asset = Asset(source="test", symbol="BTC", price=100.0, name="Bitcoin")
        await asset.insert()

        # Modify only price
        asset.price = 200.0
        await asset.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[asset.id]
        assert reloaded.price == 200.0
        assert reloaded.name == "Bitcoin"  # Unchanged
        assert reloaded.symbol == "BTC"  # Unchanged

    async def test_update_nullable_field_to_none(self, graph):
        """Test updating a field to None."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            description: str | None = None

        item = Item(source="test", name="Item", description="Original")
        await item.insert()

        item.description = None
        await item.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = list(graph.registry.values())[0]
        assert reloaded.description is None

    async def test_upsert_insert_path(self, graph):
        """Test upsert when object doesn't have ID (insert path)."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        assert asset.id is None

        await asset.upsert()

        # Should have inserted
        assert asset.id is not None
        assert asset.id in graph.registry

    async def test_upsert_update_path(self, graph):
        """Test upsert when object has ID (update path)."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        original_id = asset.id

        # Modify and upsert
        asset.name = "Bitcoin Core"
        await asset.upsert()

        # Should have updated, not created new
        assert asset.id == original_id

        # Verify
        graph.registry.clear()
        await graph.load()

        reloaded = graph.registry[original_id]
        assert reloaded.name == "Bitcoin Core"

    async def test_delete_basic(self, graph):
        """Test basic delete operation."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id
        assert asset_id in graph.registry

        # Delete
        await asset.delete()

        # Should be removed from registry
        assert asset_id not in graph.registry
        assert asset.id is None

        # Verify deleted from database
        graph.registry.clear()
        await graph.load()
        assert asset_id not in graph.registry

    async def test_delete_without_id_raises_error(self, graph):
        """Test that deleting object without ID raises error."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")

        with pytest.raises(ValueError, match="without id"):
            await asset.delete()

    async def test_crud_with_decimal(self, graph):
        """Test CRUD operations preserve Decimal precision."""
        class Account(graph.DBObject):
            category = "financial"
            type = "account"
            balance: Decimal

        account = Account(source="test", balance=Decimal("123.45"))
        await account.insert()

        # Reload and verify precision
        graph.registry.clear()
        await graph.load()

        reloaded = list(graph.registry.values())[0]
        assert isinstance(reloaded.balance, Decimal)
        assert reloaded.balance == Decimal("123.45")

    async def test_crud_with_complex_types(self, graph):
        """Test CRUD with complex types (lists, dicts)."""
        class Config(graph.DBObject):
            category = "system"
            type = "config"
            settings: dict[str, int]
            tags: list[str]

        config = Config(
            source="test",
            settings={"timeout": 30, "retries": 3},
            tags=["production", "critical"]
        )
        await config.insert()

        # Update
        config.settings["timeout"] = 60
        config.tags.append("monitored")
        await config.update()

        # Reload and verify
        graph.registry.clear()
        await graph.load()

        reloaded = list(graph.registry.values())[0]
        assert reloaded.settings == {"timeout": 60, "retries": 3}
        assert reloaded.tags == ["production", "critical", "monitored"]


@pytest.mark.asyncio
class TestObjectIdentity:
    """Test object identity and registry management."""

    async def test_same_id_returns_same_instance(self, graph):
        """Test that accessing same ID from registry returns same instance."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Access from registry
        from_registry = graph.registry[asset.id]

        # Should be the exact same Python object
        assert from_registry is asset

    async def test_object_in_registry_after_insert(self, graph):
        """Test that object is added to registry after insert."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")

        # Not in registry before insert
        assert asset.id is None
        assert asset not in graph.registry.values()

        # In registry after insert
        await asset.insert()
        assert asset.id in graph.registry
        assert graph.registry[asset.id] is asset

    async def test_object_removed_from_registry_after_delete(self, graph):
        """Test that object is removed from registry after delete."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id
        assert asset_id in graph.registry

        # Remove from registry after delete
        await asset.delete()
        assert asset_id not in graph.registry

    async def test_object_stays_in_registry_after_update(self, graph):
        """Test that object remains in registry after update."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            price: float

        asset = Asset(source="test", symbol="BTC", price=100.0)
        await asset.insert()

        original_instance = asset

        # Update
        asset.price = 200.0
        await asset.update()

        # Still same instance in registry
        assert graph.registry[asset.id] is original_instance

    async def test_type_specific_registry(self, graph):
        """Test type-specific registry management."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        # Should be in type registry
        assert "asset" in graph.registry_type
        assert asset.id in graph.registry_type["asset"]
        assert graph.registry_type["asset"][asset.id] is asset

        # Both registries point to same instance
        from_main_registry = graph.registry[asset.id]
        from_type_registry = graph.registry_type["asset"][asset.id]
        assert from_main_registry is from_type_registry

    async def test_delete_removes_from_both_registries(self, graph):
        """Test that delete removes object from all registries."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        asset_id = asset.id
        assert asset_id in graph.registry
        assert asset_id in graph.registry_type["asset"]

        await asset.delete()

        assert asset_id not in graph.registry
        assert asset_id not in graph.registry_type.get("asset", {})

    async def test_reload_creates_new_instances(self, graph):
        """Test that reloading from database creates new Python instances."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()
        asset_id = asset.id

        original_instance = asset

        # Clear and reload
        graph.registry.clear()
        await graph.load()

        reloaded_instance = graph.registry[asset_id]

        # Should be different Python objects (new instance)
        assert reloaded_instance is not original_instance
        # But represent same database object
        assert reloaded_instance.id == original_instance.id
        assert reloaded_instance.symbol == original_instance.symbol

    async def test_identity_within_session(self, graph):
        """Test that identity is maintained within each load session."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()
        asset_id = asset.id

        # First reload
        graph.registry.clear()
        await graph.load()
        instance1 = graph.registry[asset_id]

        # Access same object multiple times in this session
        instance1_again = graph.registry[asset_id]
        assert instance1 is instance1_again

        # Second reload (new session)
        graph.registry.clear()
        await graph.load()
        instance2 = graph.registry[asset_id]

        # Within this session, same identity
        instance2_again = graph.registry[asset_id]
        assert instance2 is instance2_again

        # But different from previous session
        assert instance2 is not instance1

    async def test_creating_object_with_id_adds_to_registry(self, graph):
        """Test that creating object with existing ID adds it to registry."""
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        # Create object with ID (simulating database load)
        asset = Asset(id=999, source="test", symbol="BTC")

        # Should be in registry automatically
        assert 999 in graph.registry
        assert graph.registry[999] is asset


@pytest.mark.asyncio
class TestChangedFlag:
    """Test boolean return values from CRUD operations."""

    async def test_insert_returns_true(self, graph):
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        assert await asset.insert() is True

    async def test_update_returns_true_when_changed(self, graph):
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        asset.name = "Bitcoin Core"
        assert await asset.update() is True

    async def test_update_returns_false_when_unchanged(self, graph):
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        assert await asset.update() is False

    async def test_upsert_insert_path_returns_true(self, graph):
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        assert await asset.upsert() is True

    async def test_upsert_update_path_returns_true_when_changed(self, graph):
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str

        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        asset.name = "Bitcoin Core"
        assert await asset.upsert() is True

    async def test_upsert_update_path_returns_false_when_unchanged(self, graph):
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        assert await asset.upsert() is False

    async def test_delete_returns_true(self, graph):
        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        assert await asset.delete() is True
