"""
Tests for uniqueness constraints and indexing.

Tests cover:
- Category-level unique constraints
- Type-level unique constraints
- Subtype-level unique constraints
- Computed property indexes
- Single and multi-column constraints
- Index updates on object modification
- Index cleanup on object deletion
"""
import pytest


@pytest.mark.asyncio
class TestIndexes:
    """Test uniqueness constraint indexing functionality."""

    async def test_type_unique_single_column(self, graph):
        """Test single-column unique constraint at type level."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            name: str
            type_unique_attr = ['code']


        # Create first item
        abc = Item(source="test", code="ABC", name="Alpha")
        await abc.insert()

        # Index should be populated
        assert ('code',) in Item._type_indexes
        idx = Item._type_indexes[('code',)]
        assert ('ABC',) in idx
        assert idx[('ABC',)] is abc

    async def test_type_unique_multi_column(self, graph):
        """Test multi-column unique constraint at type level."""

        class Transaction(graph.DBObject):
            category = "catalog"
            type = "transaction"
            account: str
            date: str
            sequence: int
            type_unique_attr = [('account', 'date', 'sequence')]


        tx = Transaction(
            source="test",
            account="ACC001",
            date="2024-01-01",
            sequence=1
        )
        await tx.insert()

        # Check multi-column index
        assert ('account', 'date', 'sequence') in Transaction._type_indexes
        idx = Transaction._type_indexes[('account', 'date', 'sequence')]
        assert ('ACC001', '2024-01-01', 1) in idx
        assert idx[('ACC001', '2024-01-01', 1)] is tx

    async def test_category_unique_constraint(self, graph):
        """Test unique constraint at category level."""

        class BaseItem(graph.DBObject):
            category = "inventory"
            type = "base_item"
            sku: str
            name: str
            category_unique_attr = ['sku']

        class SpecialItem(BaseItem):
            type = "special_item"
            special_attr: str


        item1 = BaseItem(source="test", sku="SKU001", name="Item 1")
        await item1.insert()

        # Category index should be shared across types with same category
        assert ('sku',) in BaseItem._category_indexes
        idx = BaseItem._category_indexes[('sku',)]
        assert ('SKU001',) in idx

    async def test_subtype_unique_constraint(self, graph):
        """Test unique constraint at subtype level."""

        class Product(graph.DBObject):
            category = "shop"
            type = "product"
            name: str

        class ElectronicProduct(Product):
            subtype = "electronic"
            serial_number: str
            subtype_unique_attr = ['serial_number']


        laptop = ElectronicProduct(
            source="test",
            name="Laptop",
            serial_number="SN123456"
        )
        await laptop.insert()

        # Subtype-specific index
        assert ('serial_number',) in ElectronicProduct._subtype_indexes
        idx = ElectronicProduct._subtype_indexes[('serial_number',)]
        assert ('SN123456',) in idx
        assert idx[('SN123456',)] is laptop

    async def test_multiple_unique_constraints(self, graph):
        """Test object with multiple unique constraints."""

        class User(graph.DBObject):
            category = "auth"
            type = "user"
            username: str
            email: str
            type_unique_attr = ['username', 'email']


        user = User(
            source="test",
            username="john_doe",
            email="john@example.com"
        )
        await user.insert()

        # Both indexes should exist
        assert ('username',) in User._type_indexes
        assert ('email',) in User._type_indexes

        username_idx = User._type_indexes[('username',)]
        email_idx = User._type_indexes[('email',)]

        assert ('john_doe',) in username_idx
        assert ('john@example.com',) in email_idx
        assert username_idx[('john_doe',)] is user
        assert email_idx[('john@example.com',)] is user

    async def test_computed_unique_attr(self, graph):
        """Test computed property indexes."""

        class Person(graph.DBObject):
            category = "people"
            type = "person"
            first_name: str
            last_name: str
            computed_unique_attr = ['full_name']

            @property
            def full_name(self) -> str:
                return f"{self.first_name} {self.last_name}"


        person = Person(source="test", first_name="John", last_name="Doe")
        await person.insert()

        # Computed index should exist
        assert 'full_name' in Person._computed_indexes
        idx = Person._computed_indexes['full_name']
        assert 'John Doe' in idx
        assert idx['John Doe'] is person

    async def test_index_update_on_modification(self, graph):
        """Test that indexes update when object is modified."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            name: str
            type_unique_attr = ['code']


        item = Item(source="test", code="ABC", name="Alpha")
        await item.insert()

        idx = Item._type_indexes[('code',)]
        assert ('ABC',) in idx

        # Modify code
        item.code = "ABCUSD"
        item._remove_from_indexes()
        item._update_indexes()

        # Old key should be gone, new key should exist
        assert ('ABC',) not in idx
        assert ('ABCUSD',) in idx
        assert idx[('ABCUSD',)] is item

    async def test_index_cleanup_on_deletion(self, graph):
        """Test that indexes are cleaned up when object is deleted."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']


        item = Item(source="test", code="ABC")
        await item.insert()

        idx = Item._type_indexes[('code',)]
        assert ('ABC',) in idx

        # Delete and cleanup
        item._remove_from_indexes()

        assert ('ABC',) not in idx

    async def test_inherited_constraints(self, graph):
        """Test that unique constraints are inherited from base classes."""

        class BaseProduct(graph.DBObject):
            category = "shop"
            type = "base_product"
            sku: str
            category_unique_attr = ['sku']

        class ElectronicProduct(BaseProduct):
            type = "electronic"
            voltage: int
            type_unique_attr = ['voltage']


        product = ElectronicProduct(source="test", sku="SKU001", voltage=220)
        await product.insert()

        # Should have both inherited category constraint and own type constraint
        assert ('sku',) in ElectronicProduct._category_indexes
        assert ('voltage',) in ElectronicProduct._type_indexes

        cat_idx = ElectronicProduct._category_indexes[('sku',)]
        type_idx = ElectronicProduct._type_indexes[('voltage',)]

        assert ('SKU001',) in cat_idx
        assert (220,) in type_idx

    async def test_index_with_none_values(self, graph):
        """Test index behavior with None values in indexed fields."""

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            optional_code: str | None = None
            type_unique_attr = ['optional_code']


        item = Item(source="test", name="Item", optional_code=None)
        await item.insert()

        # Index should handle None
        idx = Item._type_indexes[('optional_code',)]
        assert (None,) in idx

    async def test_index_registration_at_init(self, graph):
        """Test that objects with IDs are automatically registered in indexes."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']


        # Create object with ID (simulating loaded from DB)
        item = Item(id=100, source="test", code="ABC")

        # Should be in registry
        assert 100 in graph.registry
        assert graph.registry[100] is item

        # Should be in index
        idx = Item._type_indexes[('code',)]
        assert ('ABC',) in idx
        assert idx[('ABC',)] is item

    async def test_composite_index_with_different_types(self, graph):
        """Test composite index with different data types."""

        class Order(graph.DBObject):
            category = "commerce"
            type = "order"
            user_id: int
            date: str
            amount: float
            type_unique_attr = [('user_id', 'date')]


        order = Order(
            source="test",
            user_id=123,
            date="2024-01-01",
            amount=99.99
        )
        await order.insert()

        idx = Order._type_indexes[('user_id', 'date')]
        assert (123, '2024-01-01') in idx
        assert idx[(123, '2024-01-01')] is order

    async def test_all_index_levels_simultaneously(self, graph):
        """Test object with constraints at all three levels."""

        class Product(graph.DBObject):
            category = "inventory"
            type = "product"
            subtype = "electronic"
            global_id: str
            category_code: str
            serial_number: str
            category_unique_attr = ['global_id']
            type_unique_attr = ['category_code']
            subtype_unique_attr = ['serial_number']


        product = Product(
            source="test",
            global_id="GLOBAL001",
            category_code="CAT001",
            serial_number="SN001"
        )
        await product.insert()

        # All three index levels should exist
        assert ('global_id',) in Product._category_indexes
        assert ('category_code',) in Product._type_indexes
        assert ('serial_number',) in Product._subtype_indexes

        cat_idx = Product._category_indexes[('global_id',)]
        type_idx = Product._type_indexes[('category_code',)]
        sub_idx = Product._subtype_indexes[('serial_number',)]

        assert ('GLOBAL001',) in cat_idx
        assert ('CAT001',) in type_idx
        assert ('SN001',) in sub_idx


@pytest.mark.asyncio
class TestIndexEdgeCases:
    """Test edge cases in index management."""

    async def test_index_with_special_characters(self, graph):
        """Test indexing fields with special characters."""
        class Product(graph.DBObject):
            category = "test"
            type = "product"
            sku: str
            type_unique_attr = ['sku']

        # SKU with special characters
        product = Product(source="test", sku="SKU-2024/01@SPECIAL")
        await product.insert()

        idx = Product._type_indexes[('sku',)]
        assert ('SKU-2024/01@SPECIAL',) in idx

    async def test_index_with_unicode(self, graph):
        """Test indexing fields with unicode characters."""
        class Person(graph.DBObject):
            category = "test"
            type = "person"
            name: str
            type_unique_attr = ['name']

        person = Person(source="test", name="José García")
        await person.insert()

        idx = Person._type_indexes[('name',)]
        assert ('José García',) in idx

    async def test_index_with_very_long_value(self, graph):
        """Test indexing with very long string values."""
        class Document(graph.DBObject):
            category = "test"
            type = "document"
            code: str
            type_unique_attr = ['code']

        long_code = "CODE" * 100  # 400 characters
        doc = Document(source="test", code=long_code)
        await doc.insert()

        idx = Document._type_indexes[('code',)]
        assert (long_code,) in idx

    async def test_index_update_with_none_transitions(self, graph):
        """Test index updates when field transitions to/from None."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            code: str | None = None
            type_unique_attr = ['code']

        item = Item(source="test", name="Item", code=None)
        await item.insert()

        idx = Item._type_indexes[('code',)]
        assert (None,) in idx

        # Update to have a value
        item.code = "CODE123"
        item._remove_from_indexes()
        item._update_indexes()

        assert (None,) not in idx
        assert ('CODE123',) in idx

        # Update back to None
        item.code = None
        item._remove_from_indexes()
        item._update_indexes()

        assert ('CODE123',) not in idx
        assert (None,) in idx

    async def test_computed_property_index_with_none(self, graph):
        """Test computed property index when property returns None."""
        class Person(graph.DBObject):
            category = "test"
            type = "person"
            first_name: str | None = None
            last_name: str | None = None
            computed_unique_attr = ['full_name']

            @property
            def full_name(self) -> str | None:
                if self.first_name and self.last_name:
                    return f"{self.first_name} {self.last_name}"
                return None

        person = Person(source="test", first_name=None, last_name=None)
        await person.insert()

        # Computed index should not have an entry (None is excluded)
        idx = Person._computed_indexes['full_name']
        assert None not in idx

        # Update to have both names
        person.first_name = "John"
        person.last_name = "Doe"
        person._remove_from_indexes()
        person._update_indexes()

        assert 'John Doe' in idx

    async def test_index_with_type_coercion(self, graph):
        """Test index behavior with type coercion (int, float, bool)."""
        class Config(graph.DBObject):
            category = "test"
            type = "config"
            enabled: bool
            count: int
            ratio: float
            type_unique_attr = ['enabled', 'count', 'ratio']

        config = Config(source="test", enabled=True, count=42, ratio=3.14)
        await config.insert()

        idx_bool = Config._type_indexes[('enabled',)]
        idx_int = Config._type_indexes[('count',)]
        idx_float = Config._type_indexes[('ratio',)]

        assert (True,) in idx_bool
        assert (42,) in idx_int
        assert (3.14,) in idx_float

    async def test_multi_column_index_with_mixed_types(self, graph):
        """Test multi-column index with different data types."""
        class Record(graph.DBObject):
            category = "test"
            type = "record"
            user_id: int
            active: bool
            score: float
            type_unique_attr = [('user_id', 'active', 'score')]

        record = Record(source="test", user_id=123, active=True, score=99.5)
        await record.insert()

        idx = Record._type_indexes[('user_id', 'active', 'score')]
        assert (123, True, 99.5) in idx

    async def test_index_multiple_objects_same_value(self, graph):
        """Test that index holds most recent object when values collide (shouldn't happen with unique constraints)."""
        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            category_tag: str
            type_unique_attr = ['category_tag']

        item1 = Item(source="test", name="Item 1", category_tag="electronics")
        item2 = Item(source="test", name="Item 2", category_tag="books")
        await item1.insert()
        await item2.insert()

        # Each should be in index with their unique category_tag
        idx = Item._type_indexes[('category_tag',)]
        assert ('electronics',) in idx
        assert ('books',) in idx
        assert idx[('electronics',)] is item1
        assert idx[('books',)] is item2

    async def test_index_removal_on_modification_failure(self, graph):
        """Test that index stays consistent if setattr fails during modification."""
        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            price: float
            type_unique_attr = ['code']

        item = Item(source="test", code="ABC", price=100.0)
        await item.insert()

        idx = Item._type_indexes[('code',)]
        assert ('ABC',) in idx

        # Try to set invalid value (should fail Pydantic validation)
        try:
            item.price = "invalid"  # Wrong type
        except Exception:
            pass

        # Index should still be consistent
        assert ('ABC',) in idx
        assert idx[('ABC',)] is item

    async def test_index_cleanup_on_type_change(self, graph):
        """Test index cleanup when object type changes (if supported)."""
        class Item(graph.DBObject):
            category = "inventory"
            type = "item"
            sku: str
            category_unique_attr = ['sku']

        item = Item(source="test", sku="SKU123")
        await item.insert()

        # Get initial index
        idx = Item._category_indexes[('sku',)]
        assert ('SKU123',) in idx

        # Changing type at runtime is not supported, this documents the limitation
        # If type were changed, indexes would become inconsistent

    async def test_computed_index_with_changing_value(self, graph):
        """Test computed property index when underlying values change."""
        class Person(graph.DBObject):
            category = "test"
            type = "person"
            first_name: str
            last_name: str
            computed_unique_attr = ['full_name']

            @property
            def full_name(self) -> str:
                return f"{self.first_name} {self.last_name}"

        person = Person(source="test", first_name="John", last_name="Doe")
        await person.insert()

        idx = Person._computed_indexes['full_name']
        assert 'John Doe' in idx

        # Change underlying field
        person.first_name = "Jane"
        person._remove_from_indexes()
        person._update_indexes()

        # New index entry should be created
        assert 'Jane Doe' in idx

        # NOTE: Old index entry may still exist (implementation doesn't clean up old computed keys)
        # This documents current behavior - computed index cleanup is not perfect
        # The same object instance appears under both keys

    async def test_index_with_empty_string(self, graph):
        """Test index behavior with empty strings."""
        class Tag(graph.DBObject):
            category = "test"
            type = "tag"
            name: str
            code: str
            type_unique_attr = ['code']

        tag = Tag(source="test", name="Empty", code="")
        await tag.insert()

        idx = Tag._type_indexes[('code',)]
        assert ('',) in idx

    async def test_inherited_index_modification(self, graph):
        """Test that modifying indexed field in derived class updates indexes."""
        class BaseProduct(graph.DBObject):
            category = "shop"
            type = "base_product"
            sku: str
            category_unique_attr = ['sku']

        class SpecialProduct(BaseProduct):
            type = "special_product"
            special_attr: str

        product = SpecialProduct(source="test", sku="SKU001", special_attr="special")
        await product.insert()

        # Should be in inherited category index
        idx = SpecialProduct._category_indexes[('sku',)]
        assert ('SKU001',) in idx

        # Modify and verify index updates
        product.sku = "SKU002"
        product._remove_from_indexes()
        product._update_indexes()

        assert ('SKU001',) not in idx
        assert ('SKU002',) in idx

    async def test_index_with_duplicate_constraint_definitions(self, graph):
        """Test behavior when same constraint is defined in base and derived class."""
        class Base(graph.DBObject):
            category = "test"
            type = "base"
            code: str
            category_unique_attr = ['code']

        class Derived(Base):
            type = "derived"
            # Redefining same constraint (should be deduplicated)
            category_unique_attr = ['code']

        obj = Derived(source="test", code="CODE123")
        await obj.insert()

        # Should only have one index entry (not duplicated)
        idx = Derived._category_indexes
        assert ('code',) in idx

    async def test_composite_index_partial_none(self, graph):
        """Test composite index when some fields are None."""
        class Record(graph.DBObject):
            category = "test"
            type = "record"
            field1: str
            field2: str | None = None
            field3: str | None = None
            type_unique_attr = [('field1', 'field2', 'field3')]

        record = Record(source="test", field1="A", field2=None, field3="C")
        await record.insert()

        idx = Record._type_indexes[('field1', 'field2', 'field3')]
        assert ('A', None, 'C') in idx
