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

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str
            type_unique_attr = ['symbol']


        # Create first asset
        btc = Asset(source="test", symbol="BTC", name="Bitcoin")
        await btc.insert()

        # Index should be populated
        assert ('symbol',) in Asset._type_indexes
        idx = Asset._type_indexes[('symbol',)]
        assert ('BTC',) in idx
        assert idx[('BTC',)] is btc

    async def test_type_unique_multi_column(self, graph):
        """Test multi-column unique constraint at type level."""

        class Transaction(graph.DBObject):
            category = "financial"
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

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str
            type_unique_attr = ['symbol']


        asset = Asset(source="test", symbol="BTC", name="Bitcoin")
        await asset.insert()

        idx = Asset._type_indexes[('symbol',)]
        assert ('BTC',) in idx

        # Modify symbol
        asset.symbol = "BTCUSD"
        asset._remove_from_indexes()
        asset._update_indexes()

        # Old key should be gone, new key should exist
        assert ('BTC',) not in idx
        assert ('BTCUSD',) in idx
        assert idx[('BTCUSD',)] is asset

    async def test_index_cleanup_on_deletion(self, graph):
        """Test that indexes are cleaned up when object is deleted."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']


        asset = Asset(source="test", symbol="BTC")
        await asset.insert()

        idx = Asset._type_indexes[('symbol',)]
        assert ('BTC',) in idx

        # Delete and cleanup
        asset._remove_from_indexes()

        assert ('BTC',) not in idx

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

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']


        # Create object with ID (simulating loaded from DB)
        asset = Asset(id=100, source="test", symbol="BTC")

        # Should be in registry
        assert 100 in graph.registry
        assert graph.registry[100] is asset

        # Should be in index
        idx = Asset._type_indexes[('symbol',)]
        assert ('BTC',) in idx
        assert idx[('BTC',)] is asset

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
