"""
Tests for hierarchical index lookups.

Tests that get() and find() and load() methods work hierarchically, so that:
- Asset.get(symbol="BTC") can return a Token instance
- Asset.find(symbol="BTC") can return a Token instance or None
- Token.get(symbol="ETH") can return an ERC20Token instance
- Asset.load() loads all descendants (Token, Stock, etc.)
"""
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestHierarchicalGet:
    """Test hierarchical lookups with get() method."""

    async def test_get_hierarchical_basic(self, graph):
        """Test basic hierarchical get - parent class can retrieve child instances."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            type = "token"
            contract_address: str | None = None

        class Stock(Asset):
            type = "stock"
            exchange: str | None = None

        # Create instances
        btc = Token(source="test", symbol="BTC", name="Bitcoin", contract_address="0xbtc")
        await btc.insert()

        tsla = Stock(source="test", symbol="TSLA", name="Tesla", exchange="NASDAQ")
        await tsla.insert()

        # Asset.get() should find both Token and Stock instances
        found_btc = Asset.get(symbol="BTC")
        assert found_btc is btc
        assert isinstance(found_btc, Token)

        found_tsla = Asset.get(symbol="TSLA")
        assert found_tsla is tsla
        assert isinstance(found_tsla, Stock)

        # Token.get() should only find Token instances
        found_token = Token.get(symbol="BTC")
        assert found_token is btc

        # Stock.get() should only find Stock instances
        found_stock = Stock.get(symbol="TSLA")
        assert found_stock is tsla

        # Cross-type lookups should fail
        with pytest.raises(KeyError):
            Token.get(symbol="TSLA")  # TSLA is a Stock, not a Token

        with pytest.raises(KeyError):
            Stock.get(symbol="BTC")  # BTC is a Token, not a Stock

    async def test_get_hierarchical_three_levels(self, graph):
        """Test hierarchical get with three levels of inheritance."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            type = "token"
            blockchain: str

        class ERC20Token(Token):
            type = "erc20_token"
            decimals: int

        class WrappedToken(Token):
            type = "wrapped_token"
            underlying_symbol: str

        # Create instances at different levels
        eth = Token(source="test", symbol="ETH", blockchain="ethereum")
        await eth.insert()

        usdc = ERC20Token(source="test", symbol="USDC", blockchain="ethereum", decimals=6)
        await usdc.insert()

        weth = WrappedToken(
            source="test",
            symbol="WETH",
            blockchain="ethereum",
            underlying_symbol="ETH"
        )
        await weth.insert()

        # Asset.get() should find all three
        assert Asset.get(symbol="ETH") is eth
        assert Asset.get(symbol="USDC") is usdc
        assert Asset.get(symbol="WETH") is weth

        # Token.get() should find all three (they're all tokens)
        assert Token.get(symbol="ETH") is eth
        assert Token.get(symbol="USDC") is usdc
        assert Token.get(symbol="WETH") is weth

        # ERC20Token.get() should only find USDC
        assert ERC20Token.get(symbol="USDC") is usdc
        with pytest.raises(KeyError):
            ERC20Token.get(symbol="ETH")
        with pytest.raises(KeyError):
            ERC20Token.get(symbol="WETH")

        # WrappedToken.get() should only find WETH
        assert WrappedToken.get(symbol="WETH") is weth
        with pytest.raises(KeyError):
            WrappedToken.get(symbol="ETH")
        with pytest.raises(KeyError):
            WrappedToken.get(symbol="USDC")

    async def test_get_hierarchical_multi_column_index(self, graph):
        """Test hierarchical get with multi-column unique constraints."""

        class Transaction(graph.DBObject):
            category = "financial"
            type = "transaction"
            account: str
            date: str
            type_unique_attr = [('account', 'date')]

        class Deposit(Transaction):
            type = "deposit"
            method: str

        class Withdrawal(Transaction):
            type = "withdrawal"
            destination: str

        # Create instances
        dep = Deposit(
            source="test",
            account="ACC001",
            date="2024-01-01",
            method="bank_transfer"
        )
        await dep.insert()

        wd = Withdrawal(
            source="test",
            account="ACC002",
            date="2024-01-02",
            destination="external"
        )
        await wd.insert()

        # Transaction.get() should find both using composite key
        found_dep = Transaction.get(account="ACC001", date="2024-01-01")
        assert found_dep is dep
        assert isinstance(found_dep, Deposit)

        found_wd = Transaction.get(account="ACC002", date="2024-01-02")
        assert found_wd is wd
        assert isinstance(found_wd, Withdrawal)

    async def test_get_hierarchical_computed_property(self, graph):
        """Test hierarchical get with computed property indexes."""

        class Person(graph.DBObject):
            category = "people"
            type = "person"
            first_name: str
            last_name: str
            computed_unique_attr = ['full_name']

            @property
            def full_name(self) -> str:
                return f"{self.first_name} {self.last_name}"

        class Employee(Person):
            type = "employee"
            employee_id: str
            type_unique_attr = ['employee_id']

        class Manager(Employee):
            type = "manager"
            department: str

        # Create instances
        john = Person(source="test", first_name="John", last_name="Doe")
        await john.insert()

        jane = Employee(
            source="test",
            first_name="Jane",
            last_name="Smith",
            employee_id="EMP001"
        )
        await jane.insert()

        bob = Manager(
            source="test",
            first_name="Bob",
            last_name="Johnson",
            employee_id="EMP002",
            department="Engineering"
        )
        await bob.insert()

        # Person.get() with computed property should find all
        assert Person.get(full_name="John Doe") is john
        assert Person.get(full_name="Jane Smith") is jane
        assert Person.get(full_name="Bob Johnson") is bob

        # Employee.get() with computed property should find employees and managers
        assert Employee.get(full_name="Jane Smith") is jane
        assert Employee.get(full_name="Bob Johnson") is bob
        with pytest.raises(KeyError):
            Employee.get(full_name="John Doe")  # John is just a Person

        # Employee.get() with employee_id should work hierarchically
        assert Employee.get(employee_id="EMP001") is jane
        assert Employee.get(employee_id="EMP002") is bob

    async def test_get_hierarchical_category_constraints(self, graph):
        """Test hierarchical get with category-level constraints."""

        class BaseItem(graph.DBObject):
            category = "inventory"
            type = "base_item"
            sku: str
            name: str
            category_unique_attr = ['sku']

        class PhysicalItem(BaseItem):
            type = "physical"
            weight: float

        class DigitalItem(BaseItem):
            type = "digital"
            file_size: int

        # Create instances
        book = PhysicalItem(source="test", sku="PHY001", name="Book", weight=0.5)
        await book.insert()

        ebook = DigitalItem(source="test", sku="DIG001", name="E-Book", file_size=1024)
        await ebook.insert()

        # Category constraint should work hierarchically from base
        assert BaseItem.get(sku="PHY001") is book
        assert BaseItem.get(sku="DIG001") is ebook

        # Each type can find its own
        assert PhysicalItem.get(sku="PHY001") is book
        assert DigitalItem.get(sku="DIG001") is ebook

    async def test_get_hierarchical_diamond_inheritance(self, graph):
        """Test hierarchical get with diamond inheritance pattern."""

        class Base(graph.DBObject):
            category = "test"
            type = "base"
            name: str
            type_unique_attr = ['name']

        class MixinA(Base):
            type = "mixin_a"
            feature_a: str

        class MixinB(Base):
            type = "mixin_b"
            feature_b: str

        class Combined(MixinA):
            type = "combined"
            feature_c: str

        # Create instances
        base = Base(source="test", name="base_obj")
        await base.insert()

        a = MixinA(source="test", name="a_obj", feature_a="A")
        await a.insert()

        b = MixinB(source="test", name="b_obj", feature_b="B")
        await b.insert()

        combined = Combined(source="test", name="combined_obj", feature_a="A", feature_c="C")
        await combined.insert()

        # Base.get() should find all
        assert Base.get(name="base_obj") is base
        assert Base.get(name="a_obj") is a
        assert Base.get(name="b_obj") is b
        assert Base.get(name="combined_obj") is combined

        # MixinA.get() should find itself and Combined
        assert MixinA.get(name="a_obj") is a
        assert MixinA.get(name="combined_obj") is combined
        with pytest.raises(KeyError):
            MixinA.get(name="b_obj")

        # MixinB.get() should only find itself
        assert MixinB.get(name="b_obj") is b
        with pytest.raises(KeyError):
            MixinB.get(name="combined_obj")  # Combined doesn't inherit from MixinB

    async def test_get_hierarchical_no_match(self, graph):
        """Test that hierarchical get raises appropriate errors when no match found."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            type = "token"

        btc = Token(source="test", symbol="BTC")
        await btc.insert()

        # Should raise KeyError when no match found
        with pytest.raises(KeyError, match="No Asset found"):
            Asset.get(symbol="NONEXISTENT")

        with pytest.raises(KeyError, match="No Token found"):
            Token.get(symbol="NONEXISTENT")

    async def test_get_hierarchical_multiple_constraints(self, graph):
        """Test hierarchical get with multiple different constraint types."""

        class Product(graph.DBObject):
            category = "shop"
            type = "product"
            global_id: str
            name: str
            category_unique_attr = ['global_id']
            type_unique_attr = ['name']

        class ElectronicProduct(Product):
            type = "electronic"
            serial: str
            type_unique_attr = ['serial']

        # Create instances
        generic = Product(source="test", global_id="G001", name="Generic")
        await generic.insert()

        laptop = ElectronicProduct(
            source="test",
            global_id="G002",
            name="Laptop",
            serial="SN123"
        )
        await laptop.insert()

        # Can query by global_id (category constraint) hierarchically
        assert Product.get(global_id="G001") is generic
        assert Product.get(global_id="G002") is laptop

        # Can query by name (type constraint) hierarchically
        assert Product.get(name="Generic") is generic
        assert Product.get(name="Laptop") is laptop

        # Can query by serial (electronic-specific type constraint)
        assert Product.get(serial="SN123") is laptop
        assert ElectronicProduct.get(serial="SN123") is laptop


@pytest.mark.asyncio
class TestHierarchicalLoad:
    """Test hierarchical loading with load() method."""

    async def test_load_hierarchical_basic(self, graph):
        """Test that load() loads all descendants."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            type = "token"

        class Stock(Asset):
            type = "stock"

        await graph.maintain()

        # Create instances
        btc = Token(source="test", symbol="BTC")
        await btc.insert()

        eth = Token(source="test", symbol="ETH")
        await eth.insert()

        tsla = Stock(source="test", symbol="TSLA")
        await tsla.insert()

        # Clear registry to test loading
        graph.registry.clear()
        graph.registry_type.clear()

        # Asset.load() should load all assets including tokens and stocks
        await Asset.load()
        loaded = list(graph.registry.values())
        assert len(loaded) == 3
        assert any(obj.symbol == "BTC" for obj in loaded)
        assert any(obj.symbol == "ETH" for obj in loaded)
        assert any(obj.symbol == "TSLA" for obj in loaded)

        # Verify types
        btc_loaded = next(obj for obj in loaded if obj.symbol == "BTC")
        assert isinstance(btc_loaded, Token)

        tsla_loaded = next(obj for obj in loaded if obj.symbol == "TSLA")
        assert isinstance(tsla_loaded, Stock)

    async def test_load_hierarchical_three_levels(self, graph):
        """Test that load() works with three levels of hierarchy."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        class Token(Asset):
            type = "token"

        class ERC20Token(Token):
            type = "erc20_token"

        class NFT(Token):
            type = "nft"

        await graph.maintain()

        # Create instances
        btc = Token(source="test", symbol="BTC")
        await btc.insert()

        usdc = ERC20Token(source="test", symbol="USDC")
        await usdc.insert()

        bayc = NFT(source="test", symbol="BAYC")
        await bayc.insert()

        # Clear registry
        graph.registry.clear()
        graph.registry_type.clear()

        # Asset.load() should load all three
        await Asset.load()
        assert len(graph.registry) == 3

        # Token.load() should also load all three (they're all tokens)
        graph.registry.clear()
        graph.registry_type.clear()
        await Token.load()
        assert len(graph.registry) == 3

        # ERC20Token.load() should only load USDC
        graph.registry.clear()
        graph.registry_type.clear()
        await ERC20Token.load()
        assert len(graph.registry) == 1
        assert list(graph.registry.values())[0].symbol == "USDC"

    async def test_load_hierarchical_with_expand(self, graph):
        """Test that hierarchical load works with expand=True."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        class Company(graph.DBObject):
            category = "entities"
            type = "company"
            name: str
            issued_tokens: Backlink["Token"]

        class Token(Asset):
            type = "token"
            issuer: Link[Company, "issued_tokens"] | None = None

        await graph.maintain()

        # Create company
        company = Company(source="test", name="Acme Corp")
        await company.insert()

        # Create token linked to company
        token = Token(source="test", symbol="ACME", issuer=company)
        await token.insert()

        # Clear registry
        graph.registry.clear()
        graph.registry_type.clear()

        # Load with expansion
        await Asset.load(expand=True)

        # Should load both the token and the related company
        assert len(graph.registry) >= 1
        assert any(isinstance(obj, Token) for obj in graph.registry.values())

    async def test_load_hierarchical_separate_branches(self, graph):
        """Test that load() respects inheritance boundaries."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            name: str

        class Token(Asset):
            type = "token"

        class Stock(Asset):
            type = "stock"

        class NFT(Token):
            type = "nft"

        await graph.maintain()

        # Create instances
        btc = Token(source="test", name="Bitcoin")
        await btc.insert()

        bayc = NFT(source="test", name="BAYC")
        await bayc.insert()

        tsla = Stock(source="test", name="Tesla")
        await tsla.insert()

        # Clear registry
        graph.registry.clear()
        graph.registry_type.clear()

        # Token.load() should load Token and NFT, but not Stock
        await Token.load()
        assert len(graph.registry) == 2
        assert all(isinstance(obj, Token) for obj in graph.registry.values())

        # Stock.load() should only load Stock
        graph.registry.clear()
        graph.registry_type.clear()
        await Stock.load()
        assert len(graph.registry) == 1
        assert isinstance(list(graph.registry.values())[0], Stock)


@pytest.mark.asyncio
class TestHierarchicalEdgeCases:
    """Test edge cases in hierarchical indexing."""

    async def test_get_hierarchical_no_descendants(self, graph):
        """Test get() when a class has no descendants."""

        class LeafClass(graph.DBObject):
            category = "test"
            type = "leaf"
            name: str
            type_unique_attr = ['name']

        obj = LeafClass(source="test", name="leaf")
        await obj.insert()

        # Should still work even with no descendants
        found = LeafClass.get(name="leaf")
        assert found is obj

    async def test_get_hierarchical_conflicting_constraints(self, graph):
        """Test that more specific classes are checked first."""

        class Base(graph.DBObject):
            category = "test"
            type = "base"
            code: str
            type_unique_attr = ['code']

        class Derived(Base):
            type = "derived"
            # Same constraint, but derived is more specific
            type_unique_attr = ['code']

        base_obj = Base(source="test", code="BASE")
        await base_obj.insert()

        derived_obj = Derived(source="test", code="DERIVED")
        await derived_obj.insert()

        # Base.get() should find both
        assert Base.get(code="BASE") is base_obj
        assert Base.get(code="DERIVED") is derived_obj

        # Derived.get() should only find derived
        assert Derived.get(code="DERIVED") is derived_obj
        with pytest.raises(KeyError):
            Derived.get(code="BASE")

    async def test_load_empty_hierarchy(self, graph):
        """Test load() when no objects exist."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str

        await graph.maintain()

        # Should populate registry with nothing, not error
        await Asset.load()
        assert len(graph.registry) == 0

    async def test_get_hierarchical_error_messages(self, graph):
        """Test that error messages in hierarchical get are helpful."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            type = "token"
            address: str
            type_unique_attr = ['address']

        # Try to get with wrong constraint
        with pytest.raises(ValueError) as exc:
            Asset.get(nonexistent="value")

        # Error should mention it checked subclasses
        assert "Asset or its subclasses" in str(exc.value)
        # Should list available constraints from all classes
        assert "symbol" in str(exc.value)
        assert "address" in str(exc.value)


@pytest.mark.asyncio
class TestHierarchicalFind:
    """Test hierarchical lookups with find() method that returns None instead of raising KeyError."""

    async def test_find_returns_none_when_not_found(self, graph):
        """Test that find() returns None instead of raising KeyError."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        btc = Asset(source="test", symbol="BTC")
        await btc.insert()

        # find() should return the object when found
        found = Asset.find(symbol="BTC")
        assert found is btc

        # find() should return None when not found (not raise KeyError)
        not_found = Asset.find(symbol="ETH")
        assert not_found is None

        # Compare with get() which raises KeyError
        with pytest.raises(KeyError, match="No Asset found"):
            Asset.get(symbol="ETH")

    async def test_find_hierarchical_basic(self, graph):
        """Test basic hierarchical find - parent class can retrieve child instances."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            type = "token"
            chain: str

        class Stock(Asset):
            type = "stock"
            exchange: str

        btc = Token(source="test", symbol="BTC", name="Bitcoin", chain="BTC")
        tsla = Stock(source="test", symbol="TSLA", name="Tesla", exchange="NASDAQ")
        await btc.insert()
        await tsla.insert()

        # Asset.find() should find both Token and Stock instances
        found_btc = Asset.find(symbol="BTC")
        assert found_btc is btc
        assert isinstance(found_btc, Token)

        found_tsla = Asset.find(symbol="TSLA")
        assert found_tsla is tsla
        assert isinstance(found_tsla, Stock)

        # Non-existent should return None
        not_found = Asset.find(symbol="AAPL")
        assert not_found is None

    async def test_find_hierarchical_scoped_to_subclass(self, graph):
        """Test that find() on subclass only finds that subclass and descendants."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            type = "token"
            chain: str

        class Stock(Asset):
            type = "stock"
            exchange: str

        btc = Token(source="test", symbol="BTC", chain="BTC")
        tsla = Stock(source="test", symbol="TSLA", exchange="NASDAQ")
        await btc.insert()
        await tsla.insert()

        # Token.find() should find token
        found_btc = Token.find(symbol="BTC")
        assert found_btc is btc

        # Token.find() should return None for stock (not a token)
        not_found_tsla = Token.find(symbol="TSLA")
        assert not_found_tsla is None

        # Stock.find() should find stock
        found_tsla = Stock.find(symbol="TSLA")
        assert found_tsla is tsla

        # Stock.find() should return None for token (not a stock)
        not_found_btc = Stock.find(symbol="BTC")
        assert not_found_btc is None

    async def test_find_with_computed_property(self, graph):
        """Test find() with computed property index."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            name: str
            computed_unique_attr = ['full_name']

            @property
            def full_name(self) -> str:
                return f"{self.symbol}:{self.name}"

        btc = Asset(source="test", symbol="BTC", name="Bitcoin")
        await btc.insert()

        # Should find by computed property
        found = Asset.find(full_name="BTC:Bitcoin")
        assert found is btc

        # Should return None for non-existent
        not_found = Asset.find(full_name="ETH:Ethereum")
        assert not_found is None

    async def test_find_multi_column_constraint(self, graph):
        """Test find() with multi-column unique constraint."""

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

        # Should find with all columns
        found = Transaction.find(account="ACC001", date="2024-01-01", sequence=1)
        assert found is tx

        # Should return None with wrong value
        not_found = Transaction.find(account="ACC001", date="2024-01-01", sequence=2)
        assert not_found is None

    async def test_find_with_none_value(self, graph):
        """Test find() can match None values."""

        class Item(graph.DBObject):
            category = "test"
            type = "item"
            name: str
            code: str | None = None
            type_unique_attr = ['code']

        item = Item(source="test", name="Item", code=None)
        await item.insert()

        # Should find by None value
        found = Item.find(code=None)
        assert found is item

    async def test_find_invalid_constraint_raises_value_error(self, graph):
        """Test that find() raises ValueError for invalid constraints (not KeyError)."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        btc = Asset(source="test", symbol="BTC")
        await btc.insert()

        # Invalid constraint should raise ValueError (same as get())
        with pytest.raises(ValueError, match="No unique constraint"):
            Asset.find(nonexistent="value")

    async def test_find_deeply_nested_hierarchy(self, graph):
        """Test find() with deeply nested class hierarchy."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        class Token(Asset):
            subtype = "token"
            chain: str

        class ERC20Token(Token):
            subtype = "erc20"
            decimals: int

        class WrappedToken(ERC20Token):
            subtype = "wrapped"
            backing_asset: str

        weth = WrappedToken(
            source="test",
            symbol="WETH",
            chain="ETH",
            decimals=18,
            backing_asset="ETH"
        )
        await weth.insert()

        # All levels should be able to find it
        assert Asset.find(symbol="WETH") is weth
        assert Token.find(symbol="WETH") is weth
        assert ERC20Token.find(symbol="WETH") is weth
        assert WrappedToken.find(symbol="WETH") is weth

        # Non-existent returns None at all levels
        assert Asset.find(symbol="DOGE") is None
        assert Token.find(symbol="DOGE") is None
        assert ERC20Token.find(symbol="DOGE") is None
        assert WrappedToken.find(symbol="DOGE") is None

    async def test_find_vs_get_consistency(self, graph):
        """Test that find() and get() return same object when found."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        btc = Asset(source="test", symbol="BTC")
        await btc.insert()

        # Both should return same instance when found
        found_with_find = Asset.find(symbol="BTC")
        found_with_get = Asset.get(symbol="BTC")
        assert found_with_find is found_with_get is btc

        # find() returns None, get() raises KeyError when not found
        assert Asset.find(symbol="ETH") is None
        with pytest.raises(KeyError):
            Asset.get(symbol="ETH")

    async def test_find_after_index_removal(self, graph):
        """Test find() returns None after object is removed from indexes."""

        class Asset(graph.DBObject):
            category = "financial"
            type = "asset"
            symbol: str
            type_unique_attr = ['symbol']

        btc = Asset(source="test", symbol="BTC")
        await btc.insert()

        # Should find before removal
        assert Asset.find(symbol="BTC") is btc

        # Remove from indexes (simulating deletion cleanup)
        btc._remove_from_indexes()

        # Should return None after removal
        assert Asset.find(symbol="BTC") is None

    async def test_find_with_inherited_constraints(self, graph):
        """Test find() with constraints inherited from base class."""

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

        # Should find by both inherited and own constraints
        assert ElectronicProduct.find(sku="SKU001") is product
        assert ElectronicProduct.find(voltage=220) is product

        # Should return None for non-existent
        assert ElectronicProduct.find(sku="SKU999") is None
        assert ElectronicProduct.find(voltage=110) is None
