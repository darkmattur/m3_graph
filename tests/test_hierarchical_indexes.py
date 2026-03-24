"""
Tests for hierarchical index lookups.

Tests that get() and find() and load() methods work hierarchically, so that:
- Item.get(code="ABC") can return a Widget instance
- Item.find(code="ABC") can return a Widget instance or None
- Widget.get(code="XYZ") can return a SpecialWidget instance
- Item.load() loads all descendants (Widget, Gadget, etc.)
"""
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestHierarchicalGet:
    """Test hierarchical lookups with get() method."""

    async def test_get_hierarchical_basic(self, graph):
        """Test basic hierarchical get - parent class can retrieve child instances."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            name: str
            type_unique_attr = ['code']

        class Widget(Item):
            type = "widget"
            serial_number: str | None = None

        class Gadget(Item):
            type = "gadget"
            platform: str | None = None

        # Create instances
        abc = Widget(source="test", code="ABC", name="Alpha", serial_number="sn001")
        await abc.insert()

        jkl = Gadget(source="test", code="JKL", name="Gamma", platform="platform_a")
        await jkl.insert()

        # Item.get() should find both Widget and Gadget instances
        found_abc = Item.get(code="ABC")
        assert found_abc is abc
        assert isinstance(found_abc, Widget)

        found_jkl = Item.get(code="JKL")
        assert found_jkl is jkl
        assert isinstance(found_jkl, Gadget)

        # Widget.get() should only find Widget instances
        found_widget = Widget.get(code="ABC")
        assert found_widget is abc

        # Gadget.get() should only find Gadget instances
        found_gadget = Gadget.get(code="JKL")
        assert found_gadget is jkl

        # Cross-type lookups should fail
        with pytest.raises(KeyError):
            Widget.get(code="JKL")  # JKL is a Gadget, not a Widget

        with pytest.raises(KeyError):
            Gadget.get(code="ABC")  # ABC is a Widget, not a Gadget

    async def test_get_hierarchical_three_levels(self, graph):
        """Test hierarchical get with three levels of inheritance."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        class Widget(Item):
            type = "widget"
            network: str

        class SpecialWidget(Widget):
            type = "special_widget"
            decimals: int

        class CustomWidget(Widget):
            type = "custom_widget"
            base_code: str

        # Create instances at different levels
        xyz = Widget(source="test", code="XYZ", network="network_a")
        await xyz.insert()

        def_ = SpecialWidget(source="test", code="DEF", network="network_a", decimals=6)
        await def_.insert()

        ghi = CustomWidget(
            source="test",
            code="GHI",
            network="network_a",
            base_code="XYZ"
        )
        await ghi.insert()

        # Item.get() should find all three
        assert Item.get(code="XYZ") is xyz
        assert Item.get(code="DEF") is def_
        assert Item.get(code="GHI") is ghi

        # Widget.get() should find all three (they're all widgets)
        assert Widget.get(code="XYZ") is xyz
        assert Widget.get(code="DEF") is def_
        assert Widget.get(code="GHI") is ghi

        # SpecialWidget.get() should only find DEF
        assert SpecialWidget.get(code="DEF") is def_
        with pytest.raises(KeyError):
            SpecialWidget.get(code="XYZ")
        with pytest.raises(KeyError):
            SpecialWidget.get(code="GHI")

        # CustomWidget.get() should only find GHI
        assert CustomWidget.get(code="GHI") is ghi
        with pytest.raises(KeyError):
            CustomWidget.get(code="XYZ")
        with pytest.raises(KeyError):
            CustomWidget.get(code="DEF")

    async def test_get_hierarchical_multi_column_index(self, graph):
        """Test hierarchical get with multi-column unique constraints."""

        class Transaction(graph.DBObject):
            category = "catalog"
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

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        class Widget(Item):
            type = "widget"

        abc = Widget(source="test", code="ABC")
        await abc.insert()

        # Should raise KeyError when no match found
        with pytest.raises(KeyError, match="No Item found"):
            Item.get(code="NONEXISTENT")

        with pytest.raises(KeyError, match="No Widget found"):
            Widget.get(code="NONEXISTENT")

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

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        class Widget(Item):
            type = "widget"

        class Gadget(Item):
            type = "gadget"

        await graph.maintain()

        # Create instances
        abc = Widget(source="test", code="ABC")
        await abc.insert()

        xyz = Widget(source="test", code="XYZ")
        await xyz.insert()

        jkl = Gadget(source="test", code="JKL")
        await jkl.insert()

        # Clear registry to test loading
        graph.registry.clear()
        graph.registry_type.clear()

        # Item.load() should load all items including widgets and gadgets
        await Item.load()
        loaded = list(graph.registry.values())
        assert len(loaded) == 3
        assert any(obj.code == "ABC" for obj in loaded)
        assert any(obj.code == "XYZ" for obj in loaded)
        assert any(obj.code == "JKL" for obj in loaded)

        # Verify types
        abc_loaded = next(obj for obj in loaded if obj.code == "ABC")
        assert isinstance(abc_loaded, Widget)

        jkl_loaded = next(obj for obj in loaded if obj.code == "JKL")
        assert isinstance(jkl_loaded, Gadget)

    async def test_load_hierarchical_three_levels(self, graph):
        """Test that load() works with three levels of hierarchy."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        class Widget(Item):
            type = "widget"

        class SpecialWidget(Widget):
            type = "special_widget"

        class NFT(Widget):
            type = "nft"

        await graph.maintain()

        # Create instances
        abc = Widget(source="test", code="ABC")
        await abc.insert()

        def_ = SpecialWidget(source="test", code="DEF")
        await def_.insert()

        nft1 = NFT(source="test", code="NFT1")
        await nft1.insert()

        # Clear registry
        graph.registry.clear()
        graph.registry_type.clear()

        # Item.load() should load all three
        await Item.load()
        assert len(graph.registry) == 3

        # Widget.load() should also load all three (they're all widgets)
        graph.registry.clear()
        graph.registry_type.clear()
        await Widget.load()
        assert len(graph.registry) == 3

        # SpecialWidget.load() should only load DEF
        graph.registry.clear()
        graph.registry_type.clear()
        await SpecialWidget.load()
        assert len(graph.registry) == 1
        assert list(graph.registry.values())[0].code == "DEF"

    async def test_load_hierarchical_includes_related(self, graph):
        """Test that hierarchical load includes related objects."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        class Company(graph.DBObject):
            category = "entities"
            type = "company"
            name: str
            issued_widgets: Backlink["Widget"]

        class Widget(Item):
            type = "widget"
            issuer: Link[Company, "issued_widgets"] | None = None

        await graph.maintain()

        # Create company
        company = Company(source="test", name="Acme Corp")
        await company.insert()

        # Create widget linked to company
        widget = Widget(source="test", code="ACME", issuer=company)
        await widget.insert()

        # Clear registry
        graph.registry.clear()
        graph.registry_type.clear()

        await Item.load()

        # Should load both the widget and the related company
        assert len(graph.registry) >= 1
        assert any(isinstance(obj, Widget) for obj in graph.registry.values())

    async def test_load_hierarchical_separate_branches(self, graph):
        """Test that load() respects inheritance boundaries."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            name: str

        class Widget(Item):
            type = "widget"

        class Gadget(Item):
            type = "gadget"

        class NFT(Widget):
            type = "nft"

        await graph.maintain()

        # Create instances
        abc = Widget(source="test", name="Alpha")
        await abc.insert()

        nft1 = NFT(source="test", name="NFT1")
        await nft1.insert()

        jkl = Gadget(source="test", name="Gamma")
        await jkl.insert()

        # Clear registry
        graph.registry.clear()
        graph.registry_type.clear()

        # Widget.load() should load Widget and NFT, but not Gadget
        await Widget.load()
        assert len(graph.registry) == 2
        assert all(isinstance(obj, Widget) for obj in graph.registry.values())

        # Gadget.load() should only load Gadget
        graph.registry.clear()
        graph.registry_type.clear()
        await Gadget.load()
        assert len(graph.registry) == 1
        assert isinstance(list(graph.registry.values())[0], Gadget)


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

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str

        await graph.maintain()

        # Should populate registry with nothing, not error
        await Item.load()
        assert len(graph.registry) == 0

    async def test_get_hierarchical_error_messages(self, graph):
        """Test that error messages in hierarchical get are helpful."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        class Widget(Item):
            type = "widget"
            address: str
            type_unique_attr = ['address']

        # Try to get with wrong constraint
        with pytest.raises(ValueError) as exc:
            Item.get(nonexistent="value")

        # Error should mention it checked subclasses
        assert "Item or its subclasses" in str(exc.value)
        # Should list available constraints from all classes
        assert "code" in str(exc.value)
        assert "address" in str(exc.value)


@pytest.mark.asyncio
class TestHierarchicalFind:
    """Test hierarchical lookups with find() method that returns None instead of raising KeyError."""

    async def test_find_returns_none_when_not_found(self, graph):
        """Test that find() returns None instead of raising KeyError."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        abc = Item(source="test", code="ABC")
        await abc.insert()

        # find() should return the object when found
        found = Item.find(code="ABC")
        assert found is abc

        # find() should return None when not found (not raise KeyError)
        not_found = Item.find(code="XYZ")
        assert not_found is None

        # Compare with get() which raises KeyError
        with pytest.raises(KeyError, match="No Item found"):
            Item.get(code="XYZ")

    async def test_find_hierarchical_basic(self, graph):
        """Test basic hierarchical find - parent class can retrieve child instances."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            name: str
            type_unique_attr = ['code']

        class Widget(Item):
            type = "widget"
            network_id: str

        class Gadget(Item):
            type = "gadget"
            platform: str

        abc = Widget(source="test", code="ABC", name="Alpha", network_id="ABC")
        jkl = Gadget(source="test", code="JKL", name="Gamma", platform="platform_a")
        await abc.insert()
        await jkl.insert()

        # Item.find() should find both Widget and Gadget instances
        found_abc = Item.find(code="ABC")
        assert found_abc is abc
        assert isinstance(found_abc, Widget)

        found_jkl = Item.find(code="JKL")
        assert found_jkl is jkl
        assert isinstance(found_jkl, Gadget)

        # Non-existent should return None
        not_found = Item.find(code="AAPL")
        assert not_found is None

    async def test_find_hierarchical_scoped_to_subclass(self, graph):
        """Test that find() on subclass only finds that subclass and descendants."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        class Widget(Item):
            type = "widget"
            network_id: str

        class Gadget(Item):
            type = "gadget"
            platform: str

        abc = Widget(source="test", code="ABC", network_id="ABC")
        jkl = Gadget(source="test", code="JKL", platform="platform_a")
        await abc.insert()
        await jkl.insert()

        # Widget.find() should find widget
        found_abc = Widget.find(code="ABC")
        assert found_abc is abc

        # Widget.find() should return None for gadget (not a widget)
        not_found_jkl = Widget.find(code="JKL")
        assert not_found_jkl is None

        # Gadget.find() should find gadget
        found_jkl = Gadget.find(code="JKL")
        assert found_jkl is jkl

        # Gadget.find() should return None for widget (not a gadget)
        not_found_abc = Gadget.find(code="ABC")
        assert not_found_abc is None

    async def test_find_with_computed_property(self, graph):
        """Test find() with computed property index."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            name: str
            computed_unique_attr = ['full_name']

            @property
            def full_name(self) -> str:
                return f"{self.code}:{self.name}"

        abc = Item(source="test", code="ABC", name="Alpha")
        await abc.insert()

        # Should find by computed property
        found = Item.find(full_name="ABC:Alpha")
        assert found is abc

        # Should return None for non-existent
        not_found = Item.find(full_name="XYZ:Beta")
        assert not_found is None

    async def test_find_multi_column_constraint(self, graph):
        """Test find() with multi-column unique constraint."""

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

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        abc = Item(source="test", code="ABC")
        await abc.insert()

        # Invalid constraint should raise ValueError (same as get())
        with pytest.raises(ValueError, match="No unique constraint"):
            Item.find(nonexistent="value")

    async def test_find_deeply_nested_hierarchy(self, graph):
        """Test find() with deeply nested class hierarchy."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        class Widget(Item):
            subtype = "widget"
            network_id: str

        class SpecialWidget(Widget):
            subtype = "special_widget"
            decimals: int

        class CustomWidget(SpecialWidget):
            subtype = "custom_widget"
            backing_item: str

        ghi = CustomWidget(
            source="test",
            code="GHI",
            network_id="XYZ",
            decimals=18,
            backing_item="XYZ"
        )
        await ghi.insert()

        # All levels should be able to find it
        assert Item.find(code="GHI") is ghi
        assert Widget.find(code="GHI") is ghi
        assert SpecialWidget.find(code="GHI") is ghi
        assert CustomWidget.find(code="GHI") is ghi

        # Non-existent returns None at all levels
        assert Item.find(code="DOGE") is None
        assert Widget.find(code="DOGE") is None
        assert SpecialWidget.find(code="DOGE") is None
        assert CustomWidget.find(code="DOGE") is None

    async def test_find_vs_get_consistency(self, graph):
        """Test that find() and get() return same object when found."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        abc = Item(source="test", code="ABC")
        await abc.insert()

        # Both should return same instance when found
        found_with_find = Item.find(code="ABC")
        found_with_get = Item.get(code="ABC")
        assert found_with_find is found_with_get is abc

        # find() returns None, get() raises KeyError when not found
        assert Item.find(code="XYZ") is None
        with pytest.raises(KeyError):
            Item.get(code="XYZ")

    async def test_find_after_index_removal(self, graph):
        """Test find() returns None after object is removed from indexes."""

        class Item(graph.DBObject):
            category = "catalog"
            type = "item"
            code: str
            type_unique_attr = ['code']

        abc = Item(source="test", code="ABC")
        await abc.insert()

        # Should find before removal
        assert Item.find(code="ABC") is abc

        # Remove from indexes (simulating deletion cleanup)
        abc._remove_from_indexes()

        # Should return None after removal
        assert Item.find(code="ABC") is None

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
