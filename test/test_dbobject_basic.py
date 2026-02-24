"""
Tests for DBObject basic functionality.

Tests cover:
- Basic object creation
- Attribute storage and retrieval
- Category, type, subtype classification
- Excluded attributes
- Pydantic integration and validation
"""
import pytest
from pydantic import ValidationError
from decimal import Decimal


class TestDBObjectBasics:
    """Test basic DBObject creation and attributes."""

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

    def test_optional_fields(self, graph):
        """Test optional field handling."""

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

    def test_decimal_support(self, graph):
        """Test that Decimal values are preserved."""

        class DecimalObj(graph.DBObject):
            category = "test"
            type = "decimal"
            amount: Decimal

        obj = DecimalObj(source="test", amount=Decimal("123.45"))
        assert isinstance(obj.amount, Decimal)
        assert obj.amount == Decimal("123.45")

    def test_complex_types(self, graph):
        """Test complex field types (lists, dicts)."""

        class ComplexObj(graph.DBObject):
            category = "test"
            type = "complex"
            tags: list[str]
            metadata: dict[str, int]

        obj = ComplexObj(
            source="test",
            tags=["tag1", "tag2"],
            metadata={"key1": 1, "key2": 2}
        )

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

    def test_get_attr_filtering(self, graph):
        """Test _get_attr() correctly filters attributes."""

        class FilteredObj(graph.DBObject):
            category = "test"
            type = "filtered"
            name: str
            value: int
            optional: str | None = None

        obj = FilteredObj(source="test", name="obj", value=10)
        attrs = obj._get_attr()

        # Should include non-None values
        assert "name" in attrs
        assert "value" in attrs

        # Should exclude None values (unless it's a foreign key)
        assert "optional" not in attrs

        # Should never include id and source
        assert "id" not in attrs
        assert "source" not in attrs

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

    def test_source_is_optional(self, graph):
        """Test that source field is optional."""

        class SourceObj(graph.DBObject):
            category = "test"
            type = "source"
            name: str

        obj = SourceObj(name="no_source")
        assert obj.source is None

        obj2 = SourceObj(source="manual", name="with_source")
        assert obj2.source == "manual"
