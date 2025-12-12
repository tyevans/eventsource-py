"""Unit tests for ReadModel base class."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.readmodels import ReadModel
from eventsource.readmodels.base import _camel_to_snake, _pluralize


class TestCamelToSnake:
    """Tests for _camel_to_snake helper function."""

    def test_simple_conversion(self) -> None:
        """Test basic CamelCase to snake_case conversion."""
        assert _camel_to_snake("OrderSummary") == "order_summary"

    def test_single_word(self) -> None:
        """Test single word conversion."""
        assert _camel_to_snake("Order") == "order"

    def test_multiple_capitals(self) -> None:
        """Test conversion with consecutive capitals."""
        assert _camel_to_snake("HTTPResponse") == "http_response"

    def test_dto_suffix(self) -> None:
        """Test conversion with DTO suffix."""
        assert _camel_to_snake("UserProfileDTO") == "user_profile_dto"

    def test_already_snake_case(self) -> None:
        """Test that snake_case input is unchanged."""
        assert _camel_to_snake("already_snake") == "already_snake"

    def test_single_letter(self) -> None:
        """Test single letter conversion."""
        assert _camel_to_snake("A") == "a"

    def test_all_caps(self) -> None:
        """Test all caps conversion."""
        assert _camel_to_snake("ABC") == "abc"

    def test_mixed_case_numbers(self) -> None:
        """Test conversion with numbers."""
        assert _camel_to_snake("Item2View") == "item2_view"


class TestPluralize:
    """Tests for _pluralize helper function."""

    def test_regular_plural(self) -> None:
        """Test regular words that just add 's'."""
        assert _pluralize("order") == "orders"
        assert _pluralize("user") == "users"
        assert _pluralize("model") == "models"

    def test_y_ending_with_consonant(self) -> None:
        """Test words ending in 'y' preceded by consonant."""
        assert _pluralize("summary") == "summaries"
        assert _pluralize("category") == "categories"
        assert _pluralize("entity") == "entities"

    def test_y_ending_with_vowel(self) -> None:
        """Test words ending in 'y' preceded by vowel."""
        assert _pluralize("day") == "days"
        assert _pluralize("key") == "keys"
        assert _pluralize("boy") == "boys"

    def test_s_ending(self) -> None:
        """Test words ending in 's'."""
        assert _pluralize("address") == "addresses"
        assert _pluralize("bus") == "buses"
        assert _pluralize("status") == "statuses"

    def test_x_ending(self) -> None:
        """Test words ending in 'x'."""
        assert _pluralize("box") == "boxes"
        assert _pluralize("tax") == "taxes"
        assert _pluralize("index") == "indexes"

    def test_ch_ending(self) -> None:
        """Test words ending in 'ch'."""
        assert _pluralize("batch") == "batches"
        assert _pluralize("match") == "matches"
        assert _pluralize("search") == "searches"

    def test_sh_ending(self) -> None:
        """Test words ending in 'sh'."""
        assert _pluralize("dash") == "dashes"
        assert _pluralize("wish") == "wishes"

    def test_z_ending(self) -> None:
        """Test words ending in 'z'."""
        assert _pluralize("quiz") == "quizes"


class TestReadModel:
    """Tests for ReadModel base class."""

    def test_create_with_required_fields(self) -> None:
        """Test creating read model with only required fields."""

        class SimpleModel(ReadModel):
            name: str

        model_id = uuid4()
        model = SimpleModel(id=model_id, name="test")

        assert model.id == model_id
        assert model.name == "test"
        assert model.version == 1
        assert model.deleted_at is None
        assert isinstance(model.created_at, datetime)
        assert isinstance(model.updated_at, datetime)

    def test_table_name_derivation(self) -> None:
        """Test automatic table name derivation."""

        class OrderSummary(ReadModel):
            status: str

        assert OrderSummary.table_name() == "order_summaries"

    def test_table_name_with_multiple_words(self) -> None:
        """Test table name for multi-word class names."""

        class UserProfileView(ReadModel):
            email: str

        assert UserProfileView.table_name() == "user_profile_views"

    def test_table_name_override(self) -> None:
        """Test explicit table name override."""

        class CustomModel(ReadModel):
            __table_name__ = "my_custom_table"
            value: int

        assert CustomModel.table_name() == "my_custom_table"

    def test_field_names(self) -> None:
        """Test field name extraction."""

        class ProductView(ReadModel):
            sku: str
            price: Decimal

        names = ProductView.field_names()
        assert "id" in names
        assert "created_at" in names
        assert "updated_at" in names
        assert "version" in names
        assert "deleted_at" in names
        assert "sku" in names
        assert "price" in names

    def test_custom_field_names(self) -> None:
        """Test custom field name extraction (excluding base fields)."""

        class ProductView(ReadModel):
            sku: str
            price: Decimal

        custom_names = ProductView.custom_field_names()
        assert "sku" in custom_names
        assert "price" in custom_names
        assert "id" not in custom_names
        assert "created_at" not in custom_names
        assert "updated_at" not in custom_names
        assert "version" not in custom_names
        assert "deleted_at" not in custom_names

    def test_is_deleted_false(self) -> None:
        """Test is_deleted when not deleted."""

        class SimpleModel(ReadModel):
            name: str

        model = SimpleModel(id=uuid4(), name="test")
        assert model.is_deleted() is False

    def test_is_deleted_true(self) -> None:
        """Test is_deleted when deleted."""

        class SimpleModel(ReadModel):
            name: str

        model = SimpleModel(id=uuid4(), name="test", deleted_at=datetime.now(UTC))
        assert model.is_deleted() is True

    def test_model_is_mutable(self) -> None:
        """Test that read models are mutable (not frozen)."""

        class SimpleModel(ReadModel):
            status: str

        model = SimpleModel(id=uuid4(), status="pending")
        model.status = "completed"  # Should not raise
        assert model.status == "completed"

    def test_str_representation(self) -> None:
        """Test string representation."""

        class OrderModel(ReadModel):
            number: str

        model_id = uuid4()
        model = OrderModel(id=model_id, number="ORD-001")

        result = str(model)
        assert "OrderModel" in result
        assert str(model_id) in result
        assert "version=1" in result

    def test_repr_representation(self) -> None:
        """Test repr for debugging."""

        class OrderModel(ReadModel):
            number: str

        model = OrderModel(id=uuid4(), number="ORD-001")
        result = repr(model)

        assert "OrderModel" in result
        assert "id=" in result
        assert "version=" in result
        assert "created_at=" in result
        assert "updated_at=" in result

    def test_repr_with_deleted(self) -> None:
        """Test repr includes deleted_at when set."""

        class OrderModel(ReadModel):
            number: str

        model = OrderModel(id=uuid4(), number="ORD-001", deleted_at=datetime.now(UTC))
        result = repr(model)
        assert "deleted_at=" in result

    def test_from_attributes(self) -> None:
        """Test construction from object with attributes."""

        class SourceObject:
            def __init__(self) -> None:
                self.id = uuid4()
                self.name = "from_attrs"
                self.created_at = datetime.now(UTC)
                self.updated_at = datetime.now(UTC)
                self.version = 1
                self.deleted_at = None

        class SimpleModel(ReadModel):
            name: str

        source = SourceObject()
        model = SimpleModel.model_validate(source)
        assert model.name == "from_attrs"
        assert model.id == source.id

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""

        class SimpleModel(ReadModel):
            name: str
            amount: Decimal

        model = SimpleModel(
            id=uuid4(),
            name="test",
            amount=Decimal("99.99"),
        )

        data = model.to_dict()
        assert isinstance(data["id"], str)  # UUID serialized to string
        assert isinstance(data["created_at"], str)  # datetime serialized to ISO string
        assert data["name"] == "test"
        assert data["version"] == 1

    def test_json_serialization(self) -> None:
        """Test JSON serialization via model_dump."""

        class SimpleModel(ReadModel):
            name: str
            amount: Decimal

        model = SimpleModel(
            id=uuid4(),
            name="test",
            amount=Decimal("99.99"),
        )

        data = model.model_dump(mode="json")
        assert isinstance(data["id"], str)  # UUID serialized to string
        assert isinstance(data["created_at"], str)  # datetime serialized to ISO string
        assert data["name"] == "test"

    def test_version_default_value(self) -> None:
        """Test that version defaults to 1."""

        class SimpleModel(ReadModel):
            name: str

        model = SimpleModel(id=uuid4(), name="test")
        assert model.version == 1

    def test_version_validation(self) -> None:
        """Test that version must be >= 1."""

        class SimpleModel(ReadModel):
            name: str

        with pytest.raises(ValueError):
            SimpleModel(id=uuid4(), name="test", version=0)

    def test_timestamps_are_utc(self) -> None:
        """Test that default timestamps are UTC."""

        class SimpleModel(ReadModel):
            name: str

        model = SimpleModel(id=uuid4(), name="test")
        assert model.created_at.tzinfo is not None
        assert model.updated_at.tzinfo is not None
