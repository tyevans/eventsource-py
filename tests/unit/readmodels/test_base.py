"""Unit tests for ReadModel base class."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.ports.readmodels.model import ReadModel, _camel_to_snake, _pluralize


class _SimpleModel(ReadModel):
    name: str


class _OrderModel(ReadModel):
    number: str


class _AmountModel(ReadModel):
    name: str
    amount: Decimal


class TestCamelToSnake:
    def test_simple_conversion(self) -> None:
        assert _camel_to_snake("OrderSummary") == "order_summary"

    def test_single_word(self) -> None:
        assert _camel_to_snake("Order") == "order"

    def test_multiple_capitals(self) -> None:
        assert _camel_to_snake("HTTPResponse") == "http_response"

    def test_dto_suffix(self) -> None:
        assert _camel_to_snake("UserProfileDTO") == "user_profile_dto"

    def test_already_snake_case(self) -> None:
        assert _camel_to_snake("already_snake") == "already_snake"

    def test_single_letter(self) -> None:
        assert _camel_to_snake("A") == "a"

    def test_all_caps(self) -> None:
        assert _camel_to_snake("ABC") == "abc"

    def test_mixed_case_numbers(self) -> None:
        assert _camel_to_snake("Item2View") == "item2_view"


class TestPluralize:
    def test_regular_plural(self) -> None:
        assert _pluralize("order") == "orders"
        assert _pluralize("user") == "users"
        assert _pluralize("model") == "models"

    def test_y_ending_with_consonant(self) -> None:
        assert _pluralize("summary") == "summaries"
        assert _pluralize("category") == "categories"
        assert _pluralize("entity") == "entities"

    def test_y_ending_with_vowel(self) -> None:
        assert _pluralize("day") == "days"
        assert _pluralize("key") == "keys"
        assert _pluralize("boy") == "boys"

    def test_s_ending(self) -> None:
        assert _pluralize("address") == "addresses"
        assert _pluralize("bus") == "buses"
        assert _pluralize("status") == "statuses"

    def test_x_ending(self) -> None:
        assert _pluralize("box") == "boxes"
        assert _pluralize("tax") == "taxes"
        assert _pluralize("index") == "indexes"

    def test_ch_ending(self) -> None:
        assert _pluralize("batch") == "batches"
        assert _pluralize("match") == "matches"
        assert _pluralize("search") == "searches"

    def test_sh_ending(self) -> None:
        assert _pluralize("dash") == "dashes"
        assert _pluralize("wish") == "wishes"

    def test_z_ending(self) -> None:
        assert _pluralize("quiz") == "quizes"


class TestReadModel:
    def test_create_with_required_fields(self) -> None:
        model_id = uuid4()
        model = _SimpleModel(id=model_id, name="test")
        assert model.id == model_id
        assert model.name == "test"
        assert model.version == 1
        assert model.deleted_at is None
        assert isinstance(model.created_at, datetime)
        assert isinstance(model.updated_at, datetime)

    def test_table_name_derivation(self) -> None:
        class OrderSummary(ReadModel):
            status: str

        assert OrderSummary.table_name() == "order_summaries"

    def test_table_name_with_multiple_words(self) -> None:
        class UserProfileView(ReadModel):
            email: str

        assert UserProfileView.table_name() == "user_profile_views"

    def test_table_name_override(self) -> None:
        class CustomModel(ReadModel):
            __table_name__ = "my_custom_table"
            value: int

        assert CustomModel.table_name() == "my_custom_table"

    def test_field_names(self) -> None:
        class ProductView(ReadModel):
            sku: str
            price: Decimal

        names = ProductView.field_names()
        for expected in ("id", "created_at", "updated_at", "version", "deleted_at", "sku", "price"):
            assert expected in names

    def test_custom_field_names(self) -> None:
        class ProductView(ReadModel):
            sku: str
            price: Decimal

        custom_names = ProductView.custom_field_names()
        assert "sku" in custom_names and "price" in custom_names
        for base_field in ("id", "created_at", "updated_at", "version", "deleted_at"):
            assert base_field not in custom_names

    def test_is_deleted_false(self) -> None:
        assert _SimpleModel(id=uuid4(), name="test").is_deleted() is False

    def test_is_deleted_true(self) -> None:
        model = _SimpleModel(id=uuid4(), name="test", deleted_at=datetime.now(UTC))
        assert model.is_deleted() is True

    def test_model_is_mutable(self) -> None:
        model = _SimpleModel(id=uuid4(), name="pending")
        model.name = "completed"
        assert model.name == "completed"

    def test_str_representation(self) -> None:
        class OrderModel(ReadModel):
            number: str

        model_id = uuid4()
        result = str(OrderModel(id=model_id, number="ORD-001"))
        assert "OrderModel" in result and str(model_id) in result and "version=1" in result

    def test_repr_representation(self) -> None:
        class OrderModel(ReadModel):
            number: str

        result = repr(OrderModel(id=uuid4(), number="ORD-001"))
        for expected in ("OrderModel", "id=", "version=", "created_at=", "updated_at="):
            assert expected in result

    def test_repr_with_deleted(self) -> None:
        model = _OrderModel(id=uuid4(), number="ORD-001", deleted_at=datetime.now(UTC))
        assert "deleted_at=" in repr(model)

    def test_from_attributes(self) -> None:
        class SourceObject:
            def __init__(self) -> None:
                self.id = uuid4()
                self.name = "from_attrs"
                self.created_at = datetime.now(UTC)
                self.updated_at = datetime.now(UTC)
                self.version = 1
                self.deleted_at = None

        source = SourceObject()
        model = _SimpleModel.model_validate(source)
        assert model.name == "from_attrs" and model.id == source.id

    def test_to_dict(self) -> None:
        data = _AmountModel(id=uuid4(), name="test", amount=Decimal("99.99")).to_dict()
        assert isinstance(data["id"], str) and isinstance(data["created_at"], str)
        assert data["name"] == "test" and data["version"] == 1

    def test_json_serialization(self) -> None:
        data = _AmountModel(id=uuid4(), name="test", amount=Decimal("99.99")).model_dump(
            mode="json"
        )
        assert isinstance(data["id"], str) and isinstance(data["created_at"], str)
        assert data["name"] == "test"

    def test_version_default_value(self) -> None:
        assert _SimpleModel(id=uuid4(), name="test").version == 1

    def test_version_validation(self) -> None:
        with pytest.raises(ValueError):
            _SimpleModel(id=uuid4(), name="test", version=0)

    def test_timestamps_are_utc(self) -> None:
        model = _SimpleModel(id=uuid4(), name="test")
        assert model.created_at.tzinfo is not None and model.updated_at.tzinfo is not None
