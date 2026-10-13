"""Tests for serialization module export compatibility."""

from __future__ import annotations

from uuid import uuid4

import pytest


class TestNewModuleExports:
    """Tests to verify the serialization module exports work correctly."""

    def test_import_from_serialization_module(self) -> None:
        """Test importing from eventsource.adapters.serialization works."""
        from eventsource.adapters.serialization import (
            json_dumps as sub_dumps,
        )
        from eventsource.adapters.serialization import (
            json_loads as sub_loads,
        )

        result = sub_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = sub_loads(result)
        assert isinstance(parsed, dict)

    def test_import_from_serialization_json_submodule(self) -> None:
        """Test importing from eventsource.adapters.serialization.json works."""
        from eventsource.adapters.serialization.json import (
            json_dumps as sub_dumps,
        )
        from eventsource.adapters.serialization.json import (
            json_loads as sub_loads,
        )

        result = sub_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = sub_loads(result)
        assert isinstance(parsed, dict)

    def test_legacy_encoder_not_exported(self) -> None:
        """Test that the legacy EventSourceJSONEncoder shim is no longer exported."""
        import eventsource
        import eventsource.adapters.serialization
        import eventsource.adapters.serialization.json

        assert not hasattr(eventsource, "EventSourceJSONEncoder")
        assert not hasattr(eventsource.adapters.serialization, "EventSourceJSONEncoder")
        assert not hasattr(eventsource.adapters.serialization.json, "EventSourceJSONEncoder")
        with pytest.raises(AttributeError):
            _ = eventsource.EventSourceJSONEncoder
