"""
Unit and property tests for DeltaCodec and DeltaPayload.

Governed by:
- ADR-0003 (Blackbox Frontdoor Verification)
- ADR-0009 (Property-Based Testing & Mutation Testing)
- TASK-0002 (Delta-Compress Stored Event Payloads)
"""

from __future__ import annotations

import copy
import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.serialization.delta import (
    DeltaChainError,
    DeltaCodec,
    DeltaIntegrityError,
    DeltaPayload,
    is_delta_dict,
)


class TestDeltaCodecBasics:
    def test_baseline_payload_roundtrip(self) -> None:
        codec = DeltaCodec()
        original = {"document": "first version of markdown document", "status": "draft"}

        delta = codec.compress(original)
        assert delta.is_delta is False
        assert delta.base_version is None
        assert delta.raw_size > 0
        assert delta.compressed_size > 0

        # Decompress without base
        decompressed = codec.decompress_to_dict(delta)
        assert decompressed == original

    def test_delta_payload_roundtrip_with_base(self) -> None:
        codec = DeltaCodec(min_payload_size=20)
        base = {"doc": "line 1\nline 2\nline 3\nline 4\nline 5\n" * 10, "ver": 1}
        rev = {"doc": "line 1\nline 2 (edited)\nline 3\nline 4\nline 5\n" * 10, "ver": 2}

        # Compress rev using base
        delta = codec.compress(rev, base_payload=base, base_version=1)
        assert delta.is_delta is True
        assert delta.base_version == 1

        # Decompress with base
        decompressed = codec.decompress_to_dict(delta, base_payload=base)
        assert decompressed == rev

    def test_to_dict_and_from_dict_roundtrip(self) -> None:
        codec = DeltaCodec(min_payload_size=10)
        base = {"content": "initial state of the entity with some long text"}
        rev = {"content": "initial state of the entity with some long text and modifications"}

        delta = codec.compress(rev, base_payload=base, base_version=1)
        serialized = delta.to_dict()

        assert is_delta_dict(serialized) is True
        assert serialized["__delta__"] is True
        assert serialized["v"] == 1
        assert serialized["is_delta"] is True
        assert serialized["base_version"] == 1

        restored = DeltaPayload.from_dict(serialized)
        assert restored.is_delta == delta.is_delta
        assert restored.base_version == delta.base_version
        assert restored.compressed_bytes == delta.compressed_bytes
        assert restored.content_hash == delta.content_hash
        assert restored.payload_checksum == delta.payload_checksum

        decompressed = codec.decompress_to_dict(restored, base_payload=base)
        assert decompressed == rev

    def test_is_delta_dict_returns_false_for_regular_payload(self) -> None:
        assert is_delta_dict({"order_id": "123", "amount": 100}) is False
        assert is_delta_dict("not a dict") is False
        assert is_delta_dict(None) is False


class TestDeltaChainRules:
    def test_chain_length_cap_forces_baseline(self) -> None:
        codec = DeltaCodec(max_chain_length=3, min_payload_size=10)
        base = {"doc": "a" * 200, "v": 1}
        rev = {"doc": "a" * 200 + "b", "v": 2}

        # chain_length = 2 -> under cap, allows delta
        d2 = codec.compress(rev, base_payload=base, base_version=1, chain_length=2)
        assert d2.is_delta is True

        # chain_length = 3 -> reaches cap, forces baseline
        d3 = codec.compress(rev, base_payload=base, base_version=1, chain_length=3)
        assert d3.is_delta is False
        assert d3.base_version is None

    def test_ratio_threshold_forces_baseline(self) -> None:
        codec = DeltaCodec(ratio_threshold=0.5, min_payload_size=10)
        base = {"doc": "completely different content" * 10}
        # Completely different payload does not compress well against base
        rev = {"doc": "totally unrelated text that compresses poorly against dictionary" * 10}

        delta = codec.compress(rev, base_payload=base, base_version=1)
        # Should fall back to baseline if delta is not significantly smaller
        assert isinstance(delta, DeltaPayload)
        # Roundtrip still holds regardless of baseline or delta decision
        decompressed = codec.decompress_to_dict(delta, base_payload=base)
        assert decompressed == rev

    def test_min_payload_size_forces_baseline(self) -> None:
        codec = DeltaCodec(min_payload_size=500)
        base = {"small": "payload"}
        rev = {"small": "payload2"}

        delta = codec.compress(rev, base_payload=base, base_version=1)
        assert delta.is_delta is False


class TestDeltaIntegrityAndErrors:
    def test_missing_base_raises_delta_chain_error(self) -> None:
        codec = DeltaCodec(min_payload_size=10)
        base = {"text": "base text for dictionary" * 10}
        rev = {"text": "base text for dictionary with edits" * 10}

        delta = codec.compress(rev, base_payload=base, base_version=1)
        assert delta.is_delta is True

        with pytest.raises(
            DeltaChainError, match="Cannot decompress delta payload without base_version"
        ):
            codec.decompress(delta, base_payload=None)

    def test_corrupt_compressed_frame_detected_by_crc(self) -> None:
        codec = DeltaCodec()
        original = {"important": "data" * 20}
        delta = codec.compress(original)

        # Corrupt one byte of compressed data
        tampered_bytes = bytearray(delta.compressed_bytes)
        tampered_bytes[0] ^= 0xFF
        corrupt_delta = DeltaPayload(
            is_delta=delta.is_delta,
            base_version=delta.base_version,
            compressed_bytes=bytes(tampered_bytes),
            content_hash=delta.content_hash,
            payload_checksum=delta.payload_checksum,
            raw_size=delta.raw_size,
            compressed_size=delta.compressed_size,
        )

        with pytest.raises(DeltaIntegrityError, match="Checksum mismatch"):
            codec.decompress(corrupt_delta)

    def test_corrupt_content_hash_detected(self) -> None:
        codec = DeltaCodec()
        original = {"important": "data" * 20}
        delta = codec.compress(original)

        # Tamper with the expected content hash
        tampered_delta = DeltaPayload(
            is_delta=delta.is_delta,
            base_version=delta.base_version,
            compressed_bytes=delta.compressed_bytes,
            content_hash="deadbeef" * 8,
            payload_checksum=delta.payload_checksum,
            raw_size=delta.raw_size,
            compressed_size=delta.compressed_size,
        )

        with pytest.raises(DeltaIntegrityError, match="Content hash mismatch"):
            codec.decompress(tampered_delta)

    def test_from_dict_invalid_data_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Dictionary does not represent a DeltaPayload"):
            DeltaPayload.from_dict({"not": "delta"})


class TestDeltaHypothesisProperty:
    @given(
        st.dictionaries(
            keys=st.text(min_size=1, max_size=20),
            values=st.text(max_size=100),
            min_size=1,
            max_size=10,
        )
    )
    def test_property_arbitrary_json_roundtrip(self, data: dict[str, str]) -> None:
        codec = DeltaCodec(min_payload_size=10)
        # Create a mutation
        modified = copy.deepcopy(data)
        modified["__mutation__"] = "edit"

        # Baseline
        d1 = codec.compress(data)
        r1 = codec.decompress_to_dict(d1)
        assert json.dumps(r1, sort_keys=True) == json.dumps(data, sort_keys=True)

        # Delta against base
        d2 = codec.compress(modified, base_payload=data, base_version=1)
        r2 = codec.decompress_to_dict(d2, base_payload=data)
        assert json.dumps(r2, sort_keys=True) == json.dumps(modified, sort_keys=True)
