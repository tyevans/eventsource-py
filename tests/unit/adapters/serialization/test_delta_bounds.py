"""DeltaCodec bounds decompressed output by raw_size and the codec limit (TASK-0483).

Blackbox through `DeltaCodec.compress` and `DeltaCodec.decompress`. The bomb
frames are built with `zlib` directly, because `compress` never produces a
frame that lies about its size.
"""

from __future__ import annotations

import hashlib
import zlib

import pytest

from eventsource.adapters.serialization.delta import (
    DEFAULT_MAX_DECOMPRESSED_BYTES,
    DeltaCodec,
    DeltaIntegrityError,
    DeltaPayload,
)

ONE_MIB = 1024 * 1024


def _frame(compressed: bytes, raw_size: int, raw_hash: str) -> DeltaPayload:
    """Build a baseline frame with a valid CRC32 over the compressed bytes."""
    return DeltaPayload(
        is_delta=False,
        base_version=None,
        compressed_bytes=compressed,
        content_hash=raw_hash,
        payload_checksum=zlib.crc32(compressed),
        raw_size=raw_size,
        compressed_size=len(compressed),
    )


class TestDecompressionBomb:
    def test_default_limit_is_64_mib(self) -> None:
        assert DEFAULT_MAX_DECOMPRESSED_BYTES == 64 * ONE_MIB
        assert DeltaCodec().max_decompressed_bytes == 64 * ONE_MIB

    def test_bomb_with_small_raw_size_is_rejected(self) -> None:
        # 50 MiB of zeros compresses to about 50 KB; the frame claims raw_size=1.
        bomb = zlib.compress(b"\x00" * (50 * ONE_MIB), 9)
        assert len(bomb) < 100 * 1024
        frame = _frame(bomb, raw_size=1, raw_hash=hashlib.sha256(b"\x00").hexdigest())
        codec = DeltaCodec(max_decompressed_bytes=ONE_MIB)
        with pytest.raises(DeltaIntegrityError, match="exceeds raw_size=1"):
            codec.decompress(frame)

    def test_raw_size_above_codec_limit_is_rejected_before_decompressing(self) -> None:
        small = zlib.compress(b"payload")
        frame = _frame(small, raw_size=2 * ONE_MIB, raw_hash="0" * 64)
        codec = DeltaCodec(max_decompressed_bytes=ONE_MIB)
        with pytest.raises(DeltaIntegrityError, match="outside the codec limit"):
            codec.decompress(frame)

    def test_negative_raw_size_is_rejected(self) -> None:
        frame = _frame(zlib.compress(b"payload"), raw_size=-1, raw_hash="0" * 64)
        with pytest.raises(DeltaIntegrityError, match="outside the codec limit"):
            DeltaCodec().decompress(frame)

    def test_frame_with_more_output_than_raw_size_is_rejected(self) -> None:
        data = b"x" * 500
        frame = _frame(zlib.compress(data), raw_size=100, raw_hash=hashlib.sha256(data).hexdigest())
        with pytest.raises(DeltaIntegrityError, match="exceeds raw_size=100"):
            DeltaCodec().decompress(frame)

    def test_frame_with_less_output_than_raw_size_is_rejected(self) -> None:
        data = b"y" * 100
        frame = _frame(zlib.compress(data), raw_size=500, raw_hash=hashlib.sha256(data).hexdigest())
        with pytest.raises(DeltaIntegrityError, match="does not match raw_size=500"):
            DeltaCodec().decompress(frame)


class TestNormalRoundTripWithinLimit:
    def test_baseline_round_trip_under_small_limit(self) -> None:
        codec = DeltaCodec(max_decompressed_bytes=ONE_MIB)
        original = {"document": "z" * 4096, "status": "final"}
        delta = codec.compress(original)
        assert codec.decompress_to_dict(delta) == original

    def test_delta_round_trip_under_small_limit(self) -> None:
        codec = DeltaCodec(min_payload_size=10, max_decompressed_bytes=ONE_MIB)
        base = {"doc": "line one of the document\n" * 50, "ver": 1}
        rev = {"doc": "line one of the document (edited)\n" * 50, "ver": 2}
        delta = codec.compress(rev, base_payload=base, base_version=1)
        assert delta.is_delta is True
        assert codec.decompress_to_dict(delta, base_payload=base) == rev
