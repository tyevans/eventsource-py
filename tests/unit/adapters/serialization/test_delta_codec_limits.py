"""DeltaCodec keeps compress and decompress limits consistent, and reports corrupt zlib data.

Blackbox through `DeltaCodec.compress` and `DeltaCodec.decompress`. A frame that
`compress` produces must always pass `decompress`'s size check. Corrupt
compressed bytes with a valid CRC32 must surface as `DeltaIntegrityError`.
"""

from __future__ import annotations

import hashlib
import zlib

import pytest

from eventsource.adapters.serialization.delta import (
    DeltaCodec,
    DeltaIntegrityError,
    DeltaPayload,
)

KIB = 1024


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


class TestCompressRespectsDecompressLimit:
    def test_compress_rejects_input_larger_than_limit(self) -> None:
        codec = DeltaCodec(max_decompressed_bytes=KIB)
        with pytest.raises(DeltaIntegrityError, match="outside the codec limit"):
            codec.compress(b"x" * (2 * KIB))

    def test_compress_rejects_oversized_input_even_when_it_would_be_a_baseline(self) -> None:
        codec = DeltaCodec(max_decompressed_bytes=KIB)
        with pytest.raises(DeltaIntegrityError):
            codec.compress({"blob": "y" * (2 * KIB)})

    def test_compress_rejects_oversized_delta_candidate(self) -> None:
        codec = DeltaCodec(max_decompressed_bytes=KIB)
        base = b"b" * 512
        with pytest.raises(DeltaIntegrityError):
            codec.compress(b"b" * (2 * KIB), base_payload=base, base_version=1)

    def test_input_at_limit_round_trips(self) -> None:
        codec = DeltaCodec(max_decompressed_bytes=KIB)
        payload = b"z" * KIB
        frame = codec.compress(payload)
        assert codec.decompress(frame) == payload


class TestCorruptCompressedBytes:
    def test_garbage_with_valid_crc_raises_integrity_error(self) -> None:
        garbage = bytes(range(256))
        frame = _frame(garbage, raw_size=10, raw_hash="0" * 64)
        with pytest.raises(DeltaIntegrityError) as raised:
            DeltaCodec().decompress(frame)
        assert isinstance(raised.value.__cause__, zlib.error)

    def test_corrupt_stream_body_with_valid_crc_raises_integrity_error(self) -> None:
        payload = b"event payload " * 20
        good = zlib.compress(payload)
        corrupt = good[:4] + bytes(b ^ 0xFF for b in good[4:12]) + good[12:]
        frame = _frame(corrupt, raw_size=len(payload), raw_hash=hashlib.sha256(payload).hexdigest())
        with pytest.raises(DeltaIntegrityError):
            DeltaCodec().decompress(frame)
