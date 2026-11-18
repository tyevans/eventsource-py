"""Delta compression codec for sequential event stream payloads (ADR-0007, TASK-0002).

CRC32 and SHA-256 values are stored beside the data. They detect accidental
corruption only. They are not tamper or authenticity checks: an attacker who
can write a frame can also write its checksums.
"""

from __future__ import annotations

import hashlib
import zlib
from typing import Any

import orjson

from eventsource.adapters.serialization.delta_inflate import inflate_bounded
from eventsource.adapters.serialization.delta_types import (
    DEFAULT_MAX_DECOMPRESSED_BYTES,
    DeltaChainError,
    DeltaIntegrityError,
    DeltaPayload,
    _In,
)


class DeltaCodec:
    """Configurable codec for sequential stream payload delta compression.

    `max_decompressed_bytes` caps the output of `decompress`. A frame whose
    `raw_size` exceeds the cap is rejected before any decompression, so a
    small, hostile frame cannot expand past it (decompression bomb).
    """

    def __init__(
        self,
        *,
        max_chain_length: int = 32,
        ratio_threshold: float = 0.8,
        cumulative_ratio_threshold: float = 2.0,
        min_payload_size: int = 128,
        compression_level: int = 6,
        max_decompressed_bytes: int = DEFAULT_MAX_DECOMPRESSED_BYTES,
    ) -> None:
        self.max_chain_length = max_chain_length
        self.ratio_threshold = ratio_threshold
        self.cumulative_ratio_threshold = cumulative_ratio_threshold
        self.min_payload_size = min_payload_size
        self.compression_level = compression_level
        self.max_decompressed_bytes = max_decompressed_bytes

    @staticmethod
    def _to_bytes(data: _In) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
        return orjson.dumps(data, option=orjson.OPT_SORT_KEYS)

    def compress(
        self,
        payload: _In,
        *,
        base_payload: _In | None = None,
        base_version: int | None = None,
        chain_length: int = 0,
        cumulative_chain_bytes: int = 0,
    ) -> DeltaPayload:
        """Compress payload against optional base_payload or produce a standalone baseline."""
        raw_bytes = self._to_bytes(payload)
        sz, h = len(raw_bytes), hashlib.sha256(raw_bytes).hexdigest()
        if sz > self.max_decompressed_bytes:
            # Same bound decompress enforces, so compress never emits an unreadable frame.
            raise DeltaIntegrityError(f"payload size {sz} is outside the codec limit of {self.max_decompressed_bytes} bytes")  # fmt: skip
        can_delta = (
            base_payload is not None
            and base_version is not None
            and chain_length < self.max_chain_length
            and sz >= self.min_payload_size
        )
        if can_delta and base_payload is not None:
            comp = zlib.compressobj(level=self.compression_level, zdict=self._to_bytes(base_payload))  # fmt: skip
            d_bytes = comp.compress(raw_bytes) + comp.flush()
            d_sz = len(d_bytes)
            if d_sz < (sz * self.ratio_threshold) and (cumulative_chain_bytes + d_sz) < (sz * self.cumulative_ratio_threshold):  # fmt: skip
                return DeltaPayload(True, base_version, d_bytes, h, zlib.crc32(d_bytes), sz, d_sz)

        comp = zlib.compressobj(level=self.compression_level)
        c_bytes = comp.compress(raw_bytes) + comp.flush()
        return DeltaPayload(False, None, c_bytes, h, zlib.crc32(c_bytes), sz, len(c_bytes))

    def decompress(self, delta: DeltaPayload, *, base_payload: _In | None = None) -> bytes:
        """Decompress a delta payload, verifying CRC32 and SHA-256 content hash.

        The CRC32 and SHA-256 checks detect accidental corruption only. They
        do not authenticate the frame. Output is bounded by `raw_size`, which
        must not exceed `max_decompressed_bytes`.
        """
        if not 0 <= delta.raw_size <= self.max_decompressed_bytes:
            raise DeltaIntegrityError(f"raw_size={delta.raw_size} is outside the codec limit of {self.max_decompressed_bytes} bytes")  # fmt: skip
        actual_crc = zlib.crc32(delta.compressed_bytes)
        if actual_crc != delta.payload_checksum:
            raise DeltaIntegrityError(f"Checksum mismatch on compressed frame: expected {delta.payload_checksum:#x}, got {actual_crc:#x}")  # fmt: skip
        if delta.is_delta and base_payload is None:
            raise DeltaChainError(f"Cannot decompress delta payload without base_version={delta.base_version}")  # fmt: skip
        zdict = self._to_bytes(base_payload) if delta.is_delta else None  # type: ignore[arg-type]
        raw_bytes = inflate_bounded(delta, zdict)
        actual_hash = hashlib.sha256(raw_bytes).hexdigest()
        if actual_hash != delta.content_hash:
            raise DeltaIntegrityError(f"Content hash mismatch on decompressed payload: expected {delta.content_hash}, got {actual_hash}")  # fmt: skip
        return raw_bytes

    def decompress_to_dict(self, delta: DeltaPayload, *, base_payload: _In | None = None) -> Any:
        """Decompress delta payload and parse back into Python JSON object."""
        return orjson.loads(self.decompress(delta, base_payload=base_payload))


def is_delta_dict(val: Any) -> bool:
    """Return True if dictionary contains a serialized DeltaPayload."""
    return isinstance(val, dict) and bool(val.get("__delta__"))


__all__ = ["DeltaChainError", "DeltaCodec", "DeltaIntegrityError", "DeltaPayload", "is_delta_dict"]
