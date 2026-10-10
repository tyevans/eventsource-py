"""
Delta compression codec for sequential event stream payloads.

Implements dictionary-based delta compression for sequential event streams
where whole documents are modified across revisions. Uses RFC 1950 zlib preset
dictionaries with content hash and per-payload checksums, maintaining
reconstruction safety below snapshot boundaries.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0003 (Blackbox Frontdoor Verification)
- TASK-0002 (Delta-Compress Stored Event Payloads)
"""

from __future__ import annotations

import base64
import hashlib
import json
import zlib
from dataclasses import dataclass
from typing import Any


class DeltaIntegrityError(ValueError):
    """Raised when delta payload checksum or content hash fails verification."""


class DeltaChainError(RuntimeError):
    """Raised when a delta chain cannot be reconstructed due to missing base."""


@dataclass(frozen=True, slots=True)
class DeltaPayload:
    """Serialized representation of a delta-compressed or baseline event payload."""

    is_delta: bool
    base_version: int | None
    compressed_bytes: bytes
    content_hash: str
    payload_checksum: int
    raw_size: int
    compressed_size: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize into a JSON-compatible dictionary for database storage."""
        return {
            "__delta__": True,
            "v": 1,
            "is_delta": self.is_delta,
            "base_version": self.base_version,
            "data": base64.b64encode(self.compressed_bytes).decode("ascii"),
            "hash": self.content_hash,
            "crc": self.payload_checksum,
            "raw_size": self.raw_size,
            "comp_size": self.compressed_size,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeltaPayload:
        """Construct from a dictionary serialized by to_dict."""
        if not data.get("__delta__"):
            raise ValueError("Dictionary does not represent a DeltaPayload")
        return cls(
            is_delta=bool(data["is_delta"]),
            base_version=data.get("base_version"),
            compressed_bytes=base64.b64decode(data["data"]),
            content_hash=str(data["hash"]),
            payload_checksum=int(data["crc"]),
            raw_size=int(data["raw_size"]),
            compressed_size=int(data["comp_size"]),
        )


class DeltaCodec:
    """Configurable codec for sequential stream payload delta compression."""

    def __init__(
        self,
        *,
        max_chain_length: int = 32,
        ratio_threshold: float = 0.8,
        cumulative_ratio_threshold: float = 2.0,
        min_payload_size: int = 128,
        compression_level: int = 6,
    ) -> None:
        """Initialize the DeltaCodec.

        Args:
            max_chain_length: Maximum consecutive deltas before forcing a fulltext baseline.
            ratio_threshold: If delta size >= ratio * fulltext, baseline instead.
            cumulative_ratio_threshold: If cumulative chain size >= threshold * fulltext, baseline.
            min_payload_size: Minimum uncompressed byte size eligible for delta compression.
            compression_level: zlib compression level (1-9).
        """
        self.max_chain_length = max_chain_length
        self.ratio_threshold = ratio_threshold
        self.cumulative_ratio_threshold = cumulative_ratio_threshold
        self.min_payload_size = min_payload_size
        self.compression_level = compression_level

    @staticmethod
    def _to_bytes(data: bytes | str | dict[str, Any] | list[Any]) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
        return json.dumps(data, sort_keys=True).encode("utf-8")

    def compress(
        self,
        payload: bytes | str | dict[str, Any] | list[Any],
        *,
        base_payload: bytes | str | dict[str, Any] | list[Any] | None = None,
        base_version: int | None = None,
        chain_length: int = 0,
        cumulative_chain_bytes: int = 0,
    ) -> DeltaPayload:
        """Compress payload against optional base_payload.

        Selects delta compression if base is provided and rules are satisfied;
        otherwise produces a self-contained baseline frame.
        """
        raw_bytes = self._to_bytes(payload)
        raw_size = len(raw_bytes)
        content_hash = hashlib.sha256(raw_bytes).hexdigest()

        can_delta = (
            base_payload is not None
            and base_version is not None
            and chain_length < self.max_chain_length
            and raw_size >= self.min_payload_size
        )

        if can_delta and base_payload is not None:
            base_bytes = self._to_bytes(base_payload)
            # RFC 1950 zlib compressor initialized with base as dictionary
            compressor = zlib.compressobj(
                level=self.compression_level,
                zdict=base_bytes,
            )
            delta_bytes = compressor.compress(raw_bytes) + compressor.flush()
            delta_size = len(delta_bytes)

            size_ratio_acceptable = delta_size < (raw_size * self.ratio_threshold)
            new_cumulative = cumulative_chain_bytes + delta_size
            cumulative_acceptable = new_cumulative < (raw_size * self.cumulative_ratio_threshold)

            if size_ratio_acceptable and cumulative_acceptable:
                crc = zlib.crc32(delta_bytes)
                return DeltaPayload(
                    is_delta=True,
                    base_version=base_version,
                    compressed_bytes=delta_bytes,
                    content_hash=content_hash,
                    payload_checksum=crc,
                    raw_size=raw_size,
                    compressed_size=delta_size,
                )

        # Baseline compression (standalone zlib frame without dictionary)
        compressor = zlib.compressobj(level=self.compression_level)
        comp_bytes = compressor.compress(raw_bytes) + compressor.flush()
        crc = zlib.crc32(comp_bytes)
        return DeltaPayload(
            is_delta=False,
            base_version=None,
            compressed_bytes=comp_bytes,
            content_hash=content_hash,
            payload_checksum=crc,
            raw_size=raw_size,
            compressed_size=len(comp_bytes),
        )

    def decompress(
        self,
        delta: DeltaPayload,
        *,
        base_payload: bytes | str | dict[str, Any] | list[Any] | None = None,
    ) -> bytes:
        """Decompress delta payload, verifying CRC32 and SHA256 content hash."""
        # 1. Verify CRC32 on compressed bytes
        actual_crc = zlib.crc32(delta.compressed_bytes)
        if actual_crc != delta.payload_checksum:
            raise DeltaIntegrityError(
                f"Checksum mismatch on compressed frame: expected {delta.payload_checksum:#x}, got {actual_crc:#x}"
            )

        # 2. Decompress
        if delta.is_delta:
            if base_payload is None:
                raise DeltaChainError(
                    f"Cannot decompress delta payload without base_version={delta.base_version}"
                )
            base_bytes = self._to_bytes(base_payload)
            decompressor = zlib.decompressobj(zdict=base_bytes)
            raw_bytes = decompressor.decompress(delta.compressed_bytes) + decompressor.flush()
        else:
            decompressor = zlib.decompressobj()
            raw_bytes = decompressor.decompress(delta.compressed_bytes) + decompressor.flush()

        # 3. Verify SHA-256 content hash
        actual_hash = hashlib.sha256(raw_bytes).hexdigest()
        if actual_hash != delta.content_hash:
            raise DeltaIntegrityError(
                f"Content hash mismatch on decompressed payload: expected {delta.content_hash}, got {actual_hash}"
            )

        return raw_bytes

    def decompress_to_dict(
        self,
        delta: DeltaPayload,
        *,
        base_payload: bytes | str | dict[str, Any] | list[Any] | None = None,
    ) -> Any:
        """Decompress delta payload and parse back into Python JSON object."""
        raw_bytes = self.decompress(delta, base_payload=base_payload)
        return json.loads(raw_bytes.decode("utf-8"))


def is_delta_dict(val: Any) -> bool:
    """Return True if dictionary contains a serialized DeltaPayload."""
    return isinstance(val, dict) and bool(val.get("__delta__"))


__all__ = [
    "DeltaChainError",
    "DeltaCodec",
    "DeltaIntegrityError",
    "DeltaPayload",
    "is_delta_dict",
]
