"""Delta frame types and integrity errors shared by the delta codec and its inflate helper."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any

from eventsource.domain.exceptions import EventSourceError

DEFAULT_MAX_DECOMPRESSED_BYTES = 64 * 1024 * 1024


class DeltaIntegrityError(EventSourceError, ValueError):
    """Raised when a frame fails its CRC32, SHA-256, or size check.

    The CRC32 and SHA-256 checks detect accidental corruption only. They do
    not authenticate the frame.
    """


class DeltaChainError(EventSourceError, RuntimeError):
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


type _In = bytes | str | dict[str, Any] | list[Any]
