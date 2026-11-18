"""Bounded inflate for delta frames: output is capped at the frame's declared `raw_size`."""

from __future__ import annotations

import zlib

from eventsource.adapters.serialization.delta_types import DeltaIntegrityError, DeltaPayload


def inflate_bounded(delta: DeltaPayload, zdict: bytes | None) -> bytes:
    """Inflate a frame, never producing more than `delta.raw_size` bytes.

    Output is requested with `max_length`, so the decompressor stops at
    the bound. Any input left after the stream ends, or any output past
    the bound, is an integrity failure. Corrupt zlib data raises
    `DeltaIntegrityError` with the `zlib.error` as its cause.
    """
    decompressor = zlib.decompressobj(zdict=zdict) if zdict is not None else zlib.decompressobj()
    limit, out, data = delta.raw_size, bytearray(), delta.compressed_bytes
    try:
        while not decompressor.eof:
            chunk = decompressor.decompress(data, max_length=limit + 1 - len(out))
            out.extend(chunk)
            if len(out) > limit:
                raise DeltaIntegrityError(f"Decompressed payload exceeds raw_size={limit} bytes")
            if not chunk and not decompressor.unconsumed_tail:
                break
            data = decompressor.unconsumed_tail
    except zlib.error as e:
        raise DeltaIntegrityError(f"Compressed frame is corrupt: {e}") from e
    if not decompressor.eof:
        raise DeltaIntegrityError("Compressed frame is truncated: stream did not end")
    if decompressor.unconsumed_tail or decompressor.unused_data:
        raise DeltaIntegrityError("Compressed frame has trailing data past the stream end")
    if len(out) != limit:
        raise DeltaIntegrityError(f"Decompressed size {len(out)} does not match raw_size={limit}")
    return bytes(out)


__all__ = ["inflate_bounded"]
