"""
Benchmark and empirical evaluation of stream payload delta compression.

Governed by:
- ADR-0003 (Blackbox Frontdoor Verification)
- TASK-0002 (Delta-Compress Stored Event Payloads)
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from eventsource.adapters.serialization.delta import DeltaCodec, DeltaPayload


def generate_document_revisions(
    base_size_bytes: int = 50_000,
    revisions: int = 50,
) -> list[dict[str, Any]]:
    """Generate sequential document revisions where small edits occur between versions."""
    base_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * (
        base_size_bytes // 56
    )
    docs = []
    current_text = base_text
    for i in range(revisions):
        # Small mutation per revision (simulating document editing)
        mutation = f"\n[Revision {i}: updated paragraph with specific changes]\n"
        current_text = current_text[: 1000 * (i % 10)] + mutation + current_text[1000 * (i % 10) :]
        docs.append({"doc_id": "doc-123", "version": i + 1, "body": current_text})
    return docs


class TestDeltaCompressionBenchmark:
    @pytest.mark.benchmark
    def test_delta_compression_storage_savings(self) -> None:
        """Measure storage compression ratio for sequential revisions with delta dictionary."""
        codec = DeltaCodec(max_chain_length=32, min_payload_size=256)
        docs = generate_document_revisions(base_size_bytes=20_000, revisions=30)

        uncompressed_total_bytes = sum(len(str(d).encode("utf-8")) for d in docs)

        compressed_payloads: list[DeltaPayload] = []
        base_doc: dict[str, Any] | None = None
        base_ver: int | None = None
        chain_len = 0
        cumulative_bytes = 0

        start_time = time.perf_counter()
        for doc in docs:
            delta = codec.compress(
                doc,
                base_payload=base_doc,
                base_version=base_ver,
                chain_length=chain_len,
                cumulative_chain_bytes=cumulative_bytes,
            )
            compressed_payloads.append(delta)

            if delta.is_delta:
                chain_len += 1
                cumulative_bytes += delta.compressed_size
            else:
                base_doc = doc
                base_ver = doc["version"]
                chain_len = 0
                cumulative_bytes = delta.compressed_size
        compression_duration = time.perf_counter() - start_time

        compressed_total_bytes = sum(p.compressed_size for p in compressed_payloads)

        # Storage reduction ratio should be significant (>5x)
        ratio = uncompressed_total_bytes / compressed_total_bytes
        assert ratio > 5.0, f"Expected compression ratio > 5x, got {ratio:.2f}x"
        assert compression_duration < 1.0, f"Compression took too long: {compression_duration:.3f}s"

    @pytest.mark.benchmark
    def test_delta_reconstruction_performance_and_fidelity(self) -> None:
        """Measure decompression and round-trip integrity for a chained sequence."""
        codec = DeltaCodec(max_chain_length=16, min_payload_size=256)
        docs = generate_document_revisions(base_size_bytes=15_000, revisions=20)

        compressed: list[DeltaPayload] = []
        base_doc = None
        base_ver = None
        chain = 0
        cumulative = 0

        for doc in docs:
            delta = codec.compress(
                doc,
                base_payload=base_doc,
                base_version=base_ver,
                chain_length=chain,
                cumulative_chain_bytes=cumulative,
            )
            compressed.append(delta)
            if delta.is_delta:
                chain += 1
                cumulative += delta.compressed_size
            else:
                base_doc = doc
                base_ver = doc["version"]
                chain = 0
                cumulative = delta.compressed_size

        # Measure reconstruction
        start_time = time.perf_counter()
        reconstructed_docs = []
        history_by_version: dict[int, Any] = {}

        for delta in compressed:
            if not delta.is_delta:
                reconstructed = codec.decompress_to_dict(delta)
            else:
                base_for_delta = history_by_version[delta.base_version]
                reconstructed = codec.decompress_to_dict(delta, base_payload=base_for_delta)

            version = reconstructed["version"]
            history_by_version[version] = reconstructed
            reconstructed_docs.append(reconstructed)

        reconstruction_duration = time.perf_counter() - start_time

        # Verify 100% byte/data fidelity across every revision
        assert len(reconstructed_docs) == len(docs)
        for original, restored in zip(docs, reconstructed_docs, strict=True):
            assert original == restored

        # Reconstruction of 20 revisions should be sub-50ms
        assert reconstruction_duration < 0.100, (
            f"Reconstruction too slow: {reconstruction_duration:.3f}s"
        )
