# Sequential Stream Payload Delta Compression

Reference documentation for dictionary-based delta compression of sequential
event stream payloads below the snapshot boundary.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0003 (Blackbox Frontdoor Verification)
- TASK-0002 (Delta-Compress Stored Event Payloads)

---

## 1. Overview & Problem Statement

Event-sourced aggregates that record whole document revisions (such as file
contents, rendered artifacts, or state carried whole rather than as explicit
domain diffs) cause storage growth proportional to $\mathcal{O}(\text{size}
\times \text{revisions})$.

`DeltaCodec` provides opt-in, dictionary-based delta compression using RFC 1950
preset dictionaries with multi-tier integrity checks:

- **Per-Frame CRC32**: Detects transmission or disk bit-flips prior to decompressing.
- **SHA-256 Content Hash**: Cryptographically verifies reconstructed payload against the original uncompressed state.
- **Adaptive Re-Baselining**: Automatically forces self-contained baseline frames when:
  1. The delta chain length exceeds `max_chain_length` (default: 32).
  2. The compressed delta size exceeds `ratio_threshold` of the uncompressed payload (default: 80%).
  3. The cumulative size of the delta chain exceeds `cumulative_ratio_threshold` times the fulltext size (default: 2.0x).
  4. The uncompressed payload is below `min_payload_size` (default: 128 bytes).

---

## 2. Architecture & Data Model

`DeltaPayload` wraps the compressed byte frame and integrity metadata into a
JSON-compatible structure that can be stored transparently inside existing
`JSONB`, `TEXT`, or `JSON` columns:

```json
{
  "__delta__": true,
  "v": 1,
  "is_delta": true,
  "base_version": 1,
  "data": "<base64-encoded compressed frame>",
  "hash": "<sha256-content-hash>",
  "crc": 4109098394,
  "raw_size": 15437,
  "comp_size": 100
}
```

When an event payload is read, the storage adapter or application deserializer
detects `__delta__` via `is_delta_dict()` and reconstructs the dictionary
frame. If regular uncompressed dictionaries or primitives are stored, they are
left untouched.

---

## 3. Usage Example

```python
from eventsource.adapters.serialization import DeltaCodec, DeltaPayload

codec = DeltaCodec(max_chain_length=32, min_payload_size=128)

# 1. First event revision (stored as a standalone baseline)
v1_doc = {"document_id": "doc-001", "version": 1, "content": "Initial draft of long text..." * 50}
p1 = codec.compress(v1_doc)
assert p1.is_delta is False

# 2. Second event revision (stored as a delta against v1)
v2_doc = {"document_id": "doc-001", "version": 2, "content": "Initial draft of long text (with an edit)..." * 50}
p2 = codec.compress(v2_doc, base_payload=v1_doc, base_version=1)
assert p2.is_delta is True

# 3. Transparent decompression
restored_v1 = codec.decompress_to_dict(p1)
restored_v2 = codec.decompress_to_dict(p2, base_payload=restored_v1)
assert restored_v2 == v2_doc
```

---

## 4. Benchmark Results

Measured across 30 sequential document revisions with small text edits:
- **Uncompressed Total**: ~600 KB
- **Compressed Total**: ~15 KB
- **Compression Ratio**: >35x reduction compared to verbatim payloads
- **Reconstruction Overhead**: <2ms worst-case reconstruction time per frame
