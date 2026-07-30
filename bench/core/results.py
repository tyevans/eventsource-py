"""Result data model for benchmark runs.

The JSON schema is versioned via SCHEMA_VERSION so future regression
tooling can diff two run files (spec: Reporting section).
"""

import json
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        raise ValueError("no samples")
    k = (len(sorted_values) - 1) * pct / 100.0
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


@dataclass(frozen=True)
class LatencyStats:
    p50_ms: float
    p95_ms: float
    p99_ms: float
    mean_ms: float
    min_ms: float

    @classmethod
    def from_durations(cls, durations_s: list[float]) -> "LatencyStats":
        ms = sorted(d * 1000.0 for d in durations_s)
        return cls(
            p50_ms=_percentile(ms, 50),
            p95_ms=_percentile(ms, 95),
            p99_ms=_percentile(ms, 99),
            mean_ms=statistics.fmean(ms),
            min_ms=ms[0],
        )


@dataclass(frozen=True)
class Round:
    elapsed_s: float
    operations: int
    ops_per_sec: float
    latency: LatencyStats | None = None
    counters: dict[str, int] = field(default_factory=dict)


@dataclass
class CellResult:
    scenario: str
    interface: str
    backend: str
    metric: str  # "latency" | "throughput"
    params: dict[str, Any]
    status: str  # "ok" | "skipped" | "failed"
    reason: str | None = None
    rounds: list[Round] = field(default_factory=list)

    @property
    def cell_id(self) -> str:
        rendered = ",".join(f"{key}={self.params[key]}" for key in sorted(self.params))
        return f"{self.scenario}[{self.backend}]({rendered})"

    @property
    def median_round(self) -> Round | None:
        if not self.rounds:
            return None
        ordered = sorted(self.rounds, key=lambda r: r.ops_per_sec)
        return ordered[len(ordered) // 2]


@dataclass
class RunResult:
    schema_version: int
    metadata: dict[str, Any]
    cells: list[CellResult]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    @classmethod
    def from_json(cls, text: str) -> "RunResult":
        raw = json.loads(text)
        cells = []
        for cell_raw in raw["cells"]:
            rounds = []
            for round_raw in cell_raw["rounds"]:
                latency_raw = round_raw.get("latency")
                latency = LatencyStats(**latency_raw) if latency_raw else None
                rounds.append(
                    Round(
                        elapsed_s=round_raw["elapsed_s"],
                        operations=round_raw["operations"],
                        ops_per_sec=round_raw["ops_per_sec"],
                        latency=latency,
                        counters=round_raw.get("counters", {}),
                    )
                )
            cells.append(
                CellResult(
                    scenario=cell_raw["scenario"],
                    interface=cell_raw["interface"],
                    backend=cell_raw["backend"],
                    metric=cell_raw["metric"],
                    params=cell_raw["params"],
                    status=cell_raw["status"],
                    reason=cell_raw.get("reason"),
                    rounds=rounds,
                )
            )
        return cls(
            schema_version=raw["schema_version"],
            metadata=raw["metadata"],
            cells=cells,
        )

    def save(self, path: Path) -> None:
        path.write_text(self.to_json())
