"""Render a RunResult as a Markdown report.

Rows are parameter combinations, columns are backends, so scaling curves
read down a column and backend comparison reads across a row.
"""

from collections import defaultdict
from typing import Any

from bench.core.results import CellResult, RunResult

_INTERFACE_ORDER = ["store", "bus", "snapshot", "e2e"]


def _params_key(cell: CellResult) -> str:
    if not cell.params:
        return "(default)"
    return ", ".join(f"{k}={cell.params[k]}" for k in sorted(cell.params))


def _backend_order(backends: set[str]) -> list[str]:
    ordered = sorted(backends)
    if "memory" in ordered:
        ordered.remove("memory")
        ordered.insert(0, "memory")
    return ordered


def _format_cell(cell: CellResult) -> str:
    if cell.status != "ok":
        return f"{cell.status}: {cell.reason}"
    median = cell.median_round
    if median is None:
        return "no data"
    if cell.metric == "latency" and median.latency is not None:
        text = (
            f"{median.latency.p50_ms:.2f}ms "
            f"(p95 {median.latency.p95_ms:.2f}ms, p99 {median.latency.p99_ms:.2f}ms)"
        )
    else:
        text = f"{median.ops_per_sec:,.0f}/s"
    conflicts = median.counters.get("conflicts", 0)
    if conflicts:
        rate = conflicts / (median.operations + conflicts)
        text += f" [{rate:.0%} conflicts]"
    return text


def _render_metadata(metadata: dict[str, Any]) -> list[str]:
    lines = ["# Benchmark Report", ""]
    for key in sorted(metadata):
        lines.append(f"- **{key}**: {metadata[key]}")
    lines.append("")
    return lines


def render_markdown(run: RunResult) -> str:
    lines = _render_metadata(run.metadata)
    by_interface: dict[str, dict[str, list[CellResult]]] = defaultdict(lambda: defaultdict(list))
    for cell in run.cells:
        by_interface[cell.interface][cell.scenario].append(cell)

    for interface in _INTERFACE_ORDER:
        if interface not in by_interface:
            continue
        lines.append(f"## {interface}")
        lines.append("")
        for scenario_name in sorted(by_interface[interface]):
            cells = by_interface[interface][scenario_name]
            lines.append(f"### {scenario_name}")
            lines.append("")
            backends = _backend_order({c.backend for c in cells})
            lines.append("| params | " + " | ".join(backends) + " |")
            lines.append("|" + "---|" * (len(backends) + 1))
            by_row: dict[str, dict[str, CellResult]] = defaultdict(dict)
            row_order: list[str] = []
            for cell in cells:
                key = _params_key(cell)
                if key not in by_row:
                    row_order.append(key)
                by_row[key][cell.backend] = cell
            for key in row_order:
                row_cells = [
                    _format_cell(by_row[key][b]) if b in by_row[key] else "—" for b in backends
                ]
                lines.append(f"| {key} | " + " | ".join(row_cells) + " |")
            lines.append("")
    return "\n".join(lines)
