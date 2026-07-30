"""CLI: `python -m bench run` and `python -m bench report`."""

import argparse
import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bench.adapters.base import BenchAdapter
from bench.adapters.buses import BUS_ADAPTERS
from bench.adapters.e2e import make_e2e_adapters
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS
from bench.adapters.stores import STORE_ADAPTERS
from bench.core.report import render_markdown
from bench.core.results import RunResult
from bench.core.runner import RunnerConfig, run_matrix
from bench.scenarios import all_scenarios

DEFAULT_OUT = Path(__file__).parent / "results"


def _build_adapters(backends: list[str] | None) -> dict[str, list[BenchAdapter[Any]]]:
    def wanted(name: str) -> bool:
        return backends is None or name in backends

    return {
        "store": [cls() for name, cls in STORE_ADAPTERS.items() if wanted(name)],
        "bus": [cls() for name, cls in BUS_ADAPTERS.items() if wanted(name)],
        "snapshot": [cls() for name, cls in SNAPSHOT_ADAPTERS.items() if wanted(name)],
        "e2e": [a for a in make_e2e_adapters() if wanted(a.name)],
    }


def _cmd_run(args: argparse.Namespace) -> int:
    scenarios = all_scenarios()
    known_backends = set(STORE_ADAPTERS) | set(BUS_ADAPTERS) | set(SNAPSHOT_ADAPTERS)
    if args.backend:
        unknown = set(args.backend) - known_backends
        if unknown:
            print(f"unknown backend(s): {', '.join(sorted(unknown))}", file=sys.stderr)
            return 2
    if args.scenario:
        known = {s.name for s in scenarios}
        unknown = set(args.scenario) - known
        if unknown:
            print(f"unknown scenario(s): {', '.join(sorted(unknown))}", file=sys.stderr)
            return 2
        scenarios = [s for s in scenarios if s.name in set(args.scenario)]
    if args.interface:
        scenarios = [s for s in scenarios if s.interface in set(args.interface)]

    adapters = _build_adapters(args.backend or None)
    config = RunnerConfig(quick=args.quick)
    result = asyncio.run(run_matrix(scenarios, adapters, config))

    for cell in result.cells:
        print(f"{cell.status:>7}  {cell.cell_id}" + (f"  ({cell.reason})" if cell.reason else ""))

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    out_path = out_dir / f"bench-{stamp}.json"
    result.save(out_path)
    print(f"\nresults written to {out_path}")

    return 1 if any(c.status == "failed" for c in result.cells) else 0


def _cmd_report(args: argparse.Namespace) -> int:
    for path in args.files:
        run = RunResult.from_json(Path(path).read_text())
        text = render_markdown(run)
        if args.out:
            Path(args.out).write_text(text)
            print(f"report written to {args.out}")
        else:
            print(text)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bench", description="eventsource backend benchmarks")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="run the benchmark matrix")
    run_parser.add_argument(
        "--interface", action="append", choices=["store", "bus", "snapshot", "e2e"]
    )
    run_parser.add_argument("--backend", action="append")
    run_parser.add_argument("--scenario", action="append")
    run_parser.add_argument("--quick", action="store_true")
    run_parser.add_argument("--out", default=str(DEFAULT_OUT))

    report_parser = sub.add_parser("report", help="render results as markdown")
    report_parser.add_argument("files", nargs="+")
    report_parser.add_argument("--out", default=None)

    args = parser.parse_args(argv)
    if args.command == "run":
        return _cmd_run(args)
    return _cmd_report(args)


if __name__ == "__main__":
    raise SystemExit(main())
