#!/usr/bin/env python3
"""Fail when the ADR listings disagree with the ``docs/adrs/`` directory.

``docs/adrs/index.md`` and the ``ADRs:`` section of ``mkdocs.yml`` are both
second copies of a fact the directory listing already holds. Nothing failed
when the copies disagreed, so they drifted -- five separate ``fix:`` commits
have hand-patched ``index.md`` alone. This script is the thing that fails.

It is deliberately *not* a regenerator. ``index.md`` is prose: each ADR gets a
hand-written paragraph explaining what the record decides, grouped by subject,
and the mkdocs nav titles are curated short forms (the ADR H1s themselves are
inconsistently formatted -- ``ADR-0001:``, ``0007 -``, ``8.``, ``ADR 0014:``).
Regenerating either from the directory would destroy the part a human wrote.
What is mechanically derivable is *coverage and identity*: which ADRs exist,
that each is listed exactly once in each place, that nothing is listed which
does not exist, that the nav is in numeric order, and that the number in each
listing entry matches the file it points at. Those are the failures that
actually occurred.

Run ``--emit`` to print the derived list (numbers and H1 titles) as a starting
point for the entry you have to write by hand.

Stdlib only, so CI can run it with a bare ``python3`` and no sync step.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ADR_FILENAME = re.compile(r"^(\d{4})-[a-z0-9-]+\.md$")
INDEX_LINK = re.compile(r"\]\((\d{4}-[a-z0-9-]+\.md)[^)]*\)")
NAV_ENTRY = re.compile(r"^\s*-\s*(?:\"(?P<title>[^\"]+)\":\s*)?adrs/(?P<file>\S+\.md)\s*$")
H1 = re.compile(r"^#\s+(?P<title>.+?)\s*$", re.MULTILINE)
H1_NUMBER = re.compile(r"^(?:ADR[\s-]*)?(?P<num>\d{1,4})\b")
TITLE_NUMBER = re.compile(r"(?:ADR[\s-]*)?(?P<num>\d{1,4})\b")


def adr_h1(path: Path) -> str:
    match = H1.search(path.read_text(encoding="utf-8"))
    if match is None:
        return ""
    return match.group("title")


def annotate(message: str, *, file: str | None = None) -> None:
    """Print a GitHub Actions error annotation (plain text off CI)."""
    location = f" file={file}::" if file else "::"
    print(f"::error{location}{message}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument(
        "--emit",
        action="store_true",
        help="Print the derived ADR list (number, filename, H1 title) and exit 0.",
    )
    args = parser.parse_args()

    adr_dir = args.root / "docs" / "adrs"
    index_path = adr_dir / "index.md"
    mkdocs_path = args.root / "mkdocs.yml"

    files: dict[str, int] = {}
    for path in sorted(adr_dir.glob("*.md")):
        match = ADR_FILENAME.match(path.name)
        if match:
            files[path.name] = int(match.group(1))
    ordered = list(files)

    if args.emit:
        for name in ordered:
            print(f"{files[name]:04d}\t{name}\t{adr_h1(adr_dir / name)}")
        return 0

    if not files:
        annotate(f"No ADR files found under {adr_dir}; the check is misconfigured.")
        return 1

    status = 0

    # --- the H1 is the title source, so it has to name the right ADR --------
    for name in ordered:
        title = adr_h1(adr_dir / name)
        if not title:
            annotate(f"{name} has no H1 heading to take a title from.", file=f"docs/adrs/{name}")
            status = 1
            continue
        number = H1_NUMBER.match(title)
        if number is None or int(number.group("num")) != files[name]:
            annotate(
                f"{name}: H1 {title!r} does not lead with ADR number {files[name]:04d}.",
                file=f"docs/adrs/{name}",
            )
            status = 1

    # --- index.md must link every ADR, and only real ones ------------------
    index_text = index_path.read_text(encoding="utf-8")
    linked = INDEX_LINK.findall(index_text)
    linked_set = set(linked)
    for name in ordered:
        if name not in linked_set:
            annotate(
                f"docs/adrs/index.md does not link {name} "
                f"(H1: {adr_h1(adr_dir / name)!r}). Add an entry describing the decision.",
                file="docs/adrs/index.md",
            )
            status = 1
    for name in sorted(linked_set - set(files)):
        annotate(
            f"docs/adrs/index.md links {name}, which does not exist.",
            file="docs/adrs/index.md",
        )
        status = 1

    # --- mkdocs nav must list every ADR exactly once, in order -------------
    nav_files: list[str] = []
    nav_titles: dict[str, str] = {}
    for line in mkdocs_path.read_text(encoding="utf-8").splitlines():
        match = NAV_ENTRY.match(line)
        if match is None:
            continue
        name = match.group("file")
        if name == "index.md":
            continue
        nav_files.append(name)
        if match.group("title"):
            nav_titles[name] = match.group("title")

    for name in ordered:
        count = nav_files.count(name)
        if count == 0:
            annotate(
                f"mkdocs.yml nav is missing adrs/{name} "
                f"(H1: {adr_h1(adr_dir / name)!r}). A strict build will not catch this.",
                file="mkdocs.yml",
            )
            status = 1
        elif count > 1:
            annotate(f"mkdocs.yml nav lists adrs/{name} {count} times.", file="mkdocs.yml")
            status = 1
    for name in sorted(set(nav_files) - set(files)):
        annotate(f"mkdocs.yml nav lists adrs/{name}, which does not exist.", file="mkdocs.yml")
        status = 1

    present = [name for name in nav_files if name in files]
    if present != [name for name in ordered if name in present]:
        annotate(
            "mkdocs.yml nav does not list the ADRs in ascending number order.", file="mkdocs.yml"
        )
        status = 1

    # The nav title is curated, but it must name the ADR it points at.
    for name, title in nav_titles.items():
        if name not in files:
            continue
        number = TITLE_NUMBER.search(title)
        if number is None or int(number.group("num")) != files[name]:
            annotate(
                f"mkdocs.yml nav title {title!r} does not match the number of {name}.",
                file="mkdocs.yml",
            )
            status = 1

    if status == 0:
        print(f"docs/adrs/index.md and the mkdocs nav both cover all {len(files)} ADRs.")
    else:
        print(
            "\nADR listings disagree with docs/adrs/. "
            "Run `python scripts/check_adr_index.py --emit` for the derived list.",
            file=sys.stderr,
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
