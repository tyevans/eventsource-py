#!/usr/bin/env python3
"""
Example Validation Script

Validates that code examples in the examples/ directory:
1. Have valid Python syntax (compile successfully)
2. Run without errors (execute successfully)

Also optionally validates Python code blocks in Markdown documentation
for syntax correctness.

Usage:
    python scripts/validate_examples.py              # Validate and run examples
    python scripts/validate_examples.py --syntax     # Syntax check only
    python scripts/validate_examples.py --docs       # Also check docs markdown
"""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
from pathlib import Path


class ValidationResult:
    """Holds validation results for a file."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.syntax_ok = False
        self.execution_ok = False
        self.syntax_error: str | None = None
        self.execution_error: str | None = None


def validate_syntax(file_path: Path) -> tuple[bool, str | None]:
    """
    Validate Python syntax by attempting to compile the file.

    Args:
        file_path: Path to the Python file.

    Returns:
        Tuple of (success, error_message).
    """
    try:
        source = file_path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(file_path))
        return True, None
    except SyntaxError as e:
        return False, f"Line {e.lineno}: {e.msg}"


def execute_example(file_path: Path, timeout: int = 60) -> tuple[bool, str | None]:
    """
    Execute an example file and check for errors.

    Args:
        file_path: Path to the Python file.
        timeout: Maximum execution time in seconds.

    Returns:
        Tuple of (success, error_message).
    """
    try:
        result = subprocess.run(
            [sys.executable, str(file_path)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=file_path.parent.parent,  # Run from project root
        )
        if result.returncode != 0:
            error_output = result.stderr.strip() or result.stdout.strip()
            # Get last few lines of error
            error_lines = error_output.split("\n")[-10:]
            return False, "\n".join(error_lines)
        return True, None
    except subprocess.TimeoutExpired:
        return False, f"Execution timed out after {timeout}s"
    except Exception as e:
        return False, str(e)


def extract_python_blocks_from_markdown(file_path: Path) -> list[tuple[int, str]]:
    """
    Extract Python code blocks from a Markdown file.

    Args:
        file_path: Path to the Markdown file.

    Returns:
        List of (line_number, code) tuples.
    """
    content = file_path.read_text(encoding="utf-8")
    # Match ```python or ```py code blocks
    pattern = r"```(?:python|py)\n(.*?)```"
    blocks: list[tuple[int, str]] = []

    for match in re.finditer(pattern, content, re.DOTALL):
        # Calculate line number
        start_pos = match.start()
        line_num = content[:start_pos].count("\n") + 1
        code = match.group(1)
        blocks.append((line_num, code))

    return blocks


def validate_markdown_code_blocks(
    docs_dir: Path,
) -> list[tuple[Path, int, str]]:
    """
    Validate Python code blocks in Markdown files.

    Args:
        docs_dir: Path to the docs directory.

    Returns:
        List of (file_path, line_number, error_message) for failures.
    """
    errors: list[tuple[Path, int, str]] = []

    for md_file in docs_dir.rglob("*.md"):
        blocks = extract_python_blocks_from_markdown(md_file)
        for line_num, code in blocks:
            # Skip blocks that are clearly incomplete snippets
            # (e.g., contain ... or # ... for truncation)
            if "# ..." in code or code.strip().endswith("..."):
                continue

            try:
                ast.parse(code)
            except SyntaxError as e:
                # Some code blocks are intentionally incomplete (showing API usage)
                # Skip those that are obviously partial
                if "..." in code or "@" in code.split("\n")[0]:
                    continue
                errors.append((md_file, line_num, f"Line {line_num + (e.lineno or 0)}: {e.msg}"))

    return errors


def find_example_files(examples_dir: Path) -> list[Path]:
    """
    Find all Python example files to validate.

    Args:
        examples_dir: Path to the examples directory.

    Returns:
        List of Python file paths (excluding __init__.py).
    """
    files = []
    for py_file in examples_dir.glob("*.py"):
        if py_file.name != "__init__.py":
            files.append(py_file)
    return sorted(files)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Validate Python examples in the project.")
    parser.add_argument(
        "--syntax",
        action="store_true",
        help="Only check syntax, don't execute examples",
    )
    parser.add_argument(
        "--docs",
        action="store_true",
        help="Also validate Python code blocks in documentation",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout for example execution in seconds (default: 60)",
    )
    args = parser.parse_args()

    # Determine project root
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    examples_dir = project_root / "examples"
    docs_dir = project_root / "docs"

    print("=" * 60)
    print("Example Validation")
    print("=" * 60)

    # Track results
    results: list[ValidationResult] = []
    all_passed = True

    # Validate examples directory
    if not examples_dir.exists():
        print(f"\nERROR: Examples directory not found: {examples_dir}")
        return 1

    example_files = find_example_files(examples_dir)
    if not example_files:
        print(f"\nWARNING: No example files found in {examples_dir}")
        return 0

    print(f"\nFound {len(example_files)} example file(s) to validate\n")

    # Validate each example
    for file_path in example_files:
        result = ValidationResult(str(file_path.relative_to(project_root)))

        # Syntax check
        print(f"Checking: {result.path}")
        result.syntax_ok, result.syntax_error = validate_syntax(file_path)

        if not result.syntax_ok:
            print(f"  SYNTAX ERROR: {result.syntax_error}")
            all_passed = False
            results.append(result)
            continue

        print("  Syntax: OK")

        # Execution check (if not syntax-only mode)
        if not args.syntax:
            result.execution_ok, result.execution_error = execute_example(
                file_path, timeout=args.timeout
            )

            if result.execution_ok:
                print("  Execution: OK")
            else:
                print(f"  EXECUTION ERROR:\n{result.execution_error}")
                all_passed = False
        else:
            result.execution_ok = True  # Skip execution

        results.append(result)

    # Validate documentation code blocks (optional)
    doc_errors: list[tuple[Path, int, str]] = []
    if args.docs and docs_dir.exists():
        print("\n" + "-" * 60)
        print("Validating documentation code blocks")
        print("-" * 60 + "\n")

        doc_errors = validate_markdown_code_blocks(docs_dir)
        if doc_errors:
            all_passed = False
            for file_path, line_num, error in doc_errors:
                rel_path = file_path.relative_to(project_root)
                print(f"SYNTAX ERROR in {rel_path}:{line_num}")
                print(f"  {error}\n")
        else:
            print("All documentation code blocks have valid syntax.\n")

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    syntax_passed = sum(1 for r in results if r.syntax_ok)
    exec_passed = sum(1 for r in results if r.execution_ok)
    total = len(results)

    print("\nExamples:")
    print(f"  Syntax:    {syntax_passed}/{total} passed")
    if not args.syntax:
        print(f"  Execution: {exec_passed}/{total} passed")

    if args.docs and docs_dir.exists():
        print("\nDocumentation:")
        print(f"  Code blocks with errors: {len(doc_errors)}")

    if all_passed:
        print("\n[PASS] All validations passed!")
        return 0
    else:
        print("\n[FAIL] Some validations failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
