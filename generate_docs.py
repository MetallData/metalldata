#!/usr/bin/env python3
"""
Generate markdown documentation from clippy executables.

Usage: ./generate_docs.py <class_dir> [class_dir...]

Example: ./generate_docs.py build/src/clippy/MetallGraph build/src/clippy/MetallUtils
"""

import argparse
import json
import os
import subprocess
import sys
from typing import Any


def find_methods(class_dir: str) -> list[str]:
    """Find method executables in a class directory.

    Methods are regular files with no extension, excluding Makefile.
    """
    methods: list[str] = []
    for name in os.listdir(class_dir):
        path: str = os.path.join(class_dir, name)
        if not os.path.isfile(path):
            continue
        if "." in name or name == "Makefile":
            continue
        methods.append(name)
    return sorted(methods)


def get_class_doc(class_dir: str) -> str:
    """Read class-level documentation from meta.json if present."""
    meta_path: str = os.path.join(class_dir, "meta.json")
    if not os.path.isfile(meta_path):
        return ""
    try:
        with open(meta_path) as f:
            meta: dict[str, Any] = json.load(f)
        return meta.get("__doc__", "")
    except (json.JSONDecodeError, OSError):
        return ""


def get_method_doc(exe_path: str) -> dict[str, Any] | None:
    """Run an executable with --clippy-help and return parsed JSON."""
    result = subprocess.run(
        [exe_path, "--clippy-help"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def format_method(data: dict[str, Any]) -> str:
    """Format a method's documentation as markdown."""
    lines: list[str] = []
    name: str = data.get("method_name", "")
    desc: str = data.get("desc", "")
    state: dict[str, Any] = data.get("_state", {})
    args: dict[str, Any] = data.get("args", {})

    lines.append(f"### `{name}`")
    lines.append("")
    if desc:
        lines.append(desc)
        lines.append("")

    # Separate positional args (position >= 0) from keyword args (position == -1)
    positional: list[tuple[int, str, dict[str, Any]]] = []
    keyword: list[tuple[str, dict[str, Any]]] = []
    for aname, ainfo in args.items():
        pos: int = ainfo.get("position", -1)
        if pos >= 0:
            positional.append((pos, aname, ainfo))
        else:
            keyword.append((aname, ainfo))

    positional.sort(key=lambda x: x[0])

    if positional or keyword:
        lines.append("#### Arguments")
        lines.append("")
        lines.append("| Name | Description | Position | Default |")
        lines.append("|------|-------------|----------|---------|")
        for pos, aname, ainfo in positional:
            adesc = ainfo.get("desc", "")
            default_str = (
                f"`{json.dumps(ainfo['default_val'])}`"
                if "default_val" in ainfo
                else "*required*"
            )
            lines.append(f"| `{aname}` | {adesc} | {pos} | {default_str} |")
        for aname, ainfo in sorted(keyword):
            adesc = ainfo.get("desc", "")
            default_str = (
                f"`{json.dumps(ainfo['default_val'])}`"
                if "default_val" in ainfo
                else "*required*"
            )
            lines.append(f"| `{aname}` | {adesc} | keyword | {default_str} |")
        lines.append("")

    if state:
        lines.append("#### State")
        lines.append("")
        lines.append("| Name | Description |")
        lines.append("|------|-------------|")
        for sname, sinfo in sorted(state.items()):
            sdesc = sinfo.get("desc", "") if isinstance(sinfo, dict) else str(sinfo)
            lines.append(f"| `{sname}` | {sdesc} |")
        lines.append("")

    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate markdown documentation from clippy executables."
    )
    parser.add_argument(
        "class_dirs",
        nargs="+",
        metavar="class_dir",
        help="Path to a class directory containing method executables",
    )
    args: argparse.Namespace = parser.parse_args()

    # Sort class directories alphabetically by basename
    sorted_dirs: list[str] = sorted(args.class_dirs, key=lambda d: os.path.basename(d).lower())

    # First pass: collect methods per class
    class_data: list[tuple[str, str, list[str]]] = []
    for class_dir in sorted_dirs:
        class_name: str = os.path.basename(class_dir)
        methods: list[str] = find_methods(class_dir)
        class_data.append((class_name, class_dir, methods))

    # Header
    print("# API Reference")
    print()

    # Table of contents
    print("## Table of Contents")
    print()
    for class_name, _, methods in class_data:
        print(f"- [{class_name}](#{class_name.lower()})")
        for method in methods:
            print(f"  - [`{method}`](#{method})")
    print()

    # Documentation
    for class_name, class_dir, methods in class_data:
        class_doc: str = get_class_doc(class_dir)

        print(f"## {class_name}")
        print()
        if class_doc:
            print(class_doc)
            print()

        for method in methods:
            exe_path: str = os.path.join(class_dir, method)
            try:
                data: dict[str, Any] | None = get_method_doc(exe_path)
            except (json.JSONDecodeError, OSError) as e:
                print(
                    f"warning: {class_name}/{method} --clippy-help failed: {e}",
                    file=sys.stderr,
                )
                continue
            if data is None:
                print(
                    f"warning: {class_name}/{method} --clippy-help failed, skipping",
                    file=sys.stderr,
                )
                continue

            print(format_method(data))


if __name__ == "__main__":
    main()
