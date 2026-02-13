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


def find_methods(class_dir):
    """Find method executables in a class directory.

    Methods are regular files with no extension, excluding Makefile.
    """
    methods = []
    for name in os.listdir(class_dir):
        path = os.path.join(class_dir, name)
        if not os.path.isfile(path):
            continue
        if "." in name or name == "Makefile":
            continue
        methods.append(name)
    return sorted(methods)


def get_class_doc(class_dir):
    """Read class-level documentation from meta.json if present."""
    meta_path = os.path.join(class_dir, "meta.json")
    if not os.path.isfile(meta_path):
        return ""
    try:
        with open(meta_path) as f:
            meta = json.load(f)
        return meta.get("__doc__", "")
    except (json.JSONDecodeError, OSError):
        return ""


def get_method_doc(exe_path):
    """Run an executable with --clippy-help and return parsed JSON."""
    result = subprocess.run(
        [exe_path, "--clippy-help"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def format_method(data):
    """Format a method's documentation as markdown."""
    lines = []
    name = data.get("method_name", "")
    desc = data.get("desc", "")
    state = data.get("_state", {})
    args = data.get("args", {})

    lines.append(f"### `{name}`")
    lines.append("")
    if desc:
        lines.append(desc)
        lines.append("")

    # Separate positional args (position >= 0) from keyword args (position == -1)
    positional = []
    keyword = []
    for aname, ainfo in args.items():
        pos = ainfo.get("position", -1)
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


def main():
    parser = argparse.ArgumentParser(
        description="Generate markdown documentation from clippy executables."
    )
    parser.add_argument(
        "class_dirs",
        nargs="+",
        metavar="class_dir",
        help="Path to a class directory containing method executables",
    )
    args = parser.parse_args()

    # Sort class directories alphabetically by basename
    sorted_dirs = sorted(args.class_dirs, key=lambda d: os.path.basename(d).lower())

    # First pass: collect methods per class
    class_data = []
    for class_dir in sorted_dirs:
        class_name = os.path.basename(class_dir)
        methods = find_methods(class_dir)
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
        class_doc = get_class_doc(class_dir)

        print(f"## {class_name}")
        print()
        if class_doc:
            print(class_doc)
            print()

        for method in methods:
            exe_path = os.path.join(class_dir, method)
            try:
                data = get_method_doc(exe_path)
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
