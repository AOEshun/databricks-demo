#!/usr/bin/env python3
"""PostToolUse hook: validate .ipynb files after Write/Edit.

Reads hook input JSON on stdin. If the touched file ends in .ipynb, parses it
as UTF-8 JSON and scans for common mojibake byte sequences. Exits 2 with a
diagnostic on stderr if validation fails so the model is fed back the error.
"""
import json
import sys
from pathlib import Path


MOJIBAKE_MARKERS = {
    b"\xc3\xa2\xe2\x82\xac\xe2\x80\x9d": "em-dash mojibake (U+2014 double-decoded)",
    b"\xc3\xa2\xe2\x82\xac\xe2\x80\x9c": "en-dash mojibake (U+2013 double-decoded)",
    b"\xc3\xa2\xe2\x82\xac\xc5\x93": "left-double-quote mojibake (U+201C)",
    b"\xc3\xa2\xe2\x82\xac\xc2\x9d": "right-double-quote mojibake (U+201D)",
}


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    file_path = payload.get("tool_input", {}).get("file_path", "")
    if not file_path.endswith(".ipynb"):
        sys.exit(0)

    p = Path(file_path)
    if not p.exists():
        sys.exit(0)

    try:
        raw = p.read_bytes()
    except OSError as e:
        print(f"validate_ipynb: cannot read {p}: {e}", file=sys.stderr)
        sys.exit(2)

    for marker, label in MOJIBAKE_MARKERS.items():
        if marker in raw:
            print(
                f"validate_ipynb: {p} contains {label} — encoding got double-decoded.",
                file=sys.stderr,
            )
            sys.exit(2)

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        print(f"validate_ipynb: {p} is not valid UTF-8: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        nb = json.loads(text)
    except json.JSONDecodeError as e:
        print(
            f"validate_ipynb: {p} is not valid JSON (line {e.lineno} col {e.colno}): {e.msg}",
            file=sys.stderr,
        )
        sys.exit(2)

    if "cells" not in nb:
        print(
            f"validate_ipynb: {p} parsed as JSON but has no 'cells' key — not a notebook.",
            file=sys.stderr,
        )
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
