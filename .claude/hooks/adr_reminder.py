#!/usr/bin/env python3
"""PostToolUse hook: remind to verify ADR refs after editing integration/datamart SQL.

Emits a JSON payload with `systemMessage` so the message surfaces in the UI.
Always exits 0 — this is a reminder, not a blocker.
"""
import json
import sys


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    file_path = payload.get("tool_input", {}).get("file_path", "").replace("\\", "/")
    if not file_path.endswith(".sql"):
        sys.exit(0)

    parts = file_path.split("/")
    if "integration" not in parts and "datamart" not in parts:
        sys.exit(0)

    name = parts[-1]
    msg = (
        f"Touched DLT SQL ({name}) — confirm the ADR references in the file "
        "header are still accurate. Consider running the dlt-adr-reviewer "
        "subagent before commit."
    )
    print(json.dumps({"systemMessage": msg}))
    sys.exit(0)


if __name__ == "__main__":
    main()
