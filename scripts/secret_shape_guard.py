#!/usr/bin/env python3
"""Reject CLI examples that look like real credentials to secret scanners."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

TOKEN_SHAPED_LITERALS = [
    "s3" + "cr3t",
    "secret" + "-token",
    "example" + "-token",
    "fake" + "-token",
]

FORBIDDEN = [
    (
        "concrete --principal credential example",
        re.compile(
            r"(?<![\w-])--principal\s+"
            r"(?!<principal-spec>|NAME:TOKEN:ROLES\b)"
            r"[A-Za-z0-9_.-]+:[^:\s`'\"\\]+:[A-Za-z0-9_,.-]+"
        ),
    ),
    (
        "shell token variable used as a concrete --token value",
        re.compile(r"(?<![\w-])--token\s+\$\{?N4CLUSTER_TOKEN\}?"),
    ),
    (
        "concrete --token literal example",
        re.compile(
            r"(?<![\w-])--token\s+"
            r"(?!<[^>\s]+>)"
            r"(?![A-Z_][A-Z0-9_]*\b)"
            r"(?!\$\{?[A-Z_][A-Z0-9_]*\}?)"
            r"[A-Za-z0-9][A-Za-z0-9_.:=/@+-]{11,}"
        ),
    ),
    (
        "literal N4CLUSTER_TOKEN assignment",
        re.compile(
            r"\bN4CLUSTER_TOKEN\s*=\s*[\"']?"
            r"(?!<[^>\s]+>)"
            r"(?![A-Z_][A-Z0-9_]*\b)"
            r"(?!\$\{?[A-Z_][A-Z0-9_]*\}?)"
            r"[A-Za-z0-9][A-Za-z0-9_.:=/@+-]{11,}"
        ),
    ),
    (
        "token-shaped example literal",
        re.compile(r"\b(?:" + "|".join(re.escape(value) for value in TOKEN_SHAPED_LITERALS) + r")\b", re.IGNORECASE),
    ),
]


def _tracked_files() -> list[Path]:
    proc = subprocess.run(["git", "ls-files", "-z"], check=True, capture_output=True)
    return [Path(name.decode()) for name in proc.stdout.split(b"\0") if name]


def main() -> int:
    findings: list[str] = []
    for path in _tracked_files():
        try:
            raw = path.read_bytes()
        except OSError as exc:
            findings.append(f"{path}: could not read file: {exc}")
            continue
        if b"\0" in raw:
            continue
        text = raw.decode("utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), start=1):
            for label, pattern in FORBIDDEN:
                if pattern.search(line):
                    findings.append(f"{path}:{lineno}: {label}")

    if findings:
        print("Token-shaped CLI examples are forbidden; use placeholders such as <principal-spec>.", file=sys.stderr)
        for finding in findings:
            print(finding, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
