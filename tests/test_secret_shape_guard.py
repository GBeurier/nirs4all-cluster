from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_guard():
    path = ROOT / "scripts" / "secret_shape_guard.py"
    spec = importlib.util.spec_from_file_location("secret_shape_guard", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_guard(monkeypatch, tmp_path: Path, text: str) -> int:
    guard = _load_guard()
    path = tmp_path / "doc.md"
    path.write_text(text, encoding="utf-8")
    monkeypatch.setattr(guard, "_tracked_files", lambda: [path])
    return guard.main()


def test_secret_shape_guard_allows_placeholders_and_variables(monkeypatch, tmp_path: Path) -> None:
    assert (
        _run_guard(
            monkeypatch,
            tmp_path,
            "\n".join(
                [
                    "n4cluster server --token <auth-token>",
                    "n4cluster server --principal <principal-spec>",
                    "n4cluster server --principal NAME:TOKEN:ROLES",
                    "n4cluster worker --token TOKEN",
                    "N4CLUSTER_TOKEN=<auth-token>",
                    "N4CLUSTER_TOKEN=$TOKEN_FROM_SECRET_MANAGER",
                ]
            ),
        )
        == 0
    )


def test_secret_shape_guard_rejects_literal_cli_token(monkeypatch, tmp_path: Path, capsys) -> None:
    literal = "abcdefgh" + "ijklmnop"
    status = _run_guard(
        monkeypatch,
        tmp_path,
        f"n4cluster server --token {literal} --state ./state\n",
    )

    captured = capsys.readouterr()
    assert status == 1
    assert "concrete --token literal example" in captured.err
    assert literal not in captured.err


def test_secret_shape_guard_rejects_literal_principal(monkeypatch, tmp_path: Path, capsys) -> None:
    literal = "abcdefgh" + "ijklmnop"
    prefix = "n4cluster server --" + "principal "
    suffix = ":viewer\n"
    status = _run_guard(
        monkeypatch,
        tmp_path,
        prefix + "submitter:" + literal + suffix,
    )

    captured = capsys.readouterr()
    assert status == 1
    assert "concrete --principal credential example" in captured.err
    assert literal not in captured.err


def test_secret_shape_guard_rejects_literal_env_token(monkeypatch, tmp_path: Path, capsys) -> None:
    literal = "abcdefgh" + "ijklmnop"
    status = _run_guard(monkeypatch, tmp_path, f'N4CLUSTER_TOKEN="{literal}"\n')

    captured = capsys.readouterr()
    assert status == 1
    assert "literal N4CLUSTER_TOKEN assignment" in captured.err
    assert literal not in captured.err


def test_secret_shape_guard_passes_on_the_tracked_tree() -> None:
    """The live repo must stay free of token-shaped CLI examples (the GitGuardian gate).

    The other tests use synthetic fixtures via ``_tracked_files`` monkeypatching;
    this one runs the guard exactly as the pre-commit hook / CI does — over the
    real ``git ls-files`` tree from the repo root — so the pytest green gate, not
    only CI, fails if a realistic-looking credential ever lands in a tracked file.
    """
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "secret_shape_guard.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
