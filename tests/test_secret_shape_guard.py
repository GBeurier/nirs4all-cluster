from __future__ import annotations

import importlib.util
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
                    "n4cluster worker --token TOKEN",
                    "N4CLUSTER_TOKEN=<auth-token>",
                    "N4CLUSTER_TOKEN=$TOKEN_FROM_SECRET_MANAGER",
                ]
            ),
        )
        == 0
    )


def test_secret_shape_guard_rejects_literal_cli_token(monkeypatch, tmp_path: Path, capsys) -> None:
    status = _run_guard(
        monkeypatch,
        tmp_path,
        "n4cluster server --token abcdefghijklmnop --state ./state\n",
    )

    captured = capsys.readouterr()
    assert status == 1
    assert "concrete --token literal example" in captured.err
    assert "abcdefghijklmnop" not in captured.err


def test_secret_shape_guard_rejects_literal_env_token(monkeypatch, tmp_path: Path, capsys) -> None:
    status = _run_guard(monkeypatch, tmp_path, 'N4CLUSTER_TOKEN="abcdefghijklmnop"\n')

    captured = capsys.readouterr()
    assert status == 1
    assert "literal N4CLUSTER_TOKEN assignment" in captured.err
    assert "abcdefghijklmnop" not in captured.err
