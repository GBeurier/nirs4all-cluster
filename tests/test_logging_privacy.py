"""Owned daemon logging policy, including explicit third-party DEBUG loggers."""

import json
import logging
import subprocess
import sys

import pytest

from nirs4all_cluster.cli import build_parser, cmd_server
from nirs4all_cluster.logging_setup import LOG_LEVELS, configure_logging


@pytest.mark.parametrize("level", LOG_LEVELS)
def test_owned_namespace_and_handler_levels(tmp_path, level):
    script = """import json,logging,sys
from nirs4all_cluster.logging_setup import configure_logging
configure_logging(sys.argv[1],sys.argv[2])
for name in ['nirs4all_cluster.server','nirs4all_cluster.worker','nirs4all_cluster.worker.executor']:
 lg=logging.getLogger(name);lg.setLevel(logging.DEBUG)
 for n in [logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR]:lg.log(n,'application-diagnostic-%s',n)
lg=logging.getLogger('nirs4all_cluster');lg.critical('critical-application-diagnostic')
for name in ['uvicorn.error','uvicorn.access','httpx','httpcore','websockets.server','unknown.transport','nirs4all_clusterish']:
 lg=logging.getLogger(name);lg.setLevel(logging.DEBUG)
 for n in [logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR]:lg.log(n,'third-party-diagnostic-%s',n)
print(json.dumps({'root':logging.getLogger().level,'application':logging.getLogger('nirs4all_cluster').level}))
"""
    path = tmp_path / "app.log"
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, level, str(path)], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0
    numeric = getattr(logging, level.upper())
    assert json.loads(result.stdout) == {"root": max(logging.WARNING, numeric), "application": numeric}
    assert path.read_text() == result.stderr
    text = result.stderr
    assert (
        text.count("application-diagnostic-")
        == {"critical": 0, "error": 3, "warning": 6, "info": 9, "debug": 12}[level]
    )
    assert "critical-application-diagnostic" in text
    assert "third-party-diagnostic-10" not in text and "third-party-diagnostic-20" not in text
    assert text.count("third-party-diagnostic-30") == (7 if numeric <= logging.WARNING else 0)
    assert text.count("third-party-diagnostic-40") == (7 if numeric <= logging.ERROR else 0)


@pytest.mark.parametrize("bad", ["trace", "unknown", "INFO", "", None])
def test_invalid_logging_raises_before_opening_file(tmp_path, bad):
    path = tmp_path / "never-created"
    with pytest.raises(ValueError, match="invalid application log level"):
        configure_logging(bad, str(path))
    assert not path.exists()


@pytest.mark.parametrize("command", ["server", "worker"])
@pytest.mark.parametrize(
    "flag", ["--log-level=trace", "--log-level=unknown", "--access-log", "--uvicorn-log-level=debug", "--unsafe"]
)
def test_no_transport_verbosity_bypass(command, flag, capsys):
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args([command, flag])
    assert exc.value.code == 2 and "invalid command arguments" in capsys.readouterr().err


@pytest.mark.parametrize("level", LOG_LEVELS)
def test_server_exact_fixed_uvicorn_profile(monkeypatch, level):
    from nirs4all_cluster import logging_setup
    from nirs4all_cluster.server import app as app_module

    events = []
    app = object()
    monkeypatch.setattr(app_module, "create_app", lambda config: app)
    monkeypatch.setattr(logging_setup, "configure_logging", lambda *a: events.append(("logging", a)))
    monkeypatch.setattr("uvicorn.run", lambda *a, **kw: events.append(("uvicorn", a, kw)))
    args = build_parser().parse_args(["server", "--log-level", level])
    assert cmd_server(args) == 0
    assert events == [
        ("logging", (level, None)),
        (
            "uvicorn",
            (app,),
            {
                "host": "127.0.0.1",
                "port": 8765,
                "log_config": None,
                "log_level": "warning",
                "access_log": False,
                "ssl_keyfile": None,
                "ssl_certfile": None,
            },
        ),
    ]
