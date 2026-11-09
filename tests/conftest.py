"""Shared fixtures for the typed-client integration tests.

The typed clients (``ClusterClient`` / ``WorkerClient``) speak real HTTP, and the
server needs its lifespan (db / broker / reaper) running — so these tests drive a
live loopback ``uvicorn`` server configured with the four RBAC roles. No nirs4all is
needed (the client layer never imports it), only ``fastapi``/``httpx``/``uvicorn``
from the API test environment.
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn
from nirs4all_access import ProjectPrincipal, issue_credential

from nirs4all_cluster.server.app import ServerConfig, create_app
from nirs4all_cluster.server.auth import rights_from_roles
from nirs4all_cluster.server.state import bootstrap_state

_ROLE_NAMES = {"submitter":"alice","executor":"worker1","viewer":"dash","admin":"ops"}
_CREDENTIALS = {role:issue_credential(ProjectPrincipal("test",name,frozenset({"cluster"}),frozenset(rights_from_roles([role]))))
                for role,name in _ROLE_NAMES.items()}
SUBMITTER_TOKEN, EXECUTOR_TOKEN, VIEWER_TOKEN, ADMIN_TOKEN = [_CREDENTIALS[r][0] for r in _ROLE_NAMES]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-release-smoke",
        action="store_true",
        default=False,
        help="Run the installed-wheel release smoke test during normal collection.",
    )
    parser.addoption(
        "--artifacts-dir",
        action="store",
        default=None,
        help="Directory where ecosystem E2E entrypoints write their declared artifacts.",
    )


@pytest.fixture
def artifacts_dir(request: pytest.FixtureRequest, tmp_path: Path) -> Path:
    raw = request.config.getoption("--artifacts-dir")
    path = Path(raw).expanduser().resolve() if raw else tmp_path / "artifacts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _release_smoke_selected_explicitly(config: pytest.Config) -> bool:
    for raw_arg in config.args:
        path = raw_arg.split("::", 1)[0]
        if path.endswith("tests/test_release_smoke.py") or path.endswith("test_release_smoke.py"):
            return True
    return False


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-release-smoke") or _release_smoke_selected_explicitly(config):
        return
    keep: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        if "release_smoke" in item.keywords:
            deselected.append(item)
        else:
            keep.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = keep


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def running_server(config: ServerConfig) -> Iterator[str]:
    """Run ``create_app(config)`` under uvicorn on a loopback port for the block's duration."""
    port = _free_port()
    app = create_app(config)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if server.started:
                with contextlib.suppress(httpx.HTTPError):
                    httpx.get(base_url + "/healthz", timeout=0.5)
                    break
            time.sleep(0.02)
        else:
            raise RuntimeError("cluster test server did not start in time")
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=10.0)


class LiveCluster:
    """Handle to a running test server: its base URL plus the four role tokens."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.submitter = SUBMITTER_TOKEN
        self.executor = EXECUTOR_TOKEN
        self.viewer = VIEWER_TOKEN
        self.admin = ADMIN_TOKEN
        self.free_port = _free_port  # for the "unreachable server" case


@pytest.fixture(scope="module")
def cluster(tmp_path_factory: pytest.TempPathFactory) -> Iterator[LiveCluster]:
    state = tmp_path_factory.mktemp("cluster-state")
    bootstrap_state(state,tuple(r for _,r in _CREDENTIALS.values()))
    config = ServerConfig(state_dir=str(state), lease_ttl_s=60.0)
    with running_server(config) as base_url:
        yield LiveCluster(base_url)
