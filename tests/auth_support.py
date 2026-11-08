"""Explicit authenticated protocol-v2 harness; never shipped as open app mode."""

from fastapi.testclient import TestClient as StarletteTestClient
from nirs4all_access import ProjectPrincipal, issue_credential

from nirs4all_cluster.server.app import create_app as production_app
from nirs4all_cluster.server.auth import ALL_RIGHTS
from nirs4all_cluster.server.state import bootstrap_state

PRINCIPAL = ProjectPrincipal("test", "ops", frozenset({"cluster"}), frozenset(ALL_RIGHTS))
TOKEN, RECORD = issue_credential(PRINCIPAL)


def provision(state, records=(RECORD,)):
    bootstrap_state(state, tuple(records))


def create_app(config):
    provision(config.state_dir)
    return production_app(config)


class TestClient(StarletteTestClient):
    """Attach recorded session/attempt to legacy simulated-worker calls explicitly."""

    __test__ = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers["Authorization"] = "Bearer " + TOKEN
        self.sessions = {}
        self.attempts = {}
        self.task_workers = {}
        self.last_worker = None

    def __enter__(self):
        super().__enter__()
        service = self.app.state.services["test"]
        self.app.state.db, self.app.state.broker, self.app.state.store = service.db, service.broker, service.store
        return self

    def request(self, method, url, **kwargs):
        url = str(url)
        headers = dict(kwargs.get("headers") or {})
        params = dict(kwargs.get("params") or {})
        worker = None
        if "/v1/workers/" in url and not url.endswith("/register"):
            worker = url.split("/workers/")[1].split("/")[0]
            if url.endswith("/heartbeat") and kwargs.get("json") is None:
                kwargs["json"] = {
                    "active_attempts": [
                        {"task_id": t, "attempt": a} for t, a in self.attempts.items() if self.task_workers[t] == worker
                    ]
                }
        elif "/v1/tasks/" in url:
            task = url.split("/tasks/")[1].split("/")[0]
            worker = params.get("worker_id") or self.task_workers.get(task) or self.last_worker
            params.setdefault("worker_id", worker)
            params.setdefault("attempt", self.attempts.get(task, 1))
            kwargs["params"] = params
        if worker in self.sessions:
            headers.setdefault("X-N4C-Worker-Secret", self.sessions[worker])
        kwargs["headers"] = headers
        response = super().request(method, url, **kwargs)
        if response.status_code == 200 and method.upper() == "POST":
            if url.endswith("/workers/register"):
                r = response.json()
                self.sessions[r["worker_id"]] = r["worker_secret"]
                self.last_worker = r["worker_id"]
            elif url.endswith("/lease"):
                t = response.json().get("task")
                if t:
                    self.attempts[t["task_id"]] = t["attempt"]
                    self.task_workers[t["task_id"]] = worker
        return response
