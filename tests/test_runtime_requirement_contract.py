"""Runtime-requirement routing contract for distributed ``nirs4all.run`` jobs.

The cluster runs whole ``nirs4all.run`` calls in a worker subprocess, so every
task must land on a worker that can *prove* the ``nirs4all`` runtime. The server
therefore attests a package-availability requirement on submission
(``server/app.py::submit_job``). This suite pins that seam — the one that keeps a
mixed fleet correct as the ecosystem migrates toward a ``nirs4all-core`` skeleton:

- when the client pins no ``nirs4all`` spec, the server injects a presence-only
  requirement (``packages["nirs4all"] == ""``) — fail-closed against a worker
  that never declared it;
- an explicit client pin (e.g. ``">=0.9,<0.10"``) is preserved, never clobbered;
- pinning *other* packages (extra fleet capabilities) composes with — does not
  replace — the mandatory ``nirs4all`` presence.

Routing is decided by what a worker declares at registration, so a worker that
cannot prove ``nirs4all`` is never leased an ``nirs4all.run`` task. No nirs4all is
imported here (same as the rest of the API suite); the server runs in open/dev
mode so the checks stay focused on the requirement contract, not RBAC.
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from nirs4all_cluster.schemas import JobRequest
from nirs4all_cluster.server.app import ServerConfig, create_app


def _client(tmp_path) -> TestClient:
    return TestClient(create_app(ServerConfig(state_dir=str(tmp_path / "state"))))


def _stored_request(client: TestClient, job_id: str) -> JobRequest:
    return JobRequest.model_validate_json(client.app.state.db.get_job(job_id)["request_json"])


def _worker(packages: dict[str, str], labels: dict[str, str] | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"slots_total": 1, "version": {"packages": packages}}
    if labels is not None:
        body["labels"] = labels
    return body


def _atomic_job(requirements: dict[str, Any] | None = None) -> dict[str, Any]:
    job: dict[str, Any] = {
        "type": "nirs4all.run",
        "pipeline": {"kind": "path", "path": "/shared/pls.yaml"},
        "dataset": {"kind": "shared_path", "path": "/shared/data"},
    }
    if requirements is not None:
        job["requirements"] = requirements
    return job


def _lease(client: TestClient, worker_body: dict[str, Any]) -> dict[str, Any] | None:
    worker_id = client.post("/v1/workers/register", json=worker_body).json()["worker_id"]
    return client.post(f"/v1/workers/{worker_id}/lease").json()["task"]


def test_server_injects_presence_only_nirs4all_requirement(tmp_path):
    with _client(tmp_path) as client:
        job_id = client.post("/v1/jobs", json=_atomic_job()).json()["id"]
        stored = _stored_request(client, job_id)
        assert stored.requirements.packages == {"nirs4all": ""}

        # Fail-closed: a worker that never declared nirs4all is not eligible.
        assert _lease(client, _worker({})) is None
        assert _lease(client, _worker({"scipy": "1.11.0"})) is None
        # A worker that proves nirs4all (any version) is leased the task.
        leased = _lease(client, _worker({"nirs4all": "0.9.5"}))
        assert leased is not None
        assert leased["job_id"] == job_id


def test_server_preserves_client_pinned_nirs4all_range(tmp_path):
    with _client(tmp_path) as client:
        pinned = _atomic_job({"packages": {"nirs4all": ">=0.9,<0.10"}})
        job_id = client.post("/v1/jobs", json=pinned).json()["id"]
        stored = _stored_request(client, job_id)
        # The explicit pin survives; the server does not clobber it with "".
        assert stored.requirements.packages == {"nirs4all": ">=0.9,<0.10"}

        assert _lease(client, _worker({"nirs4all": "0.8.9"})) is None  # out of range
        leased = _lease(client, _worker({"nirs4all": "0.9.5"}))
        assert leased is not None
        assert leased["job_id"] == job_id


def test_extra_package_pins_compose_with_mandatory_nirs4all(tmp_path):
    """Pinning fleet-capability packages must not drop the nirs4all presence guard."""
    with _client(tmp_path) as client:
        job = _atomic_job({"labels": {"site": "lab"}, "packages": {"torch": ">=2.2"}})
        job_id = client.post("/v1/jobs", json=job).json()["id"]
        stored = _stored_request(client, job_id)
        assert stored.requirements.packages == {"torch": ">=2.2", "nirs4all": ""}

        # Extra package but no nirs4all -> ineligible.
        assert _lease(client, _worker({"torch": "2.3.0"}, labels={"site": "lab"})) is None
        # nirs4all but missing the extra package -> ineligible.
        assert _lease(client, _worker({"nirs4all": "0.9.5"}, labels={"site": "lab"})) is None
        # Only a worker proving both packages (and the label) is leased.
        capable = _worker({"nirs4all": "0.9.5", "torch": "2.3.0"}, labels={"site": "lab"})
        leased = _lease(client, capable)
        assert leased is not None
        assert leased["job_id"] == job_id
