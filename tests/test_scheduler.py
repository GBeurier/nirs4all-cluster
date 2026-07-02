"""Scheduling tests: label matching, slots, priority ordering, lease reaping.

These drive the Database directly (no HTTP) so the leasing/state logic is tested
in isolation.
"""

import sqlite3
import time

import pytest

from nirs4all_cluster.schemas import (
    DatasetRef,
    JobRequest,
    JobStatus,
    PipelineRef,
    Requirements,
    TaskStatus,
    WorkerRegister,
)
from nirs4all_cluster.server.db import Database
from nirs4all_cluster.server.scheduler import requirements_match, version_satisfies


def _make_db(tmp_path) -> Database:
    return Database(tmp_path / "store.sqlite")


def _job(labels=None, priority=0, min_memory_gb=None):
    return JobRequest(
        name="t",
        priority=priority,
        pipeline=PipelineRef(kind="path", path="/shared/pls.yaml"),
        dataset=DatasetRef(kind="shared_path", path="/shared/data"),
        requirements=Requirements(labels=labels or {}, min_memory_gb=min_memory_gb),
    )


# --------------------------------------------------------------------------- #
# requirements_match
# --------------------------------------------------------------------------- #


def test_requirements_match_labels():
    reqs = Requirements(labels={"cuda": "true", "site": "lab-a"})
    assert requirements_match(reqs, {"cuda": "true", "site": "lab-a", "extra": "x"})
    assert not requirements_match(reqs, {"cuda": "false", "site": "lab-a"})
    assert not requirements_match(reqs, {"cuda": "true"})  # missing site


def test_requirements_match_memory_floor():
    reqs = Requirements(min_memory_gb=16)
    assert requirements_match(reqs, {}, {"memory_gb": 32})
    assert not requirements_match(reqs, {}, {"memory_gb": 8})
    assert requirements_match(reqs, {}, {})  # undeclared memory -> permissive


def test_version_satisfies():
    assert version_satisfies("0.9.1", ">=0.9,<0.10")
    assert not version_satisfies("0.8.5", ">=0.9,<0.10")
    assert version_satisfies("0.9.1", "")  # presence-only
    assert not version_satisfies(None, "")  # absent never satisfies
    assert not version_satisfies(None, ">=0.9")
    assert not version_satisfies("0.9.1", "not-a-specifier")  # malformed -> no match


def test_requirements_match_gpu_count():
    reqs = Requirements(min_gpu_count=1)
    assert requirements_match(reqs, {}, {"gpu_count": 2})
    assert not requirements_match(reqs, {}, {"gpu_count": 0})
    assert not requirements_match(reqs, {}, {})  # fail-closed: undeclared == 0
    assert requirements_match(Requirements(min_gpu_count=2), {}, {"gpu_count": 2})
    assert not requirements_match(Requirements(min_gpu_count=2), {}, {"gpu_count": 1})


def test_requirements_match_packages():
    reqs = Requirements(packages={"nirs4all": ">=0.9,<0.10"})
    assert requirements_match(reqs, {}, worker_versions={"nirs4all": "0.9.1"})
    assert not requirements_match(reqs, {}, worker_versions={"nirs4all": "0.8.0"})
    assert not requirements_match(reqs, {}, worker_versions={})  # availability: absent

    presence = Requirements(packages={"torch": ""})
    assert requirements_match(presence, {}, worker_versions={"torch": "2.3.0"})
    assert not requirements_match(presence, {}, worker_versions={"numpy": "1.26"})

    py = Requirements(packages={"python": ">=3.11"})
    assert requirements_match(py, {}, worker_versions={"python": "3.11.8"})
    assert not requirements_match(py, {}, worker_versions={"python": "3.10.0"})


# --------------------------------------------------------------------------- #
# leasing
# --------------------------------------------------------------------------- #


def test_lease_respects_labels(tmp_path):
    db = _make_db(tmp_path)
    job_id = db.create_job(_job(labels={"cuda": "true"}))
    db.create_tasks_for_job(job_id, _job(labels={"cuda": "true"}))

    cpu_worker = db.register_worker(WorkerRegister(labels={"cuda": "false"}, slots_total=1))
    assert db.lease_next_task(cpu_worker, 60) is None  # label mismatch

    gpu_worker = db.register_worker(WorkerRegister(labels={"cuda": "true"}, slots_total=1))
    payload = db.lease_next_task(gpu_worker, 60)
    assert payload is not None
    assert payload.job_id == job_id


def test_lease_priority_order(tmp_path):
    db = _make_db(tmp_path)
    low = db.create_job(_job(priority=1))
    db.create_tasks_for_job(low, _job(priority=1))
    high = db.create_job(_job(priority=9))
    db.create_tasks_for_job(high, _job(priority=9))

    worker = db.register_worker(WorkerRegister(slots_total=1))
    first = db.lease_next_task(worker, 60)
    assert first.job_id == high  # higher priority leased first


def test_lease_skips_ineligible_high_priority_task(tmp_path):
    db = _make_db(tmp_path)
    gpu_req = JobRequest(
        priority=9,
        pipeline=PipelineRef(kind="path", path="/gpu.yaml"),
        dataset=DatasetRef(kind="shared_path", path="/gpu-data"),
        requirements=Requirements(min_gpu_count=1),
    )
    cpu_req = _job(priority=1)
    gpu_job = db.create_job(gpu_req)
    db.create_tasks_for_job(gpu_job, gpu_req)
    cpu_job = db.create_job(cpu_req)
    db.create_tasks_for_job(cpu_job, cpu_req)

    cpu_worker = db.register_worker(WorkerRegister(slots_total=1, capabilities={"gpu_count": 0}))
    first = db.lease_next_task(cpu_worker, 60)
    assert first is not None
    assert first.job_id == cpu_job

    gpu_worker = db.register_worker(WorkerRegister(slots_total=1, capabilities={"gpu_count": 1}))
    second = db.lease_next_task(gpu_worker, 60)
    assert second is not None
    assert second.job_id == gpu_job


def test_slots_limit_concurrency(tmp_path):
    db = _make_db(tmp_path)
    job_id = db.create_job(_job())
    # two tasks via a 1x2 matrix
    req = JobRequest(
        pipeline=PipelineRef(kind="path", path="/p.yaml"),
        datasets=[DatasetRef(kind="shared_path", path="/a"), DatasetRef(kind="shared_path", path="/b")],
    )
    db.create_tasks_for_job(job_id, req)

    worker = db.register_worker(WorkerRegister(slots_total=1))
    assert db.lease_next_task(worker, 60) is not None
    assert db.lease_next_task(worker, 60) is None  # no free slot
    # second worker can take the other task
    worker2 = db.register_worker(WorkerRegister(slots_total=1))
    assert db.lease_next_task(worker2, 60) is not None


def test_lease_expiry_requeues_and_increments_attempt(tmp_path):
    db = _make_db(tmp_path)
    job_id = db.create_job(_job())
    task_ids = db.create_tasks_for_job(job_id, _job())
    worker = db.register_worker(WorkerRegister(slots_total=1))

    payload = db.lease_next_task(worker, lease_ttl_s=0.01)
    assert payload.attempt == 1
    time.sleep(0.05)
    affected = db.reap_expired_leases()
    assert (task_ids[0], job_id) in affected

    row = db.get_task(task_ids[0])
    assert row["status"] == TaskStatus.QUEUED.value
    # leasable again with a higher attempt
    payload2 = db.lease_next_task(worker, lease_ttl_s=60)
    assert payload2.attempt == 2


def test_lease_expiry_fails_after_max_attempts(tmp_path):
    db = _make_db(tmp_path)
    req = _job()
    req.retry.max_attempts = 1
    job_id = db.create_job(req)
    task_ids = db.create_tasks_for_job(job_id, req)
    worker = db.register_worker(WorkerRegister(slots_total=1))

    db.lease_next_task(worker, lease_ttl_s=0.01)
    time.sleep(0.05)
    db.reap_expired_leases()
    row = db.get_task(task_ids[0])
    assert row["status"] == TaskStatus.FAILED.value


def test_slots_not_oversubscribed_after_dead_and_revive(tmp_path):
    """Regression: slot count is derived from the task table, so a worker that is
    marked dead (while a task is still in-flight) and then revives via heartbeat
    cannot lease beyond its slot total."""
    db = _make_db(tmp_path)
    job_id = db.create_job(_job())
    db.create_tasks_for_job(
        job_id,
        JobRequest(
            pipeline=PipelineRef(kind="path", path="/p.yaml"),
            datasets=[DatasetRef(kind="shared_path", path="/a"), DatasetRef(kind="shared_path", path="/b")],
        ),
    )
    worker = db.register_worker(WorkerRegister(slots_total=1))
    assert db.lease_next_task(worker, 60) is not None  # 1 in-flight, slot full

    db.mark_dead_workers(0)  # worker goes silent -> dead (task still running)
    db.heartbeat_worker(worker, 60)  # revives to ALIVE
    # Still 1 task in-flight -> must NOT lease a second one.
    assert db.lease_next_task(worker, 60) is None


def test_dead_worker_tasks_requeue_without_waiting_for_lease_expiry(tmp_path):
    db = _make_db(tmp_path)
    job_id = db.create_job(_job())
    task_ids = db.create_tasks_for_job(job_id, _job())
    worker = db.register_worker(WorkerRegister(slots_total=1))

    leased = db.lease_next_task(worker, lease_ttl_s=3600)
    assert leased is not None
    db.start_task(leased.task_id, worker)

    dead = db.mark_dead_workers(0)
    assert worker in dead
    assert db.reap_tasks_for_workers(dead) == [(task_ids[0], job_id)]

    row = db.get_task(task_ids[0])
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["worker_id"] is None
    assert row["lease_expires_at"] is None
    assert row["attempt"] == 1
    assert row["error"] == "worker lost"

    replacement = db.register_worker(WorkerRegister(slots_total=1))
    retry = db.lease_next_task(replacement, lease_ttl_s=60)
    assert retry is not None
    assert retry.task_id == task_ids[0]
    assert retry.attempt == 2
    with pytest.raises(PermissionError):
        db.complete_task(task_ids[0], worker, {"status": "succeeded"})


def test_dead_worker_task_cancels_when_job_is_cancelling(tmp_path):
    db = _make_db(tmp_path)
    job_id = db.create_job(_job())
    task_ids = db.create_tasks_for_job(job_id, _job())
    worker = db.register_worker(WorkerRegister(slots_total=1))

    leased = db.lease_next_task(worker, lease_ttl_s=3600)
    assert leased is not None
    db.start_task(leased.task_id, worker)
    db.set_job_status(job_id, JobStatus.CANCELLING)

    dead = db.mark_dead_workers(0)
    assert db.reap_tasks_for_workers(dead) == [(task_ids[0], job_id)]
    row = db.get_task(task_ids[0])
    assert row["status"] == TaskStatus.CANCELLED.value
    assert row["worker_id"] is None


def test_idempotency_key(tmp_path):
    db = _make_db(tmp_path)
    req = _job()
    req.idempotency_key = "abc"
    job_id = db.create_job(req)
    found = db.find_job_by_idempotency("abc")
    assert found is not None and found["id"] == job_id
    with pytest.raises(sqlite3.IntegrityError):
        db.create_job(req)  # unique constraint
