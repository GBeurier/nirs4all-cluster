"""SQLite persistence for the cluster server.

A single ``Database`` instance owns one ``sqlite3.Connection`` (WAL mode,
``check_same_thread=False``) guarded by a reentrant lock. The design mandates a
*single server process*, so this is sufficient and keeps leasing atomic without
a separate broker. All time values are float epoch seconds.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from ..schemas import (
    DatasetRef,
    JobRequest,
    JobStatus,
    Outputs,
    PipelineRef,
    Requirements,
    TaskPayload,
    TaskStatus,
    WorkerRegister,
    WorkerStatus,
)
from .scheduler import (
    job_can_transition,
    requirements_match,
    validate_job_transition,
    validate_task_transition,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    name            TEXT,
    status          TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    owner           TEXT,
    request_json    TEXT NOT NULL,
    result_json     TEXT,
    error           TEXT,
    idempotency_key TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idem
    ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS tasks (
    id                  TEXT PRIMARY KEY,
    job_id              TEXT NOT NULL REFERENCES jobs(id),
    status              TEXT NOT NULL,
    attempt             INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 1,
    worker_id           TEXT,
    lease_expires_at    REAL,
    priority            INTEGER NOT NULL DEFAULT 0,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL,
    started_at          REAL,
    dataset_label       TEXT,
    pipeline_label      TEXT,
    requirements_json   TEXT NOT NULL,
    payload_json        TEXT NOT NULL,
    result_json         TEXT,
    error               TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_job ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS workers (
    id                TEXT PRIMARY KEY,
    name              TEXT,
    principal         TEXT,
    status            TEXT NOT NULL,
    created_at        REAL NOT NULL,
    last_seen_at      REAL NOT NULL,
    labels_json       TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    slots_total       INTEGER NOT NULL DEFAULT 1,
    slots_used        INTEGER NOT NULL DEFAULT 0,
    version_json      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    id            TEXT PRIMARY KEY,
    sha256        TEXT NOT NULL,
    kind          TEXT NOT NULL,
    path          TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    filename      TEXT,
    created_at    REAL NOT NULL,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha ON artifacts(sha256);

CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id    TEXT,
    task_id   TEXT,
    worker_id TEXT,
    ts        REAL NOT NULL,
    level     TEXT NOT NULL,
    type      TEXT NOT NULL,
    message   TEXT NOT NULL DEFAULT '',
    data_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id, id);

CREATE TABLE IF NOT EXISTS job_artifacts (
    job_id      TEXT NOT NULL,
    task_id     TEXT,
    role        TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    PRIMARY KEY (job_id, task_id, role)
);
CREATE INDEX IF NOT EXISTS idx_jobart_job ON job_artifacts(job_id);
"""


def _now() -> float:
    return time.time()


def _gen_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._migrate_schema()
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _migrate_schema(self) -> None:
        """Apply additive migrations for SQLite files created by older betas."""
        worker_cols = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(workers)").fetchall()
        }
        if "principal" not in worker_cols:
            self._conn.execute("ALTER TABLE workers ADD COLUMN principal TEXT")

    # --------------------------------------------------------------------- #
    # Jobs
    # --------------------------------------------------------------------- #

    def find_job_by_idempotency(self, key: str) -> sqlite3.Row | None:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM jobs WHERE idempotency_key = ?", (key,))
            return cur.fetchone()

    def _insert_job(
        self,
        conn: sqlite3.Connection,
        req: JobRequest,
        job_id: str,
        now: float,
        *,
        owner: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO jobs(id, type, name, status, priority, created_at, updated_at, "
            "owner, request_json, idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                job_id,
                req.type,
                req.name,
                JobStatus.QUEUED.value,
                req.priority,
                now,
                now,
                owner,
                req.model_dump_json(),
                req.idempotency_key,
            ),
        )

    def _insert_tasks(self, conn: sqlite3.Connection, job_id: str, req: JobRequest, now: float) -> list[str]:
        ids: list[str] = []
        for pipeline in req.pipeline_list():
            for dataset in req.dataset_list():
                task_id = _gen_id("task")
                payload = {
                    "type": req.type,
                    "pipeline": pipeline.model_dump(),
                    "dataset": dataset.model_dump(),
                    "params": req.params,
                    "outputs": req.outputs.model_dump(),
                }
                if req.scheduler is not None:
                    payload["scheduler"] = req.scheduler.model_dump()
                if req.submission is not None:
                    payload["submission"] = req.submission.model_dump()
                conn.execute(
                    "INSERT INTO tasks(id, job_id, status, attempt, max_attempts, priority, "
                    "created_at, updated_at, dataset_label, pipeline_label, requirements_json, "
                    "payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        task_id,
                        job_id,
                        TaskStatus.QUEUED.value,
                        0,
                        req.retry.max_attempts,
                        req.priority,
                        now,
                        now,
                        dataset.label(),
                        _pipeline_label(pipeline),
                        req.requirements.model_dump_json(),
                        json.dumps(payload),
                    ),
                )
                ids.append(task_id)
        return ids

    def create_job(self, req: JobRequest, *, owner: str | None = None) -> str:
        now = _now()
        job_id = _gen_id("job")
        with self._lock:
            self._insert_job(self._conn, req, job_id, now, owner=owner)
            self._conn.commit()
        return job_id

    def create_tasks_for_job(self, job_id: str, req: JobRequest) -> list[str]:
        """Decompose a job into the cartesian product of pipelines x datasets."""
        now = _now()
        with self._lock:
            ids = self._insert_tasks(self._conn, job_id, req, now)
            self._conn.commit()
        return ids

    def create_job_with_tasks(self, req: JobRequest, *, owner: str | None = None) -> tuple[str, list[str]]:
        """Create a job and all its tasks in a single transaction.

        Atomic so a crash can never leave a queued job with no tasks, and so a
        duplicate ``idempotency_key`` is rejected by the unique index as one unit
        (the caller catches ``sqlite3.IntegrityError`` to dedupe).
        """
        now = _now()
        job_id = _gen_id("job")
        with self._lock:
            self._insert_job(self._conn, req, job_id, now, owner=owner)
            ids = self._insert_tasks(self._conn, job_id, req, now)
            self._conn.commit()
        return job_id, ids

    def get_job(self, job_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    def list_jobs(
        self,
        limit: int = 100,
        *,
        status: str | None = None,
        name: str | None = None,
        created_before: float | None = None,
    ) -> list[sqlite3.Row]:
        """List jobs newest-first, optionally filtered.

        ``created_before`` is the cursor for pagination: pass the ``created_at`` of
        the last row from the previous page to fetch the next page.
        """
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if name:
            clauses.append("name LIKE ?")
            params.append(f"%{name}%")
        if created_before is not None:
            clauses.append("created_at < ?")
            params.append(created_before)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._lock:
            return list(
                self._conn.execute(
                    f"SELECT * FROM jobs{where} ORDER BY created_at DESC, id DESC LIMIT ?", params
                ).fetchall()
            )

    def count_jobs_by_status(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT status, COUNT(*) AS c FROM jobs GROUP BY status").fetchall()
            return {r["status"]: int(r["c"]) for r in rows}

    def count_workers_by_status(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT status, COUNT(*) AS c FROM workers GROUP BY status").fetchall()
            return {r["status"]: int(r["c"]) for r in rows}

    def count_tasks_in_flight(self) -> int:
        """Tasks currently leased or running across all workers."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM tasks WHERE status IN (?, ?)",
                (TaskStatus.LEASED.value, TaskStatus.RUNNING.value),
            ).fetchone()
            return int(row["c"])

    def set_job_status(self, job_id: str, status: JobStatus, *, error: str | None = None) -> None:
        with self._lock:
            row = self._conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            validate_job_transition(JobStatus(row["status"]), status)
            self._conn.execute(
                "UPDATE jobs SET status = ?, error = COALESCE(?, error), updated_at = ? WHERE id = ?",
                (status.value, error, _now(), job_id),
            )
            self._conn.commit()

    def try_set_job_status(self, job_id: str, new: JobStatus) -> bool:
        """Atomically transition a job iff the transition is currently legal.

        Returns True if it transitioned, False otherwise (job gone, already in a
        terminal state, or illegal transition). Used by finalization, where two
        workers completing the last tasks concurrently must not both flip the
        job and have the loser raise.
        """
        with self._lock:
            row = self._conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                return False
            if not job_can_transition(JobStatus(row["status"]), new):
                return False
            self._conn.execute(
                "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                (new.value, _now(), job_id),
            )
            self._conn.commit()
            return True

    def set_job_result(self, job_id: str, result: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET result_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(result), _now(), job_id),
            )
            self._conn.commit()

    # --------------------------------------------------------------------- #
    # Tasks
    # --------------------------------------------------------------------- #

    def get_task(self, task_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()

    def list_tasks_for_job(self, job_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM tasks WHERE job_id = ? ORDER BY created_at ASC", (job_id,)
                ).fetchall()
            )

    def _set_task_status(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        new: TaskStatus,
        **fields: Any,
    ) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        validate_task_transition(TaskStatus(row["status"]), new)
        sets = ["status = ?", "updated_at = ?"]
        values: list[Any] = [new.value, _now()]
        for k, v in fields.items():
            sets.append(f"{k} = ?")
            values.append(v)
        values.append(task_id)
        conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", values)
        return conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()

    def lease_next_task(self, worker_id: str, lease_ttl_s: float) -> TaskPayload | None:
        """Atomically pick the highest-priority eligible queued task for a worker.

        Eligibility = worker has a free slot, worker is alive, and the worker's
        labels satisfy the task requirements. Returns a ready-to-run payload.
        """
        now = _now()
        with self._lock:
            worker = self._conn.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if worker is None or worker["status"] != WorkerStatus.ALIVE.value:
                return None
            # Derive in-flight count from the task table (authoritative) rather than
            # a mutable counter — avoids slot drift across reaping/revival/races.
            if self._in_flight_count(worker_id) >= worker["slots_total"]:
                return None
            worker_labels = json.loads(worker["labels_json"])
            worker_caps = json.loads(worker["capabilities_json"])
            worker_versions = _worker_versions(json.loads(worker["version_json"]))
            candidates = self._conn.execute(
                """
                SELECT t.*,
                       (
                           SELECT COUNT(*)
                           FROM tasks in_flight
                           WHERE in_flight.job_id = t.job_id
                             AND in_flight.status IN (?, ?)
                       ) AS job_in_flight
                FROM tasks t
                WHERE t.status = ?
                ORDER BY t.priority DESC, job_in_flight ASC, t.created_at ASC, t.id ASC
                """,
                (TaskStatus.LEASED.value, TaskStatus.RUNNING.value, TaskStatus.QUEUED.value),
            ).fetchall()
            for task in candidates:
                reqs = Requirements.model_validate_json(task["requirements_json"])
                if not requirements_match(
                    reqs, worker_labels, worker_capabilities=worker_caps, worker_versions=worker_versions
                ):
                    continue
                expires = now + lease_ttl_s
                updated = self._set_task_status(
                    self._conn,
                    task["id"],
                    TaskStatus.LEASED,
                    worker_id=worker_id,
                    lease_expires_at=expires,
                    attempt=task["attempt"] + 1,
                )
                self._sync_slots(worker_id)
                # Job goes running on first lease.
                self._maybe_job_running(updated["job_id"])
                self._conn.commit()
                return _payload_from_row(updated)
            return None

    def start_task(self, task_id: str, worker_id: str) -> sqlite3.Row:
        with self._lock:
            row = self.get_task(task_id)
            if row is None:
                raise KeyError(task_id)
            if row["worker_id"] != worker_id:
                raise PermissionError("task leased by another worker")
            updated = self._set_task_status(self._conn, task_id, TaskStatus.RUNNING, started_at=_now())
            self._conn.commit()
            return updated

    def complete_task(self, task_id: str, worker_id: str, result: dict[str, Any]) -> sqlite3.Row:
        with self._lock:
            row = self.get_task(task_id)
            if row is None:
                raise KeyError(task_id)
            if row["worker_id"] != worker_id:
                raise PermissionError("task leased by another worker")
            updated = self._set_task_status(
                self._conn, task_id, TaskStatus.SUCCEEDED, result_json=json.dumps(result)
            )
            self._sync_slots(worker_id)
            self._conn.commit()
            return updated

    def fail_task(
        self, task_id: str, worker_id: str | None, error: str, *, retriable: bool = True
    ) -> sqlite3.Row:
        """Worker-reported failure. Requeue if attempts remain and retriable.

        A report from a worker that no longer owns the task (it was reaped and
        reassigned, or it is stale) is ignored so it cannot release another
        worker's slot or requeue a running task.

        The task always moves through ``failed`` first — legal from both ``leased``
        and ``running`` (the design's ``running -> failed -> queued|failed``) — and
        a retriable failure with attempts left is then requeued from there. A direct
        ``running -> queued`` would violate the state machine and raise.
        """
        with self._lock:
            row = self.get_task(task_id)
            if row is None:
                raise KeyError(task_id)
            if worker_id is not None and row["worker_id"] != worker_id:
                raise PermissionError("stale failure report: task not owned by this worker")
            failed = self._set_task_status(self._conn, task_id, TaskStatus.FAILED, error=error)
            if retriable and row["attempt"] < row["max_attempts"]:
                updated = self._set_task_status(
                    self._conn,
                    task_id,
                    TaskStatus.QUEUED,
                    worker_id=None,
                    lease_expires_at=None,
                    error=error,
                )
            else:
                updated = failed
            if worker_id is not None:
                self._sync_slots(worker_id)
            self._conn.commit()
            return updated

    def _resolve_lost_task(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        error: str,
    ) -> tuple[str, str]:
        """Move an in-flight task through LOST to its deterministic next state."""
        self._set_task_status(conn, row["id"], TaskStatus.LOST, error=error)
        if row["job_status"] in (JobStatus.CANCELLING.value, JobStatus.CANCELLED.value):
            self._set_task_status(
                conn,
                row["id"],
                TaskStatus.CANCELLED,
                worker_id=None,
                lease_expires_at=None,
            )
        elif row["attempt"] < row["max_attempts"]:
            self._set_task_status(
                conn,
                row["id"],
                TaskStatus.QUEUED,
                worker_id=None,
                lease_expires_at=None,
            )
        else:
            self._set_task_status(conn, row["id"], TaskStatus.FAILED, error=f"{error} (max attempts)")
        if row["worker_id"]:
            self._sync_slots(row["worker_id"])
        return row["id"], row["job_id"]

    def reap_expired_leases(self) -> list[tuple[str, str]]:
        """Requeue (or fail) tasks whose lease expired.

        Returns ``(task_id, job_id)`` for affected tasks so the caller can
        re-finalize those jobs. A task whose job is being cancelled is moved to
        ``cancelled`` rather than requeued, so a cancelled job never relaunches.
        """
        now = _now()
        affected: list[tuple[str, str]] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.*, j.status AS job_status FROM tasks t JOIN jobs j ON j.id = t.job_id "
                "WHERE t.status IN (?, ?) AND t.lease_expires_at IS NOT NULL AND t.lease_expires_at < ?",
                (TaskStatus.LEASED.value, TaskStatus.RUNNING.value, now),
            ).fetchall()
            for row in rows:
                affected.append(self._resolve_lost_task(self._conn, row, error="lease expired"))
            if affected:
                self._conn.commit()
        return affected

    def reap_tasks_for_workers(self, worker_ids: list[str], *, error: str = "worker lost") -> list[tuple[str, str]]:
        """Requeue/cancel tasks owned by workers that were declared dead.

        This is the worker-loss counterpart to lease expiry: once the server has
        deterministically classified a worker as dead, its in-flight tasks are no
        longer left in limbo until the lease timestamp happens to expire.
        """
        if not worker_ids:
            return []
        affected: list[tuple[str, str]] = []
        placeholders = ",".join("?" for _ in worker_ids)
        params: list[Any] = [
            *worker_ids,
            TaskStatus.LEASED.value,
            TaskStatus.RUNNING.value,
        ]
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.*, j.status AS job_status FROM tasks t JOIN jobs j ON j.id = t.job_id "
                f"WHERE t.worker_id IN ({placeholders}) AND t.status IN (?, ?)",
                params,
            ).fetchall()
            for row in rows:
                affected.append(self._resolve_lost_task(self._conn, row, error=error))
            if affected:
                self._conn.commit()
        return affected

    def cancel_job_tasks(self, job_id: str) -> list[str]:
        """Cancel queued tasks immediately; running tasks are marked for cooperative stop."""
        cancelled: list[str] = []
        running: list[str] = []
        with self._lock:
            rows = self._conn.execute("SELECT * FROM tasks WHERE job_id = ?", (job_id,)).fetchall()
            for row in rows:
                st = TaskStatus(row["status"])
                if st in (TaskStatus.QUEUED, TaskStatus.LEASED):
                    leasing_worker = row["worker_id"]
                    self._set_task_status(
                        self._conn, row["id"], TaskStatus.CANCELLED, worker_id=None, lease_expires_at=None
                    )
                    if leasing_worker:
                        self._sync_slots(leasing_worker)
                    cancelled.append(row["id"])
                elif st == TaskStatus.RUNNING:
                    running.append(row["id"])
            self._conn.commit()
        return running  # caller asks workers to stop these

    def cancel_running_task(self, task_id: str, worker_id: str) -> sqlite3.Row | None:
        with self._lock:
            row = self.get_task(task_id)
            if row is None or row["worker_id"] != worker_id:
                return None
            updated = self._set_task_status(self._conn, task_id, TaskStatus.CANCELLED)
            self._sync_slots(worker_id)
            self._conn.commit()
            return updated

    def tasks_for_worker(self, worker_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM tasks WHERE worker_id = ? AND status IN (?, ?)",
                (worker_id, TaskStatus.LEASED.value, TaskStatus.RUNNING.value),
            ).fetchall()
            return [r["id"] for r in rows]

    def _in_flight_count(self, worker_id: str) -> int:
        """Number of non-terminal (leased/running) tasks a worker currently owns."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS c FROM tasks WHERE worker_id = ? AND status IN (?, ?)",
            (worker_id, TaskStatus.LEASED.value, TaskStatus.RUNNING.value),
        ).fetchone()
        return int(row["c"])

    def _sync_slots(self, worker_id: str) -> None:
        """Refresh the worker's ``slots_used`` display cache from the task table."""
        self._conn.execute(
            "UPDATE workers SET slots_used = ? WHERE id = ?",
            (self._in_flight_count(worker_id), worker_id),
        )

    def _maybe_job_running(self, job_id: str) -> None:
        row = self._conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row and row["status"] == JobStatus.QUEUED.value:
            self._conn.execute(
                "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                (JobStatus.RUNNING.value, _now(), job_id),
            )

    # --------------------------------------------------------------------- #
    # Workers
    # --------------------------------------------------------------------- #

    def register_worker(self, reg: WorkerRegister, *, principal: str | None = None) -> str:
        now = _now()
        worker_id = _gen_id("worker")
        with self._lock:
            self._conn.execute(
                "INSERT INTO workers(id, name, principal, status, created_at, last_seen_at, labels_json, "
                "capabilities_json, slots_total, slots_used, version_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    worker_id,
                    reg.name,
                    principal,
                    WorkerStatus.ALIVE.value,
                    now,
                    now,
                    json.dumps(reg.labels),
                    json.dumps(reg.capabilities),
                    reg.slots_total,
                    0,
                    json.dumps(reg.version),
                ),
            )
            self._conn.commit()
        return worker_id

    def heartbeat_worker(self, worker_id: str, lease_ttl_s: float | None = None) -> bool:
        """Record a heartbeat and renew the worker's active leases.

        Leases are renewed on every heartbeat so a task that runs longer than
        ``lease_ttl_s`` is not wrongly reaped while its worker is healthy; the
        lease only lapses once the worker stops heartbeating (design intent).
        """
        with self._lock:
            now = _now()
            cur = self._conn.execute(
                "UPDATE workers SET last_seen_at = ?, status = ? WHERE id = ?",
                (now, WorkerStatus.ALIVE.value, worker_id),
            )
            if cur.rowcount and lease_ttl_s is not None:
                self._conn.execute(
                    "UPDATE tasks SET lease_expires_at = ? WHERE worker_id = ? AND status IN (?, ?)",
                    (now + lease_ttl_s, worker_id, TaskStatus.LEASED.value, TaskStatus.RUNNING.value),
                )
            self._conn.commit()
            return cur.rowcount > 0

    def get_worker(self, worker_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()

    def list_workers(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute("SELECT * FROM workers ORDER BY created_at ASC").fetchall())

    def mark_dead_workers(self, ttl_s: float) -> list[str]:
        """Mark workers silent for > ttl as dead.

        In-flight task recovery is handled by ``reap_tasks_for_workers`` so the
        state-machine transition remains explicit and testable.
        """
        cutoff = _now() - ttl_s
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM workers WHERE status = ? AND last_seen_at < ?",
                (WorkerStatus.ALIVE.value, cutoff),
            ).fetchall()
            ids = [r["id"] for r in rows]
            for wid in ids:
                self._conn.execute(
                    "UPDATE workers SET status = ? WHERE id = ?",
                    (WorkerStatus.DEAD.value, wid),
                )
            if ids:
                self._conn.commit()
            return ids

    # --------------------------------------------------------------------- #
    # Artifacts
    # --------------------------------------------------------------------- #

    def add_artifact(
        self,
        *,
        sha256: str,
        kind: str,
        path: str,
        size_bytes: int,
        filename: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        artifact_id = _gen_id("art")
        with self._lock:
            self._conn.execute(
                "INSERT INTO artifacts(id, sha256, kind, path, size_bytes, filename, created_at, "
                "metadata_json) VALUES (?,?,?,?,?,?,?,?)",
                (
                    artifact_id,
                    sha256,
                    kind,
                    path,
                    size_bytes,
                    filename,
                    _now(),
                    json.dumps(metadata or {}),
                ),
            )
            self._conn.commit()
        return artifact_id

    def get_artifact(self, artifact_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute("SELECT * FROM artifacts WHERE id = ?", (artifact_id,)).fetchone()

    def link_job_artifact(self, job_id: str, task_id: str | None, role: str, artifact_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO job_artifacts(job_id, task_id, role, artifact_id) VALUES (?,?,?,?)",
                (job_id, task_id or "", role, artifact_id),
            )
            self._conn.commit()

    def clear_job_artifact_role(self, job_id: str, role: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM job_artifacts WHERE job_id = ? AND role = ?", (job_id, role)
            )
            self._conn.commit()

    def list_job_artifacts(self, job_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT ja.role, ja.task_id, a.* FROM job_artifacts ja "
                    "JOIN artifacts a ON a.id = ja.artifact_id WHERE ja.job_id = ?",
                    (job_id,),
                ).fetchall()
            )

    # --------------------------------------------------------------------- #
    # Events
    # --------------------------------------------------------------------- #

    def add_event(
        self,
        *,
        level: str,
        type: str,
        message: str = "",
        job_id: str | None = None,
        task_id: str | None = None,
        worker_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO events(job_id, task_id, worker_id, ts, level, type, message, data_json) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (job_id, task_id, worker_id, _now(), level, type, message, json.dumps(data or {})),
            )
            self._conn.commit()
            return int(cur.lastrowid or 0)

    def list_events(self, job_id: str, after_id: int = 0, limit: int = 500) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events WHERE job_id = ? AND id > ? ORDER BY id ASC LIMIT ?",
                    (job_id, after_id, limit),
                ).fetchall()
            )

    def list_recent_events(self, limit: int = 200) -> list[sqlite3.Row]:
        """The most recent events across all jobs, oldest-first (global stream replay)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return list(reversed(rows))


# --------------------------------------------------------------------------- #
# Row -> model helpers
# --------------------------------------------------------------------------- #


def _worker_versions(version_json: dict[str, Any]) -> dict[str, str]:
    """Flatten a worker's declared environment into ``{package: version}``.

    Includes everything under ``version["packages"]`` plus ``python`` so that
    ``requirements.packages`` can constrain both library versions and the
    interpreter (e.g. ``{"python": ">=3.11"}``).
    """
    versions: dict[str, str] = {}
    packages = version_json.get("packages")
    if isinstance(packages, dict):
        versions.update({k: v for k, v in packages.items() if isinstance(v, str)})
    python = version_json.get("python")
    if isinstance(python, str):
        versions.setdefault("python", python)
    return versions


def _pipeline_label(pipeline: PipelineRef) -> str:
    if pipeline.kind == "path" and pipeline.path:
        return Path(pipeline.path).stem
    if pipeline.kind == "python_entrypoint" and pipeline.entrypoint:
        return pipeline.entrypoint
    return pipeline.kind


def _payload_from_row(row: sqlite3.Row) -> TaskPayload:
    payload = json.loads(row["payload_json"])
    return TaskPayload(
        task_id=row["id"],
        job_id=row["job_id"],
        type=payload["type"],
        attempt=row["attempt"],
        pipeline=PipelineRef.model_validate(payload["pipeline"]),
        dataset=DatasetRef.model_validate(payload["dataset"]),
        params=payload.get("params", {}),
        outputs=Outputs.model_validate(payload.get("outputs", {})),
        scheduler=payload.get("scheduler"),
        submission=payload.get("submission"),
        lease_expires_at=row["lease_expires_at"],
    )
