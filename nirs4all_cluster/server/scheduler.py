"""Scheduling policy: state machines and worker/task matching.

This module is the *policy* half of the server; ``db.py`` is the *mechanism*
(atomic SQL). Keeping the transition tables here makes them unit-testable in
isolation (``tests/test_state_machine.py``) and lets the DB enforce them.
"""

from __future__ import annotations

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from ..schemas import JobStatus, Requirements, TaskStatus

# --------------------------------------------------------------------------- #
# State machines (mirror PROTOTYPE_DESIGN.md "Etats job" / "Etats task")
# --------------------------------------------------------------------------- #

JOB_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.QUEUED: {JobStatus.RUNNING, JobStatus.CANCELLING, JobStatus.CANCELLED, JobStatus.FAILED},
    JobStatus.RUNNING: {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLING, JobStatus.CANCELLED},
    JobStatus.CANCELLING: {JobStatus.CANCELLED, JobStatus.FAILED},
    JobStatus.FAILED: {JobStatus.QUEUED},  # manual retry
    JobStatus.SUCCEEDED: set(),
    JobStatus.CANCELLED: set(),
}

TASK_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.QUEUED: {TaskStatus.LEASED, TaskStatus.CANCELLED},
    TaskStatus.LEASED: {TaskStatus.RUNNING, TaskStatus.LOST, TaskStatus.QUEUED, TaskStatus.FAILED, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.LOST, TaskStatus.CANCELLED},
    TaskStatus.LOST: {TaskStatus.QUEUED, TaskStatus.FAILED, TaskStatus.CANCELLED},
    TaskStatus.FAILED: {TaskStatus.QUEUED},  # manual / lease retry
    TaskStatus.SUCCEEDED: set(),
    TaskStatus.CANCELLED: set(),
}


class IllegalTransition(ValueError):
    """Raised when a state transition is not permitted by the state machine."""


def job_can_transition(src: JobStatus, dst: JobStatus) -> bool:
    return dst in JOB_TRANSITIONS.get(src, set())


def task_can_transition(src: TaskStatus, dst: TaskStatus) -> bool:
    return dst in TASK_TRANSITIONS.get(src, set())


def validate_job_transition(src: JobStatus, dst: JobStatus) -> None:
    if not job_can_transition(src, dst):
        raise IllegalTransition(f"illegal job transition {src.value} -> {dst.value}")


def validate_task_transition(src: TaskStatus, dst: TaskStatus) -> None:
    if not task_can_transition(src, dst):
        raise IllegalTransition(f"illegal task transition {src.value} -> {dst.value}")


# --------------------------------------------------------------------------- #
# Matching
# --------------------------------------------------------------------------- #


def version_satisfies(installed: str | None, spec: str) -> bool:
    """Does an installed version satisfy a PEP 440 specifier?

    ``spec == ""`` means presence-only (any installed version qualifies).
    A package the worker has not declared (``installed is None``) never
    satisfies an explicit requirement — unknown availability is treated as
    *unavailable*, so a job is not routed to a worker that cannot prove it.
    """
    if installed is None:
        return False
    if not spec:
        return True
    try:
        return Version(installed) in SpecifierSet(spec)
    except (InvalidSpecifier, InvalidVersion):
        return False


def requirements_match(
    reqs: Requirements,
    worker_labels: dict[str, str],
    worker_capabilities: dict | None = None,
    worker_versions: dict[str, str] | None = None,
) -> bool:
    """Decide whether a worker may run a task with the given requirements.

    Enforces, in order: exact label filtering, an optional memory floor (when the
    worker advertises ``memory_gb``), and package availability/version matching
    (``requirements.packages``, e.g. ``{"nirs4all": ">=0.9,<0.10"}``) against the
    versions the worker declared at registration.
    """
    caps = worker_capabilities or {}
    versions = worker_versions or {}
    for key, value in reqs.labels.items():
        if worker_labels.get(key) != value:
            return False
    if reqs.min_memory_gb is not None:
        advertised = caps.get("memory_gb")
        if advertised is not None and float(advertised) < float(reqs.min_memory_gb):
            return False
    if reqs.min_gpu_count is not None:
        # Fail-closed: undeclared GPU count == 0.
        if int(caps.get("gpu_count", 0) or 0) < reqs.min_gpu_count:
            return False
    for package, spec in reqs.packages.items():
        if not version_satisfies(versions.get(package), spec):
            return False
    return True


def aggregate_metric_better(candidate: float | None, current: float | None, mode: str) -> bool:
    """Return True if ``candidate`` ranks better than ``current`` for ``mode``."""
    if candidate is None:
        return False
    if current is None:
        return True
    return candidate < current if mode == "min" else candidate > current
