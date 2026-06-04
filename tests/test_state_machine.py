"""State-machine transition tests (design 'Etats job' / 'Etats task')."""

import pytest

from nirs4all_cluster.schemas import JobStatus, TaskStatus
from nirs4all_cluster.server.scheduler import (
    IllegalTransition,
    job_can_transition,
    task_can_transition,
    validate_job_transition,
    validate_task_transition,
)


@pytest.mark.parametrize(
    "src,dst",
    [
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.CANCELLED),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.CANCELLING),
        (JobStatus.CANCELLING, JobStatus.CANCELLED),
        (JobStatus.FAILED, JobStatus.QUEUED),
    ],
)
def test_legal_job_transitions(src, dst):
    assert job_can_transition(src, dst)
    validate_job_transition(src, dst)  # does not raise


@pytest.mark.parametrize(
    "src,dst",
    [
        (JobStatus.SUCCEEDED, JobStatus.RUNNING),
        (JobStatus.CANCELLED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.SUCCEEDED),
    ],
)
def test_illegal_job_transitions(src, dst):
    assert not job_can_transition(src, dst)
    with pytest.raises(IllegalTransition):
        validate_job_transition(src, dst)


@pytest.mark.parametrize(
    "src,dst",
    [
        (TaskStatus.QUEUED, TaskStatus.LEASED),
        (TaskStatus.LEASED, TaskStatus.RUNNING),
        (TaskStatus.LEASED, TaskStatus.LOST),
        (TaskStatus.RUNNING, TaskStatus.SUCCEEDED),
        (TaskStatus.RUNNING, TaskStatus.FAILED),
        (TaskStatus.RUNNING, TaskStatus.LOST),
        (TaskStatus.LOST, TaskStatus.QUEUED),
        (TaskStatus.LOST, TaskStatus.CANCELLED),
        (TaskStatus.FAILED, TaskStatus.QUEUED),
        (TaskStatus.QUEUED, TaskStatus.CANCELLED),
    ],
)
def test_legal_task_transitions(src, dst):
    assert task_can_transition(src, dst)
    validate_task_transition(src, dst)


@pytest.mark.parametrize(
    "src,dst",
    [
        (TaskStatus.SUCCEEDED, TaskStatus.RUNNING),
        (TaskStatus.CANCELLED, TaskStatus.QUEUED),
        (TaskStatus.QUEUED, TaskStatus.SUCCEEDED),  # must lease+start first
    ],
)
def test_illegal_task_transitions(src, dst):
    assert not task_can_transition(src, dst)
    with pytest.raises(IllegalTransition):
        validate_task_transition(src, dst)
