from __future__ import annotations

from openclaw_shared.status import JobStatus

_ALLOWED_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.CREATED: {JobStatus.RUNNING, JobStatus.FAILED},
    JobStatus.RUNNING: {JobStatus.DONE, JobStatus.FAILED},
    JobStatus.DONE: set(),
    JobStatus.FAILED: set(),
}


def can_transition(current: JobStatus, next_status: JobStatus) -> bool:
    return next_status in _ALLOWED_TRANSITIONS[current]


def validate_transition(current: JobStatus, next_status: JobStatus) -> None:
    if current == next_status:
        return
    if not can_transition(current, next_status):
        raise ValueError(f"Invalid job transition: {current} -> {next_status}")
