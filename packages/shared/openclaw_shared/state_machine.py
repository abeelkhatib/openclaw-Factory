from __future__ import annotations

from openclaw_shared.status import JobStatus

_ACTIVE_STATES: set[JobStatus] = {
    JobStatus.CREATED,
    JobStatus.PENDING_SCRIPT,
    JobStatus.SCRIPTING,
    JobStatus.PENDING_CLIPS,
    JobStatus.CLIPPING,
    JobStatus.PENDING_VO,
    JobStatus.VO,
    JobStatus.PENDING_RENDER,
    JobStatus.RENDERING,
    JobStatus.PENDING_PUBLISH,
    JobStatus.PUBLISHING,
}

_TERMINAL_STATES: set[JobStatus] = {
    JobStatus.DONE,
    JobStatus.FAILED,
    JobStatus.DEAD_LETTER,
}

_FORWARD_TRANSITIONS: dict[JobStatus, JobStatus] = {
    JobStatus.CREATED: JobStatus.PENDING_SCRIPT,
    JobStatus.PENDING_SCRIPT: JobStatus.SCRIPTING,
    JobStatus.SCRIPTING: JobStatus.PENDING_CLIPS,
    JobStatus.PENDING_CLIPS: JobStatus.CLIPPING,
    JobStatus.CLIPPING: JobStatus.PENDING_VO,
    JobStatus.PENDING_VO: JobStatus.VO,
    JobStatus.VO: JobStatus.PENDING_RENDER,
    JobStatus.PENDING_RENDER: JobStatus.RENDERING,
    JobStatus.RENDERING: JobStatus.PENDING_PUBLISH,
    JobStatus.PENDING_PUBLISH: JobStatus.PUBLISHING,
    JobStatus.PUBLISHING: JobStatus.DONE,
}

_ALLOWED_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {}

for _state in _ACTIVE_STATES:
    _allowed: set[JobStatus] = {JobStatus.FAILED, JobStatus.DEAD_LETTER}
    if _state in _FORWARD_TRANSITIONS:
        _allowed.add(_FORWARD_TRANSITIONS[_state])
    _ALLOWED_TRANSITIONS[_state] = _allowed

for _state in _TERMINAL_STATES:
    _ALLOWED_TRANSITIONS[_state] = set()


def can_transition(current: JobStatus, next_status: JobStatus) -> bool:
    return next_status in _ALLOWED_TRANSITIONS.get(current, set())


def validate_transition(current: JobStatus, next_status: JobStatus) -> None:
    if current == next_status:
        return
    if not can_transition(current, next_status):
        raise ValueError(f"Invalid job transition: {current} -> {next_status}")
