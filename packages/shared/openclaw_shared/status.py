from enum import StrEnum


class JobStatus(StrEnum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
