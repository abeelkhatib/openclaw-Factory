from __future__ import annotations

import json
import uuid

import httpx

BASE_URL = "http://localhost:3000"


def main() -> None:
    payload = {
        "topic_clusters": ["news", "evergreen"],
        "videos_per_channel": 2,
        "channels": ["alpha", "beta"],
    }
    idempotency_key = str(uuid.uuid4())

    with httpx.Client(timeout=15.0) as client:
        first = client.post(
            f"{BASE_URL}/jobs/start",
            headers={"Idempotency-Key": idempotency_key},
            json=payload,
        )
        first.raise_for_status()

        second = client.post(
            f"{BASE_URL}/jobs/start",
            headers={"Idempotency-Key": idempotency_key},
            json=payload,
        )
        second.raise_for_status()

    first_body = first.json()
    second_body = second.json()

    if first_body["job_id"] != second_body["job_id"]:
        raise RuntimeError("Idempotency check failed: job_id mismatch")

    print("Idempotency check passed")
    print(f"job_id: {first_body['job_id']}")
    if first_body.get("workflow_run_id"):
        print(f"workflow_run_id: {first_body['workflow_run_id']}")
    elif first_body.get("workflow_id"):
        print(f"workflow_id: {first_body['workflow_id']}")
    print("first_response:")
    print(json.dumps(first_body, indent=2))


if __name__ == "__main__":
    main()
