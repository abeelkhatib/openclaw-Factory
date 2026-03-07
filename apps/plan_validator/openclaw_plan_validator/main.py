from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="plan_validator", version="0.1.0")


class ValidateRequest(BaseModel):
    plan: dict


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/validate")
def validate(payload: ValidateRequest) -> dict:
    return {
        "valid": True,
        "reason": "stub-validator",
        "received_keys": sorted(payload.plan.keys()),
    }


def run() -> None:
    import uvicorn

    uvicorn.run("openclaw_plan_validator.main:app", host="0.0.0.0", port=3001, reload=False)
