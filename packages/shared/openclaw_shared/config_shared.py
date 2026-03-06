from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class SharedSettings(BaseSettings):
    """Minimal shared settings consumed by both worker and orchestrator."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/openclaw"
    redis_url: str = "redis://localhost:6379/0"


shared_settings = SharedSettings()
