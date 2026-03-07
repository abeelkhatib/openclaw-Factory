from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/openclaw"
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "pipeline-task-queue"
    n8n_status_webhook_url: str = "http://localhost:5678/webhook/status"
    n8n_exceptions_webhook_url: str = "http://localhost:5678/webhook/exceptions"
    fail_mode: int = 0


settings = Settings()
