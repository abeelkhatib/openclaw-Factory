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

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # MiniMax
    minimax_api_key: str = ""
    minimax_api_base: str = "https://api.minimaxi.chat/v1"
    minimax_model: str = "MiniMax-Text-01"
    minimax_speech_model: str = "speech-01-turbo"
    minimax_embedding_model: str = "embo-01"

    # Ollama
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.2"

    # Vugola
    vugola_api_key: str = ""
    vugola_api_base: str = "https://api.vugola.com/v1"

    # Remotion
    remotion_api_base: str = "http://localhost:3002"
    remotion_template: str = "PopCultureV1"

    # R2 / S3
    r2_account_id: str = ""
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""
    r2_bucket: str = "openclaw-assets"
    r2_public_base: str = ""

    # Pipeline tuning
    backlog_target: int = 200
    backlog_low_threshold: int = 50
    qa_score_threshold: float = 0.72
    max_script_variants: int = 2
    vo_cache_ttl_days: int = 30
    topic_score_cache_ttl_s: int = 3600


settings = Settings()
