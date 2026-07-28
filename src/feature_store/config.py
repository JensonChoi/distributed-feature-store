from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FS_", env_file=".env", extra="ignore")

    database_url: str = "sqlite:///./feature_store.db"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:19092"
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    offline_bucket: str = "feature-store"
    api_url: str = "http://localhost:8000"
    inline_query_limit: int = 10_000
    job_poll_seconds: float = 1.0
    job_lease_seconds: int = 30
    job_heartbeat_seconds: int = 10
    job_max_attempts: int = 3
    job_retry_base_seconds: int = 5
    job_retry_max_seconds: int = 60
    stream_batch_size: int = 500
    stream_flush_seconds: float = 2.0
    log_level: str = "INFO"

    @property
    def storage_options(self) -> dict[str, str]:
        return {
            "AWS_ENDPOINT_URL": self.minio_endpoint,
            "AWS_ACCESS_KEY_ID": self.minio_access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_secret_key,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()
