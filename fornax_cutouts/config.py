from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DeploymentType(StrEnum):
    LOCAL = "local"
    AWS = "aws"


class DeploymentEnvironment(StrEnum):
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    is_cluster: bool = False
    use_ssl: bool = False
    timeout: float = 15.0
    search_en: bool = False

    @property
    def uri(self):
        return f"redis{'s' if self.use_ssl else ''}://{self.host}:{self.port}/0"

    @property
    def connection_kwargs(self):
        return {
            "host": self.host,
            "port": self.port,
            "ssl": self.use_ssl,
        }


class WorkerConfig(BaseModel):
    redis_prefix: str | None = None
    batch_size_per_worker: int = 5
    prefetch_multiplier: int = 1
    max_tasks_per_child: int = 50


class StorageConfig(BaseModel):
    prefix: str = "/tmp"

    @property
    def is_s3(self):
        return self.prefix.startswith("s3://")


class FornaxCutoutsConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="cutouts__",
        env_nested_delimiter="__",
        env_file=".env",
    )

    service_name: str = "Fornax Cutouts"

    deployment_type: DeploymentType = "local"
    deployment_environment: DeploymentEnvironment = "dev"

    redis: RedisConfig = Field(default_factory=RedisConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)

    sync_ttl: int = 1 * 60 * 60  # 1 Hour
    # async_ttl: int = 2 * 7 * 24 * 60 * 60  # 2 Weeks (not currently used)

    log_level: str = "info"
    source_path: Path

    storage: StorageConfig = Field(default_factory=StorageConfig)

    @model_validator(mode="after")
    def set_redis_prefix_from_service_name(self):
        if self.worker.redis_prefix is None:
            self.worker.redis_prefix = self.service_name.lower().replace(" ", "-")
        return self


CONFIG = FornaxCutoutsConfig()
