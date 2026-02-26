from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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
            "ssl": self.use_ssl
        }


class WorkerConfig(BaseModel):
    redis_prefix: str = "fornax-cutouts"
    batch_size: int = 5
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

    redis: RedisConfig = Field(default_factory=RedisConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)

    sync_ttl: int = 1 * 60 * 60  # 1 Hour
    async_ttl: int = 2 * 7 * 24 * 60 * 60  # 2 Weeks

    log_level: str = "info"
    source_path: Path

    storage: StorageConfig = Field(default_factory=StorageConfig)


CONFIG = FornaxCutoutsConfig()
