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


class WorkerConfig(BaseModel):
    redis_prefix: str = "cutouts"
    batch_size: int = 5


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


CONFIG = FornaxCutoutsConfig()
