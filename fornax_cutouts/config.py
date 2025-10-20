from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    is_cluster: bool = False
    use_ssl: bool = False
    timeout: float = 15.0

    @property
    def uri(self):
        return f"redis{'s' if self.use_ssl else ''}://{self.host}/{self.port}/0"


class FornaxCutoutsConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="cutouts__", env_nested_delimiter="__")

    redis: RedisConfig = Field(default_factory=RedisConfig)

    sync_ttl: int = 1 * 60 * 60  # 1 Hour
    async_ttl: int = 2 * 7 * 24 * 60 * 60  # 2 Weeks

    log_level: str = "info"
    source_path: Path


CONFIG = FornaxCutoutsConfig()
