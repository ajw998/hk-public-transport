from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

LogFormat = Literal["json", "console"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HK_PUBLIC_TRANSPORT_",
        env_file=".env",
        extra="ignore",
    )

    data_root: Path = Field(default=Path("data"))
    run_root: Path = Field(default=Path("_runs"))
    log_level: str = Field(default="INFO")
    log_format: LogFormat = Field(default="console")
    log_sql: bool = Field(default=False)


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    return Settings()
