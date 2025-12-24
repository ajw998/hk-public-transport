from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

LogFormat = Literal["json", "console"]

MODE_ID = {
    "bus": 1,
    "gmb": 2,
    "mtr": 3,
    "lightrail": 4,
    "mtr_bus": 5,
    "ferry": 6,
    "tram": 7,
    "peak_tram": 8,
    "unknown": 0,
}

PLACE_TYPE_ID = {
    "stop": 1,
    "station": 2,
    "station_complex": 3,
    "pier": 4,
    "platform": 5,
    "entrance_exit": 6,
    "interchange": 7,
    "other": 8,
}

SERVICE_TYPE_ID = {
    "regular": 1,
    "night": 2,
    "express": 3,
    "holiday": 4,
    "limited": 5,
    "special": 6,
    "unknown": 0,
}


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
