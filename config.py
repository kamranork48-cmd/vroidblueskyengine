from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    app_encryption_key: str = Field(default="", validation_alias="APP_ENCRYPTION_KEY")
    session_max_age_hours: int = Field(default=24, ge=1, le=720, validation_alias="SESSION_MAX_AGE_HOURS")
    bsky_handle: str = Field(default="", validation_alias="BSKY_HANDLE")
    bsky_app_password: str = Field(default="", validation_alias="BSKY_APP_PASSWORD")
    database_url: str = Field(default="sqlite:///./data/bsky_bot.db", validation_alias="DATABASE_URL")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    log_file: str = Field(default="logs/bsky-bot.log", validation_alias="LOG_FILE")
    dry_run: bool = Field(default=True, validation_alias="DRY_RUN")
    follow_delay_min_seconds: int = Field(default=15, ge=1, validation_alias="FOLLOW_DELAY_MIN_SECONDS")
    follow_delay_max_seconds: int = Field(default=40, ge=1, validation_alias="FOLLOW_DELAY_MAX_SECONDS")
    max_follows_per_day: int = Field(default=250, ge=0, validation_alias="MAX_FOLLOWS_PER_DAY")
    max_followers: int = Field(default=300, ge=1, validation_alias="MAX_FOLLOWERS")
    search_keywords: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["furry art", "3d model", "2d model", "vtuber"],
        validation_alias="SEARCH_KEYWORDS",
    )
    recent_post_limit: int = Field(default=10, ge=1, le=100, validation_alias="RECENT_POST_LIMIT")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @field_validator("search_keywords", mode="before")
    @classmethod
    def parse_keywords(cls, value: object) -> list[str]:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("follow_delay_max_seconds")
    @classmethod
    def max_delay_must_exceed_min(cls, value: int, info: object) -> int:
        minimum = info.data.get("follow_delay_min_seconds", 1)
        if value < minimum:
            raise ValueError("FOLLOW_DELAY_MAX_SECONDS must be >= FOLLOW_DELAY_MIN_SECONDS")
        return value

    @property
    def log_path(self) -> Path:
        return Path(self.log_file)

    @property
    def database_path(self) -> Path:
        if self.database_url.startswith("sqlite:///./"):
            return Path(self.database_url.removeprefix("sqlite:///./"))
        return Path("data/bsky_bot.db")


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    settings.log_path.parent.mkdir(parents=True, exist_ok=True)
    return settings
