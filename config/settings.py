"""
All configuration settings for the project
"""

import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from src.utils.helpers import get_project_root


load_dotenv()


class Environment(str, Enum):
    """Environment types"""

    DEVELOPMENT = "development"
    PRODUCTION = "production"


class BaseConfig(BaseModel):
    """Case config class"""

    environment: str = Field(
        default_factory=lambda: os.getenv("ENVIRONMENT", Environment.DEVELOPMENT.value)
    )
    project_root: Path = Field(default_factory=get_project_root)


class StorageConfig(BaseConfig):
    """Configuration for bucket storage"""

    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    raw_data_bucket: str | None = None
    processed_data_bucket: str | None = None
    region: str | None = "ap-south-1"
    secure: bool | None = False

    @classmethod
    def from_env(cls):
        """Factory to create StorageConfig from environment variables."""

        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            raw_data_bucket=os.getenv("RAW_DATA_BUCKET"),
            processed_data_bucket=os.getenv("PROCESSED_DATA_BUCKET"),
            region=os.getenv("REGION", "ap-south-1"),
            secure=os.getenv("SECURE", "False") == "true",
        )


class FastF1Config(BaseConfig):
    """Configuration settings required for FastF1"""

    # Cache settings
    cache_enabled: bool | None = None
    cache_dir: str | None = None
    force_renew_cache: bool | None = None

    # Log settings
    log_level: str | None = None

    # Connection settings
    request_timeout: int | None = None
    max_retries: int | None = None
    retry_delay: int | None = None

    # Testing sessions
    include_testing: bool | None = None

    # Data features
    enable_laps: bool | None = None
    enable_messages: bool | None = None
    enable_weather: bool | None = None
    enable_telemetry: bool | None = None

    # Sessions
    include_practice: bool | None = None
    include_sprint_qualifying: bool | None = None
    include_sprint: bool | None = None
    include_qualifying: bool | None = None
    include_race: bool | None = None

    @classmethod
    def from_env(cls):
        """Factory to create FastF1Config from environment variables."""

        return cls(
            cache_enabled=(
                os.getenv("FASTF1_CACHE_ENABLED", "False").lower() == "true"
            ),
            cache_dir=os.getenv("FASTF1_CACHE_DIR", "data/external/fastf1_cache"),
            force_renew_cache=(
                os.getenv("FASTF1_FORCE_RENEW_CACHE", "False").lower() == "true"
            ),
            log_level=os.getenv("FASTF1_LOG_LEVEL", "DEBUG"),
            request_timeout=int(os.getenv("FASTF1_REQUEST_TIMEOUT", "30")),
            max_retries=int(os.getenv("FASTF1_MAX_RETRIES", "3")),
            retry_delay=int(os.getenv("FASTF1_RETRY_DELAY", "5")),
            include_testing=(
                os.getenv("FASTF1_INCLUDE_TESTING", "False").lower() == "true"
            ),
            enable_laps=os.getenv("FASTF1_ENABLE_LAPS", "True").lower() == "true",
            enable_weather=(
                os.getenv("FASTF1_ENABLE_WEATHER", "True").lower() == "true"
            ),
            enable_messages=(
                os.getenv("FASTF1_ENABLE_MESSAGES", "True").lower() == "true"
            ),
            enable_telemetry=(
                os.getenv("FASTF1_ENABLE_TELEMETRY", "False").lower() == "true"
            ),
            include_practice=(
                os.getenv("FASTF1_INCLUDE_PRACTICE", "False").lower() == "true"
            ),
            include_sprint_qualifying=(
                os.getenv("FASTF1_INCLUDE_SPRINT_QUALIFYING", "False").lower() == "true"
            ),
            include_sprint=(
                os.getenv("FASTF1_INCLUDE_SPRINT", "False").lower() == "true"
            ),
            include_qualifying=(
                os.getenv("FASTF1_INCLUDE_QUALIFYING", "True").lower() == "true"
            ),
            include_race=os.getenv("FASTF1_INCLUDE_RACE", "True").lower() == "true",
        )
