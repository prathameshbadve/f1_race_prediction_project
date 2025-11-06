"""
Dagster resource for FastF1 API
"""

import logging
from pathlib import Path
from dagster import ConfigurableResource, InitResourceContext
import fastf1
from fastf1 import Cache

from config.settings import FastF1Config
from config.logging import get_logger
from src.utils.helpers import ensure_directory


class FastF1Resource(ConfigurableResource):
    """Resource for interacting with FastF1 API"""

    _logger: logging.Logger = get_logger("dagster.fastf1_resource")

    def get_config(self) -> FastF1Config:
        """Get Pydantic configuration from env variables"""

        return FastF1Config.from_env()

    def setup_for_execution(self, context: InitResourceContext):  # pylint: disable=unused-argument
        """
        Initialize FastF1 cache
        Called automatically by Dagster before job execution.
        """

        config = self.get_config()
        cache_path = Path("/opt/dagster/app/" + config.cache_dir)
        if config.cache_enabled:
            ensure_directory(cache_path)
            Cache.enable_cache(
                cache_dir=str(cache_path),
                force_renew=config.force_renew_cache,
            )
        else:
            Cache.set_disabled()

        fastf1.set_log_level(config.log_level)

        self._logger.info("| FastF1Resource initialized. Cache Dir: %s", cache_path)

    @classmethod
    def from_env(cls):
        """Factory to create FastF1Resource from environment variables."""

        return cls(config=FastF1Config.from_env())

    def _get_session_names(self):
        """Returns the session names as per configuration"""

        config = self.get_config()

        session_names = []

        if config.include_race:
            session_names.append("Race")
        if config.include_qualifying:
            session_names.append("Qualifying")
        if config.include_sprint:
            session_names.append("Sprint")
        if config.include_sprint_qualifying:
            session_names.extend(["Sprint Shootout", "Sprint Qualifying"])
        if config.include_practice:
            session_names.extend(["Practice 1", "Practice 2", "Practice 3"])

        return session_names

    def get_session(self, year: int, event: str, session: str):
        """
        Load an F1 session

        Args:
            year: Season year (e.g., 2024)
            event: Event name or round number (e.g., "Monaco Grand Prix" or 6)
            session: Session identifier (e.g., "Race", "Qualifying")
        """

        return fastf1.get_session(year, event, session)
