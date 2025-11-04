"""
Dagster resource for FastF1 API
"""

from pathlib import Path
from dagster import ConfigurableResource
import fastf1


class FastF1Resource(ConfigurableResource):
    """Resource for interacting with FastF1 API"""

    cache_dir: str = "/opt/dagster/app/data/external/fastf1_cache"

    def setup_for_resources(self):
        """Initialize FastF1 cache"""

        cache_path = Path(self.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(str(cache_path))

        fastf1.set_log_level("INFO")

    def get_session(self, year: int, event: str, session: str):
        """
        Load an F1 session

        Args:
            year: Season year (e.g., 2024)
            event: Event name or round number (e.g., "Monaco Grand Prix" or 6)
            session: Session identifier (e.g., "Race", "Qualifying")
        """

        return fastf1.get_session(year, event, session)
