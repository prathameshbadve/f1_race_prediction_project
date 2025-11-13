"""
Dagster resource for FastF1 API
"""

import logging
from pathlib import Path
from typing import Optional, List

import pandas as pd
import fastf1
from fastf1 import Cache
from dagster import ConfigurableResource, InitResourceContext

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

    def get_season_schedule(
        self,
        year: int,
    ) -> Optional[pd.DataFrame]:
        """
        Load an F1 season schedule.
        Checks if the schedule file is already available in the bucket storage,
        and accordingly returns the cached file or downloads the file from the API.

        Args:
            year: Season year (e.g., 2024)

        Returns:
            pd.DataFrame
        """

        # Get FastF1 Config
        config = self.get_config()

        try:
            # Get event schedule from API
            season_schedule = fastf1.get_event_schedule(
                include_testing=config.include_testing,
                year=year,
            )
            self._logger.info("| | Season schedule loaded successfully from API.")

            self._logger.debug("| | Enhancing season schedule with Season column.")
            enhanced_schedule = season_schedule.copy()
            enhanced_schedule["Season"] = year  # Add Season column

            return enhanced_schedule

        except Exception as e:  # pylint: disable=broad-except
            self._logger.error(
                "| | Failed to load season schedule for %d: %s", year, str(e)
            )
            return None

    def get_grands_prix_from_season(self, year: int) -> List[str]:
        """
        Returns the list of grands prix from a F1 season schedule.

        Args:
            year: e.g. 2023 or 2024

        Returns:
            List of grands prix names
        """

        season_schedule = self.get_season_schedule(year=year)
        grands_prix_names = list(season_schedule["EventName"])

        return grands_prix_names

    def _get_session(
        self, year: int, event: str, session: str, backend: str = "fastf1"
    ):
        """
        Load an F1 session

        Args:
            year: Season year (e.g., 2024)
            event: Event name or round number (e.g., "Monaco Grand Prix" or 6)
            session: Session identifier (e.g., "Race", "Qualifying")
        """

        return fastf1.get_session(year, event, session, backend=backend)

    def get_session_results(self, year: int, event: str, session: str):
        """
        Get results of a F1 session

        Args:
            year:
            event:
            session:

        Returns:
            pd.DataFrame
        """

        session_object = self._get_session(year, event, session)

        session_object.load(
            laps=False,
            messages=False,
            weather=False,
            telemetry=False,
        )

        session_results = session_object.results.copy()

        # Replace NaT with None in all timedelta columns
        timedelta_cols = ["Time", "Q1", "Q2", "Q3"]
        for col in timedelta_cols:
            session_results[col] = session_results[col].replace({pd.NaT: None})

        # Add session metadata
        session_results["EventName"] = (
            session_object.event.EventName if hasattr(session_object, "event") else None
        )
        session_results["SessionName"] = (
            session_object.name if hasattr(session_object, "name") else None
        )
        session_results["SessionDate"] = (
            session_object.date if hasattr(session_object, "date") else None
        )

        return session_results

    def get_session_laps(self, year: int, event: str, session: str):
        """
        Get laps data of a F1 session

        Args:
            year:
            event:
            session:

        Returns:
            pd.DataFrame
        """

        if year < 2018:
            session_object = self._get_session(
                year,
                event,
                session,
                backend="ergast",
            )

        session_object = self._get_session(year, event, session)

        session_object.load(
            laps=True,
            messages=False,
            weather=False,
            telemetry=False,
        )

        session_laps = session_object.laps.copy()

        # Add computed columns
        if "LapTime" in session_laps.columns:
            session_laps["LapTimeSeconds"] = session_laps["LapTime"].dt.total_seconds()

        # Replace NaT with None in all timedelta columns
        timedelta_cols = [
            "Time",
            "LapTime",
            "PitOutTime",
            "PitInTime",
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "Sector1SessionTime",
            "Sector2SessionTime",
            "Sector3SessionTime",
            "LapStartTime",
            "LapStartDate",
        ]
        for col in timedelta_cols:
            session_laps[col] = session_laps[col].replace({pd.NaT: None})

        # Add session metadata
        session_laps["EventName"] = (
            session_object.event.EventName if hasattr(session_object, "event") else None
        )
        session_laps["SessionName"] = (
            session_object.name if hasattr(session_object, "name") else None
        )
        session_laps["SessionDate"] = (
            session_object.date if hasattr(session_object, "date") else None
        )

        return session_laps

    def get_session_weather(self, year: int, event: str, session: str):
        """
        Get weather data of a F1 session

        Args:
            year:
            event:
            session:

        Returns:
            pd.DataFrame
        """

        session_object = self._get_session(year, event, session)

        session_object.load(
            laps=False,
            messages=False,
            weather=True,
            telemetry=False,
        )

        session_weather = session_object.weather_data.copy()

        # Add session metadata
        session_weather["EventName"] = (
            session_object.event.EventName if hasattr(session_object, "event") else None
        )
        session_weather["SessionName"] = (
            session_object.name if hasattr(session_object, "name") else None
        )
        session_weather["SessionDate"] = (
            session_object.date if hasattr(session_object, "date") else None
        )

        return session_weather

    def get_session_race_control_messages(self, year: int, event: str, session: str):
        """
        Get race control messages data of a F1 session

        Args:
            year:
            event:
            session:

        Returns:
            pd.DataFrame
        """

        session_object = self._get_session(year, event, session)

        session_object.load(
            laps=False,
            messages=True,
            weather=False,
            telemetry=False,
        )

        session_race_control_messages = session_object.race_control_messages.copy()

        # Add session metadata
        session_race_control_messages["EventName"] = (
            session_object.event.EventName if hasattr(session_object, "event") else None
        )
        session_race_control_messages["SessionName"] = (
            session_object.name if hasattr(session_object, "name") else None
        )
        session_race_control_messages["SessionDate"] = (
            session_object.date if hasattr(session_object, "date") else None
        )

        return session_race_control_messages

    def get_session_track_status(self, year: int, event: str, session: str):
        """
        Get track status data of a F1 session

        Args:
            year:
            event:
            session:

        Returns:
            pd.DataFrame
        """

        session_object = self._get_session(year, event, session)

        session_object.load(
            laps=True,
            messages=False,
            weather=False,
            telemetry=False,
        )

        session_track_status = session_object.track_status.copy()

        # Add session metadata
        session_track_status["EventName"] = (
            session_object.event.EventName if hasattr(session_object, "event") else None
        )
        session_track_status["SessionName"] = (
            session_object.name if hasattr(session_object, "name") else None
        )
        session_track_status["SessionDate"] = (
            session_object.date if hasattr(session_object, "date") else None
        )

        return session_track_status

    def get_session_status(self, year: int, event: str, session: str):
        """
        Get session status data of a F1 session

        Args:
            year:
            event:
            session:

        Returns:
            pd.DataFrame
        """

        session_object = self._get_session(year, event, session)

        session_object.load(
            laps=True,
            messages=False,
            weather=False,
            telemetry=False,
        )

        session_status = session_object.session_status.copy()

        # Add session metadata
        session_status["EventName"] = (
            session_object.event.EventName if hasattr(session_object, "event") else None
        )
        session_status["SessionName"] = (
            session_object.name if hasattr(session_object, "name") else None
        )
        session_status["SessionDate"] = (
            session_object.date if hasattr(session_object, "date") else None
        )

        return session_status

    def get_session_info(self, year: int, event: str, session: str):
        """
        Get general session info/metadata
        """

        session_object = self._get_session(year, event, session)

        session_info = {
            "event_name": [
                session_object.event.EventName
                if hasattr(session_object, "event")
                else None
            ],
            "location": [
                session_object.event.Location
                if hasattr(session_object, "event")
                else None
            ],
            "country": [
                session_object.event.Country
                if hasattr(session_object, "event")
                else None
            ],
            "session_name": [
                session_object.name if hasattr(session_object, "name") else None
            ],
            "session_date": [
                session_object.date if hasattr(session_object, "date") else None
            ],
            "total_laps": [
                session_object.total_laps
                if hasattr(session_object, "total_laps")
                else None
            ],
        }

        # Add event metadata if available
        if hasattr(session_object, "event"):
            event = session_object.event
            session_info["event_format"] = getattr(event, "EventFormat", None)
            session_info["round_number"] = getattr(event, "RoundNumber", None)
            session_info["official_event_name"] = getattr(
                event, "OfficialEventName", None
            )
            session_info["session_1"] = getattr(event, "Session1", None)
            session_info["session_2"] = getattr(event, "Session2", None)
            session_info["session_3"] = getattr(event, "Session3", None)
            session_info["session_4"] = getattr(event, "Session4", None)
            session_info["session_5"] = getattr(event, "Session5", None)

        session_info_df = pd.DataFrame(session_info)

        return session_info_df
