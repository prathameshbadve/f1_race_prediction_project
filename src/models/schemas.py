"""
Data validation schemas for F1 data using Pydantic.

Provides validation models for season schedules, race results, qualifying results, lap data,
and weather data to ensure data quality before processing.
"""

from typing import Optional, List
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict, field_validator

import pandas as pd

from config.logging import get_logger

_logger = get_logger("data_processing.schemas")


class EventFormats(str, Enum):
    """Valid event formats"""

    CONVENTIONAL = "conventional"
    SPRINT = "sprint"
    SPRINT_SHOOTOUT = "sprint_shootout"
    SPRINT_QUALIFYING = "sprint_qualifying"


class Session1Names(str, Enum):
    """Valid session 1 names"""

    PRACTICE_1 = "Practice 1"


class Session2Names(str, Enum):
    """Valid session 2 names"""

    PRACTICE_2 = "Practice 2"
    QUALIFYING = "Qualifying"  # For 2021-2023
    SPRINT_QUALIFYING = "Sprint Qualifying"  # From 2024 onwards
    NOT_HELD = ""  # Imola 2020 due to COVID-19 restrictions


class Session3Names(str, Enum):
    """Valid session 3 names"""

    PRACTICE_2 = "Practice 2"
    PRACTICE_3 = "Practice 3"
    SPRINT_SHOOTOUT = "Sprint Shootout"  # For 2023
    SPRINT = "Sprint"  # From 2024 onwards
    NOT_HELD = ""  # Imola 2020 due to COVID-19 restrictions


class Session4Names(str, Enum):
    """Valid session 4 names"""

    QUALIFYING = "Qualifying"
    SPRINT = "Sprint"  # From 2021-2023


class Session5Names(str, Enum):
    """Valid session 5 names"""

    RACE = "Race"


class SeasonScheduleSchema(BaseModel):
    """Schema for season schedule"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    RoundNumber: int = Field(..., description="Round number of the season")
    Country: str = Field(..., description="Country hosting the grand prix")
    Location: str = Field(..., description="City hosting the grand prix")
    OfficialEventName: str = Field(
        ..., description="Official event name, including the title sponsor"
    )
    EventDate: pd.Timestamp = Field(..., description="Date of the start of the event")
    EventName: str = Field(..., description="Name of the event")
    EventFormat: EventFormats = Field(
        ..., description="Format of the grand prix - conventional or sprint"
    )
    Session1: Session1Names = Field(
        ..., description="Name of first session of the grand prix"
    )
    Session1Date: Optional[pd.Timestamp] = Field(..., description="Date of session 1")
    Session1DateUtc: pd.Timestamp = Field(..., description="UTC Date of session 1")
    Session2: Session2Names = Field(
        ..., description="Name of first session of the grand prix"
    )
    Session2Date: Optional[pd.Timestamp] | None = None
    Session2DateUtc: Optional[pd.Timestamp] | None = None
    Session3: Session3Names = Field(
        ..., description="Name of first session of the grand prix"
    )
    Session3Date: Optional[pd.Timestamp] | None = None
    Session3DateUtc: Optional[pd.Timestamp] | None = None
    Session4: Session4Names = Field(
        ..., description="Name of first session of the grand prix"
    )
    Session4Date: Optional[pd.Timestamp] = Field(..., description="Date of session 4")
    Session4DateUtc: pd.Timestamp = Field(..., description="UTC Date of session 4")
    Session5: Session5Names = Field(
        ..., description="Name of first session of the grand prix"
    )
    Session5Date: Optional[pd.Timestamp] = Field(..., description="Date of session 5")
    Session5DateUtc: pd.Timestamp = Field(..., description="UTC Date of session 5")
    F1ApiSupport: bool = Field(..., description="If the session is supported by F1 API")
    Season: int = Field(..., description="Season year")

    @field_validator("RoundNumber")
    @classmethod
    def validate_round_number(cls, n: int) -> int:
        """Ensure round number is valid"""
        try:
            if not 1 <= n <= 25:
                raise ValueError(f"Round number must be between 1 and 25, got {n}")
        except ValueError as e:
            _logger.warning("Invalid round number: %s", n)
            raise ValueError(f"Round number must be numeric, got {n}") from e
        return n


class RaceResultsSchema(BaseModel):
    """Schema for race results"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    DriverNumber: str = Field(..., description="Driver's racing number")
    BroadcastName: str = Field(..., description="Broadcast name of the driver")
    Abbreviation: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver code"
    )
    DriverId: Optional[str] = Field(
        None, description="Lowercase last name of driver, unless it already exists"
    )
    TeamName: str = Field(..., description="Constructor/Team name")
    TeamColor: str = Field(..., description="Hex code of team color")
    TeamId: str = Field(..., description="Lowercase name of the team")
    FirstName: str = Field(..., description="Driver first name")
    LastName: str = Field(..., description="Driver last name")
    FullName: str = Field(..., description="Driver full name")
    HeadshotUrl: str = Field(..., description="URL of Driver headshot")
    CountryCode: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver country code"
    )
    Position: float = Field(
        ...,
        ge=1.0,
        le=20.0,
        description="Position at which the driver crossed the finishing line",
    )
    ClassifiedPosition: str = Field(
        ..., description="Position classified by the stewards"
    )
    GridPosition: float = Field(..., description="Starting position of the driver")
    Q1: pd.Timedelta | None = None
    Q2: pd.Timedelta | None = None
    Q3: pd.Timedelta | None = None
    Time: Optional[pd.Timedelta] = Field(
        None, description="Race time or time behind winner"
    )
    Status: str = Field(..., description="Finish status")
    Points: float = Field(..., ge=0, le=26, description="Championship points earned")
    Laps: float = Field(..., ge=0, le=100, description="Laps completed by the driver")
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")

    @field_validator("Abbreviation")
    @classmethod
    def validate_abbreviation(cls, v: str) -> str:
        """Ensure abbreviation is uppercase"""

        return v.upper()

    @field_validator("DriverNumber")
    @classmethod
    def validate_driver_number(cls, v: str) -> str:
        """Ensure driver number is valid"""

        try:
            num = int(v)
            if not 1 <= num <= 99:
                raise ValueError(f"Driver number must be between 1 and 99, got {num}")
        except ValueError as e:
            _logger.warning("Invalid driver number: %s", v)
            raise ValueError(f"Driver number must be numeric, got {v}") from e
        return v

    @field_validator("SessionName")
    @classmethod
    def validate_session_type(cls, v: str) -> str:
        """Ensure session type is a race"""

        valid_types = ["Race", "Sprint"]
        if v not in valid_types:
            _logger.warning("Invalid race session type: %s", v)
        return v


class QualifyingResultSchema(BaseModel):
    """Schema for race/sprint results"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    DriverNumber: str = Field(..., description="Driver's racing number")
    BroadcastName: str = Field(..., description="Broadcast name of the driver")
    Abbreviation: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver code"
    )
    DriverId: Optional[str] = Field(
        None, description="Lowercase last name of driver, unless it already exists"
    )
    TeamName: str = Field(..., description="Constructor/Team name")
    TeamColor: str = Field(..., description="Hex code of team color")
    TeamId: str = Field(..., description="Lowercase name of the team")
    FirstName: str = Field(..., description="Driver first name")
    LastName: str = Field(..., description="Driver last name")
    FullName: str = Field(..., description="Driver full name")
    HeadshotUrl: str = Field(..., description="URL of Driver headshot")
    CountryCode: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver country code"
    )
    Position: float = Field(
        ...,
        ge=1.0,
        le=20.0,
        description="Position at which the driver crossed the finishing line",
    )
    ClassifiedPosition: Optional[str] = ""
    GridPosition: float | None = None
    Q1: Optional[pd.Timedelta] = Field(None, description="Best time in Q1")
    Q2: Optional[pd.Timedelta] = Field(None, description="Best time in Q2")
    Q3: Optional[pd.Timedelta] = Field(None, description="Best time in Q3")
    Time: pd.Timedelta | None = None
    Status: Optional[str] = ""
    Points: float | None = None
    Laps: float | None = None
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")

    @field_validator("Abbreviation")
    @classmethod
    def validate_abbreviation(cls, v: str) -> str:
        """Ensure abbreviation is uppercase"""

        return v.upper()

    @field_validator("DriverNumber")
    @classmethod
    def validate_driver_number(cls, v: str) -> str:
        """Ensure driver number is valid"""

        try:
            num = int(v)
            if not 1 <= num <= 99:
                raise ValueError(f"Driver number must be between 1 and 99, got {num}")
        except ValueError as e:
            _logger.warning("Invalid driver number: %s", v)
            raise ValueError(f"Driver number must be numeric, got {v}") from e
        return v

    @field_validator("SessionName")
    @classmethod
    def validate_session_type(cls, v: str) -> str:
        """Ensure session type is a qualifying or sprint qualifying"""

        valid_types = ["Qualifying", "Sprint Qualifying", "Sprint Shootout"]
        if v not in valid_types:
            _logger.warning("Invalid qualifying session type: %s", v)
        return v


class TyreCompounds(str, Enum):
    """Valid Tyre Compounds"""

    HYPERSOFT = "HYPERSOFT"
    ULTRASOFT = "ULTRASOFT"
    SUPERSOFT = "SUPERSOFT"
    SOFT = "SOFT"
    MEDIUM = "MEDIUM"
    HARD = "HARD"
    SUPERHARD = "SUPERHARD"
    INTERMEDIATE = "INTERMEDIATE"
    WET = "WET"
    TEST_UNKNOWN = "TEST_UNKNOWN"  # Might appear in pre-seaon or in-season tests
    UNKNOWN = "UNKNOWN"


class LapsSchema(BaseModel):
    """Schema for laps"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(
        ..., description="Session time when the lap time was set (end of lap)"
    )
    Driver: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver code"
    )
    DriverNumber: str = Field(..., description="Driver number as string")
    LapTime: pd.Timedelta = Field(..., description="Recorded lap time")
    LapNumber: float = Field(..., ge=1, description="Recorded lap number")
    Stint: float = Field(..., description="Stint number")
    PitOutTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when car exited the pit"
    )
    PitInTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when car entered the pit"
    )
    Sector1Time: Optional[pd.Timedelta] = Field(
        None, description="Session 1 recorded time"
    )
    Sector2Time: Optional[pd.Timedelta] = Field(
        None, description="Session 2 recorded time"
    )
    Sector3Time: Optional[pd.Timedelta] = Field(
        None, description="Session 3 recorded time"
    )
    Sector1SessionTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when the Sector 1 time was set"
    )
    Sector2SessionTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when the Sector 2 time was set"
    )
    Sector3SessionTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when the Sector 3 time was set"
    )
    SpeedI1: Optional[float] = Field(None, description="Speedtrap sector 1 [km/h]")
    SpeedI2: Optional[float] = Field(None, description="Speedtrap sector 2 [km/h]")
    SpeedFL: Optional[float] = Field(
        None, description="Speedtrap at finish line [km/h]"
    )
    SpeedST: Optional[float] = Field(
        None, description="Speedtrap on longest straight (Not sure) [km/h]"
    )
    IsPersonalBest: bool = Field(
        ...,
        description="Flag to indicate if the lap was the driver's personal best time",
    )
    Compound: TyreCompounds = Field(..., description="Tyre compound")
    TyreLife: Optional[int] = Field(None, ge=0, le=60, description="Tyre age in laps")
    FreshTyre: Optional[bool] = Field(None, description="Whether tyre is fresh")
    Team: str = Field(..., description="Team name")
    LapStartTime: pd.Timedelta = Field(
        ..., description="Session time when the lap started"
    )
    LapStartDate: Optional[pd.Timestamp] = Field(
        None, description="Timestamp at the start of the lap"
    )
    TrackStatus: str = Field(..., description="Track status code")
    Position: Optional[float] = Field(
        None, description="Position of the driver at the end of the lap"
    )
    Deleted: bool = Field(
        ..., description="Flag to indicate if the lap time was deleted"
    )
    DeletedReason: Optional[str] = Field(
        "", description="Reason for lap deletion if deleted"
    )
    FastF1Generated: bool = Field(
        ..., description=" Indicates that this lap was added by FastF1"
    )
    IsAccurate: bool = Field(
        ...,
        description="Indicates that the lap start and end time "
        "are synced correctly with other laps",
    )

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")

    @field_validator("LapTime")
    @classmethod
    def validate_lap_time(cls, v: pd.Timedelta) -> pd.Timedelta:
        """Ensure lap time is reasonable"""

        # Reasonable lap time: between 1 minute and 3 minutes
        if v.total_seconds() < 60 or v.total_seconds() > 300:
            _logger.warning("Unusual lap time: %s seconds", v.total_seconds())
        return v


class WeatherSchema(BaseModel):
    """Validation Schema for weather data"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(..., description="Time of the data point")
    AirTemp: float = Field(..., ge=-10, le=60, description="Air temperature in Celcius")
    Humidity: float = Field(..., ge=0, le=100, description="Humidity")
    Pressure: float = Field(
        ..., ge=900, le=1100, description="Atmospheric pressure at the track in mbar"
    )
    Rainfall: bool = Field(..., description="Whether it is raining?")
    TrackTemp: float = Field(
        ..., ge=-10, le=80, description="Track temperature in Celsius"
    )
    WindDirection: Optional[float] = Field(
        None, ge=0, le=50, description="Wind speed in m/s"
    )
    WindDirection: Optional[int] = Field(
        None, ge=0, le=360, description="Wind direction in degrees"
    )

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")

    @field_validator("AirTemp")
    @classmethod
    def validate_air_temp(
        cls,
        v: float,
        info,  # pylint: disable=unused-argument
    ) -> float:  # pylint: disable=unused-argument
        """Ensure air temp is within reasonable range"""

        if v < 0:
            _logger.warning("Air temperature (%.1f) is below freezing.", v)
        elif v > 50:
            _logger.warning("Air temperature (%.1f) is unusually high.", v)
        return v

    @field_validator("Humidity")
    @classmethod
    def validate_humidity(
        cls,
        v: float,
        info,  # pylint: disable=unused-argument
    ) -> float:  # pylint: disable=unused-argument
        """Ensure air temp is within reasonable range"""

        if v > 100:
            _logger.warning("Humidity (%.1f) higher than 100%%", v)
        return v

    @field_validator("TrackTemp")
    @classmethod
    def validate_track_temp(
        cls,
        v: float,
        info,
    ) -> float:
        """Ensure track temp is warmer than air temp (usually)"""

        air_temp = info.data.get("AirTemp")
        if air_temp and v < air_temp - 5:
            _logger.warning(
                "Track temp (%.1f) is significantly lower than air temp (%.1f)",
                v,
                air_temp,
            )
        return v


class RaceControlMessagesSchema(BaseModel):
    """Schema for race control messages"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timestamp = Field(..., description="Time of the message")
    Category: str = Field(
        ..., description="Category of the message - track, flag, drs, etc."
    )
    Message: str = Field(..., description="Content of the message")
    Status: Optional[str] = Field(
        None, description="Disabled/Enabled if relevant to the message"
    )
    Flag: Optional[str] = Field(None, description="Flag color if shown")
    Scope: Optional[str] = Field(
        None, description="Scope of the message - driver, track, etc."
    )
    Sector: Optional[str] = Field(
        None, description="Sector for which the message is relevant"
    )
    RacingNumber: Optional[str] = Field(
        None,
        description="Racing number of the driver if message is in scope of the driver",
    )
    Lap: int = Field(..., description="Lap when the message was released")

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")


class SessionStatusSchema(BaseModel):
    """Schema for session status"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(..., description="Time of session status")
    Status: str = Field(
        ..., description="Status message - inactive, started, finished, end."
    )

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")


class TrackStatusSchema(BaseModel):
    """Schema for track status"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(..., description="Time of track status")
    Status: str = Field(..., description="Status code")
    Message: str = Field(..., description="Status message")

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")


class DataValidator:
    """
    Validates DataFrames against Pydantic schemas.

    Handles batch validation of pandas DataFrames and provides
    detailed error reporting.
    """

    def __init__(self):
        self._logger = get_logger("data_processing.validator")

    def validate_season_schedule(
        self,
        df: pd.DataFrame,
        raise_on_error: bool = False,
    ) -> tuple[pd.DataFrame, List[str]]:
        """Validate the season schedule"""

        return self._validate_dataframe(df, SeasonScheduleSchema, raise_on_error)

    def _validate_dataframe(
        self,
        df: pd.DataFrame,
        schema: type[BaseModel],
        raise_on_error: bool = False,
    ) -> tuple[pd.DataFrame, List[str]]:
        """
        Generic DataFrame validation against a Pydantic schema.

        Args:
            df: DataFrame to validate
            schema: Pydantic model to validate against
            raise_on_error: Whether to raise exception on validation errors

        Returns:
            Tuple of (valid_rows_df, list_of_errors)
        """

        if df is None or df.empty:
            self._logger.warning("Empty DataFrame provided for validation")
            return df, []

        errors = []
        valid_indices = []

        self._logger.info(
            "Validating %d rows against %s schema", len(df), schema.__name__
        )

        for idx, row in df.iterrows():
            try:
                # Convert row to dict and validate
                row_dict = row.to_dict()
                schema.model_validate(row_dict)
                valid_indices.append(idx)

            except Exception as e:  # pylint: disable=broad-except
                error_msg = f"Row {idx}: {str(e)}"
                errors.append(error_msg)
                self._logger.error("Validation error: %s", error_msg)

        # Filter to valid rows only
        valid_df = df.loc[valid_indices].copy() if valid_indices else pd.DataFrame()

        # Report results
        if errors:
            self._logger.error(
                "Validation completed: %d/%d rows valid, %d errors",
                len(valid_df),
                len(df),
                len(errors),
            )
            if raise_on_error:
                raise ValueError(
                    f"Validation failed with {len(errors)} errors:\n"
                    + "\n".join(errors[:5])
                )
        else:
            self._logger.info("✅ All %d rows validated successfully", len(df))

        return valid_df, errors
