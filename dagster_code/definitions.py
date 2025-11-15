"""
Dagster definitions
"""

from dagster import Definitions
from dagster_code.assets import (
    f1_season_schedule,
    f1_session_results_configurable,
    f1_session_laps_configurable,
    f1_session_weather_configurable,
    f1_session_race_control_messages_configurable,
    f1_session_track_status_configurable,
    f1_session_status_configurable,
    f1_session_session_info_configurable,
    f1_raw_schedule_validation,
    f1_raw_results_validation,
    f1_raw_laps_validation,
    f1_raw_messages_validation,
    f1_raw_weather_validation,
    f1_raw_track_status_validation,
    f1_raw_session_status_validation,
    f1_raw_session_info_validation,
)
from dagster_code.jobs import (
    f1_laps_ingestion_job,
    f1_messages_ingestion_job,
    f1_weather_ingestion_job,
    f1_track_status_ingestion_job,
    f1_session_status_ingestion_job,
    f1_session_info_ingestion_job,
)
from dagster_code.schedules import (
    f1_dynamic_laps_ingestion_schedule,
    f1_dynamic_messages_ingestion_schedule,
    f1_dynamic_session_info_ingestion_schedule,
    f1_dynamic_session_status_ingestion_schedule,
    f1_dynamic_track_status_ingestion_schedule,
    f1_dynamic_weather_ingestion_schedule,
)
from dagster_code.resources import S3Resource, FastF1Resource

from config.logging import setup_logging

setup_logging()

defs = Definitions(
    assets=[
        f1_season_schedule,
        f1_session_results_configurable,
        f1_session_laps_configurable,
        f1_session_weather_configurable,
        f1_session_race_control_messages_configurable,
        f1_session_track_status_configurable,
        f1_session_status_configurable,
        f1_session_session_info_configurable,
        f1_raw_schedule_validation,
        f1_raw_results_validation,
        f1_raw_laps_validation,
        f1_raw_messages_validation,
        f1_raw_weather_validation,
        f1_raw_track_status_validation,
        f1_raw_session_status_validation,
        f1_raw_session_info_validation,
    ],
    jobs=[
        f1_laps_ingestion_job,
        f1_messages_ingestion_job,
        f1_weather_ingestion_job,
        f1_track_status_ingestion_job,
        f1_session_status_ingestion_job,
        f1_session_info_ingestion_job,
    ],
    schedules=[
        f1_dynamic_laps_ingestion_schedule,
        f1_dynamic_messages_ingestion_schedule,
        f1_dynamic_session_info_ingestion_schedule,
        f1_dynamic_session_status_ingestion_schedule,
        f1_dynamic_track_status_ingestion_schedule,
        f1_dynamic_weather_ingestion_schedule,
    ],
    resources={
        "fastf1_resource": FastF1Resource.from_env(),
        "s3_resource": S3Resource.from_env(),
    },
)
