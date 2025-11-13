"""
Dagster jobs for materializing assets
"""

# pylint: disable=assignment-from-no-return

from dagster import define_asset_job
from dagster_code.assets import (
    f1_session_laps_configurable,
    f1_session_weather_configurable,
    f1_session_race_control_messages_configurable,
    f1_session_track_status_configurable,
    f1_session_status_configurable,
    f1_session_session_info_configurable,
)

f1_laps_ingestion_job = define_asset_job(
    name="f1_laps_ingestion_job",
    selection=[f1_session_laps_configurable],
)

f1_weather_ingestion_job = define_asset_job(
    name="f1_weather_ingestion_job",
    selection=[f1_session_weather_configurable],
)

f1_messages_ingestion_job = define_asset_job(
    name="f1_messages_ingestion_job",
    selection=[f1_session_race_control_messages_configurable],
)

f1_track_status_ingestion_job = define_asset_job(
    name="f1_track_status_ingestion_job",
    selection=[f1_session_track_status_configurable],
)

f1_session_status_ingestion_job = define_asset_job(
    name="f1_session_status_ingestion_job",
    selection=[f1_session_status_configurable],
)

f1_session_info_ingestion_job = define_asset_job(
    name="f1_session_info_ingestion_job",
    selection=[f1_session_session_info_configurable],
)
