"""
Dagster schedules for automating jobs
"""

import json
from typing import Optional, List

from dotenv import load_dotenv

from dagster import (
    ScheduleDefinition,
    RunRequest,
    SkipReason,
    ScheduleEvaluationContext,
)
from dagster_code.jobs import (
    f1_laps_ingestion_job,
    f1_messages_ingestion_job,
    f1_weather_ingestion_job,
    f1_track_status_ingestion_job,
    f1_session_status_ingestion_job,
    f1_session_info_ingestion_job,
)

from config.logging import get_logger
from src.clients.s3_client import S3Client

load_dotenv()

_logger = get_logger("dagster.schedules")
storage = S3Client.from_env()

YEARS: List[int] = list(range(2018, 2025))
# Laps
LAPS_KEY = "dagster_schedule_data/f1_laps_ingestion_state.json"
# Race control messages
MESSAGES_KEY = "dagster_schedule_data/f1_messages_ingestion_state.json"
# Weather
WEATHER_KEY = "dagster_schedule_data/f1_weather_ingestion_state.json"
# Track Status
TRACK_STATUS_KEY = "dagster_schedule_data/f1_track_status_ingestion_state.json"
# Session Status
SESSION_STATUS_KEY = "dagster_schedule_data/f1_session_status_ingestion_state.json"
# Session Info
SESSION_INFO_KEY = "dagster_schedule_data/f1_session_info_ingestion_state.json"

JOB_NAMES = {
    "laps": "raw_session_laps",
    "messages": "raw_session_race_control_messages",
    "weather": "raw_session_weather",
    "track_status": "raw_session_track_status",
    "session_status": "raw_session_session_status",
    "session_info": "raw_session_session_info",
}


def _read_state(object_key) -> dict:
    if storage.file_exists(bucket="raw", key=object_key):
        return storage.download_file(bucket="raw", key=object_key)
    return {"completed_years": []}


def _write_state(state: dict, object_key) -> None:
    json_data = json.dumps(state)
    storage.upload_file(
        bucket="raw",
        key=object_key,
        json_data=json_data,
    )


def _get_next_year(state: dict) -> Optional[int]:
    """Return next unprocessed year or None if all processed."""

    done = set(state.get("completed_years", []))
    for y in YEARS:
        if y not in done:
            return y
    return None


# LAPS


def f1_laps_execution_fn(context: ScheduleEvaluationContext):  # pylint: disable=unused-argument
    """
    Called each schedule tick. Return RunRequest with run_config for the next year,
    or a SkipReason if nothing left to do.
    """
    state = _read_state(LAPS_KEY)
    next_year = _get_next_year(state)

    if next_year is None:
        _logger.info("All configured years processed. Skipping schedule ticks.")
        return SkipReason("All years processed")

    _logger.info("Scheduling ingestion for year %s", next_year)

    # Mark it as queued/processed so subsequent ticks advance.
    # (If you want to avoid marking until run success, persist differently.)
    state.setdefault("completed_years", []).append(next_year)
    _write_state(state, LAPS_KEY)

    # Build run_config for the asset. For asset-backed jobs the config uses the op name.
    # Replace `f1_session_laps_configurable` with your asset/op function name if different.
    run_config = {
        "ops": {
            "raw_session_laps": {
                "config": {
                    "year": next_year,
                    "grand_prix_name": [],  # empty -> resource will fetch all
                    "session_name": ["Race", "Qualifying"],
                }
            }
        }
    }

    return RunRequest(run_key=str(next_year), run_config=run_config)


# Schedule runs every 10 minutes
f1_dynamic_laps_ingestion_schedule = ScheduleDefinition(
    name="f1_dynamic_laps_ingestion_schedule",
    job=f1_laps_ingestion_job,
    cron_schedule="*/10 * * * *",
    execution_fn=f1_laps_execution_fn,
)

# MESSAGES


def f1_messages_execution_fn(context: ScheduleEvaluationContext):  # pylint: disable=unused-argument
    """
    Called each schedule tick. Return RunRequest with run_config for the next year,
    or a SkipReason if nothing left to do.
    """
    state = _read_state(MESSAGES_KEY)
    next_year = _get_next_year(state)

    if next_year is None:
        _logger.info("All configured years processed. Skipping schedule ticks.")
        return SkipReason("All years processed")

    _logger.info("Scheduling ingestion for year %s", next_year)

    # Mark it as queued/processed so subsequent ticks advance.
    # (If you want to avoid marking until run success, persist differently.)
    state.setdefault("completed_years", []).append(next_year)
    _write_state(state, MESSAGES_KEY)

    # Build run_config for the asset. For asset-backed jobs the config uses the op name.
    # Replace `f1_session_laps_configurable` with your asset/op function name if different.
    run_config = {
        "ops": {
            "raw_session_race_control_messages": {
                "config": {
                    "year": next_year,
                    "grand_prix_name": [],  # empty -> resource will fetch all
                    "session_name": ["Race", "Qualifying"],
                }
            }
        }
    }

    return RunRequest(run_key=str(next_year), run_config=run_config)


# Schedule runs every 10 minutes
f1_dynamic_messages_ingestion_schedule = ScheduleDefinition(
    name="f1_dynamic_messages_ingestion_schedule",
    job=f1_messages_ingestion_job,
    cron_schedule="*/4 * * * *",
    execution_fn=f1_messages_execution_fn,
)


# WEATHER


def f1_weather_execution_fn(context: ScheduleEvaluationContext):  # pylint: disable=unused-argument
    """
    Called each schedule tick. Return RunRequest with run_config for the next year,
    or a SkipReason if nothing left to do.
    """
    state = _read_state(WEATHER_KEY)
    next_year = _get_next_year(state)

    if next_year is None:
        _logger.info("All configured years processed. Skipping schedule ticks.")
        return SkipReason("All years processed")

    _logger.info("Scheduling ingestion for year %s", next_year)

    # Mark it as queued/processed so subsequent ticks advance.
    # (If you want to avoid marking until run success, persist differently.)
    state.setdefault("completed_years", []).append(next_year)
    _write_state(state, WEATHER_KEY)

    # Build run_config for the asset. For asset-backed jobs the config uses the op name.
    # Replace `f1_session_laps_configurable` with your asset/op function name if different.
    run_config = {
        "ops": {
            "raw_session_weather": {
                "config": {
                    "year": next_year,
                    "grand_prix_name": [],  # empty -> resource will fetch all
                    "session_name": ["Race", "Qualifying"],
                }
            }
        }
    }

    return RunRequest(run_key=str(next_year), run_config=run_config)


# Schedule runs every 10 minutes
f1_dynamic_weather_ingestion_schedule = ScheduleDefinition(
    name="f1_dynamic_weather_ingestion_schedule",
    job=f1_weather_ingestion_job,
    cron_schedule="*/5 * * * *",
    execution_fn=f1_weather_execution_fn,
)


# TRACK STATUS


def f1_track_status_execution_fn(context: ScheduleEvaluationContext):  # pylint: disable=unused-argument
    """
    Called each schedule tick. Return RunRequest with run_config for the next year,
    or a SkipReason if nothing left to do.
    """
    state = _read_state(TRACK_STATUS_KEY)
    next_year = _get_next_year(state)

    if next_year is None:
        _logger.info("All configured years processed. Skipping schedule ticks.")
        return SkipReason("All years processed")

    _logger.info("Scheduling ingestion for year %s", next_year)

    # Mark it as queued/processed so subsequent ticks advance.
    # (If you want to avoid marking until run success, persist differently.)
    state.setdefault("completed_years", []).append(next_year)
    _write_state(state, TRACK_STATUS_KEY)

    # Build run_config for the asset. For asset-backed jobs the config uses the op name.
    # Replace `f1_session_laps_configurable` with your asset/op function name if different.
    run_config = {
        "ops": {
            "raw_session_track_status": {
                "config": {
                    "year": next_year,
                    "grand_prix_name": [],  # empty -> resource will fetch all
                    "session_name": ["Race", "Qualifying"],
                }
            }
        }
    }

    return RunRequest(run_key=str(next_year), run_config=run_config)


# Schedule runs every 10 minutes
f1_dynamic_track_status_ingestion_schedule = ScheduleDefinition(
    name="f1_dynamic_track_status_ingestion_schedule",
    job=f1_track_status_ingestion_job,
    cron_schedule="*/2 * * * *",
    execution_fn=f1_track_status_execution_fn,
)


# SESSION STATUS


def f1_session_status_execution_fn(context: ScheduleEvaluationContext):  # pylint: disable=unused-argument
    """
    Called each schedule tick. Return RunRequest with run_config for the next year,
    or a SkipReason if nothing left to do.
    """
    state = _read_state(SESSION_STATUS_KEY)
    next_year = _get_next_year(state)

    if next_year is None:
        _logger.info("All configured years processed. Skipping schedule ticks.")
        return SkipReason("All years processed")

    _logger.info("Scheduling ingestion for year %s", next_year)

    # Mark it as queued/processed so subsequent ticks advance.
    # (If you want to avoid marking until run success, persist differently.)
    state.setdefault("completed_years", []).append(next_year)
    _write_state(state, SESSION_STATUS_KEY)

    # Build run_config for the asset. For asset-backed jobs the config uses the op name.
    # Replace `f1_session_laps_configurable` with your asset/op function name if different.
    run_config = {
        "ops": {
            "raw_session_session_status": {
                "config": {
                    "year": next_year,
                    "grand_prix_name": [],  # empty -> resource will fetch all
                    "session_name": ["Race", "Qualifying"],
                }
            }
        }
    }

    return RunRequest(run_key=str(next_year), run_config=run_config)


# Schedule runs every 10 minutes
f1_dynamic_session_status_ingestion_schedule = ScheduleDefinition(
    name="f1_dynamic_session_status_ingestion_schedule",
    job=f1_session_status_ingestion_job,
    cron_schedule="*/2 * * * *",
    execution_fn=f1_session_status_execution_fn,
)


# SESSION INFO


def f1_session_info_execution_fn(context: ScheduleEvaluationContext):  # pylint: disable=unused-argument
    """
    Called each schedule tick. Return RunRequest with run_config for the next year,
    or a SkipReason if nothing left to do.
    """
    state = _read_state(SESSION_INFO_KEY)
    next_year = _get_next_year(state)

    if next_year is None:
        _logger.info("All configured years processed. Skipping schedule ticks.")
        return SkipReason("All years processed")

    _logger.info("Scheduling ingestion for year %s", next_year)

    # Mark it as queued/processed so subsequent ticks advance.
    # (If you want to avoid marking until run success, persist differently.)
    state.setdefault("completed_years", []).append(next_year)
    _write_state(state, SESSION_INFO_KEY)

    # Build run_config for the asset. For asset-backed jobs the config uses the op name.
    # Replace `f1_session_laps_configurable` with your asset/op function name if different.
    run_config = {
        "ops": {
            "raw_session_session_info": {
                "config": {
                    "year": next_year,
                    "grand_prix_name": [],  # empty -> resource will fetch all
                    "session_name": ["Race", "Qualifying"],
                }
            }
        }
    }

    return RunRequest(run_key=str(next_year), run_config=run_config)


# Schedule runs every 10 minutes
f1_dynamic_session_info_ingestion_schedule = ScheduleDefinition(
    name="f1_dynamic_session_info_ingestion_schedule",
    job=f1_session_info_ingestion_job,
    cron_schedule="*/5 * * * *",
    execution_fn=f1_session_info_execution_fn,
)
