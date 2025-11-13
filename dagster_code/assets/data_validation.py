"""
Validation assets for raw data

Checks and validates the raw files ingested from the API.

All grand prix from 2018-2024 should have 8 files for Race and Qualifying sessions.
    - results
    - laps
    - weather
    - race control messages
    - track status
    - session status
    - session info
"""

from datetime import datetime

import pandas as pd
from dagster import asset, Output, AssetExecutionContext

from dagster_code.resources import S3Resource
from src.models.schemas import DataValidator
from config.logging import get_logger

_logger = get_logger("data_processing.raw_validation")


# File validation
@asset(
    name="raw_schedule_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the number of files downloaded from API",
)
def f1_raw_schedule_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Validates the season schedules for 2015-2025
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_schedules_processed": 0,
        "num_total_errors": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    directories = s3_resource.list_directories(bucket="raw", prefix="")

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    schedule_validation_results = {
        "year": [],
        "len_raw_schedule": [],
        "len_validated_schedule": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    # Iterate through every directory
    for directory in directories:
        # Log message for individual years
        _logger.info("| | Validating season schedule for %s", directory)

        # Add year to validation results
        schedule_validation_results["year"].append(directory)

        # Key to download raw season schedule
        raw_schedule_key = f"{directory}/season_schedule.parquet"
        raw_season_schedule = s3_resource.download_dataframe(
            bucket="raw",
            key=raw_schedule_key,
        )

        # Add length of raw schedule
        schedule_validation_results["len_raw_schedule"].append(len(raw_season_schedule))

        # Perform validation
        validated_df, errors = validator.validate_season_schedule(raw_season_schedule)
        validated_df["is_validated"] = True
        validated_df["validation_timestamp"] = current_time
        metadata["num_schedules_processed"] += 1

        # Add length of validated schedule and number of errors
        schedule_validation_results["len_validated_schedule"].append(len(validated_df))
        schedule_validation_results["num_validation_errors"].append(len(errors))
        metadata["num_total_errors"] += len(errors)

        # Key to upload validated schedule to the silver layer
        validated_schedule_key = f"silver/{directory}/season_schedule.parquet"
        upload_status = s3_resource.upload_dataframe(
            bucket="processed", key=validated_schedule_key, df=validated_df
        )

        # Add upload status to the validation results
        schedule_validation_results["upload_status"].append(upload_status)

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_results_key = f"silver/validation_results/season_schedule/season_schedule_validation_{time_suffix}.parquet"
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_results_key,
        df=pd.DataFrame(schedule_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )
