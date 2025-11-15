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
    validation_results_key = f"silver/validation_results/season_schedule/season_schedule_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_results_key,
        df=pd.DataFrame(schedule_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )


@asset(
    name="raw_results_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the results files downloaded from API",
)
def f1_raw_results_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Checks and validates the raw results files ingested from the API.
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_results_processed": 0,
        "num_total_errors": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    years = s3_resource.list_directories(bucket="raw", prefix="")

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    results_validation_results = {
        "year": [],
        "grand_prix": [],
        "session": [],
        "len_raw_results": [],
        "len_validated_results": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    for year in years:
        # Log message for individual years
        _logger.info("| | Validating %s", year)

        grand_prix_names = s3_resource.list_directories(bucket="raw", prefix=f"{year}")

        for grand_prix in grand_prix_names:
            # Log message for individual grands prix
            _logger.info("| | | | Validating %s %s", year, grand_prix)

            session_names = s3_resource.list_directories(
                bucket="raw", prefix=f"{year}/{grand_prix}"
            )

            for session in session_names:
                # Log message for individual session
                _logger.info(
                    "| | | | | | Validating results for %s %s %s",
                    year,
                    grand_prix,
                    session,
                )

                results_validation_results["year"].append(year)
                results_validation_results["grand_prix"].append(grand_prix)
                results_validation_results["session"].append(session)

                # Key to download raw session results
                raw_results_key = f"{year}/{grand_prix}/{session}/results.parquet"
                raw_results_df = s3_resource.download_dataframe(
                    bucket="raw",
                    key=raw_results_key,
                )

                # Add length of raw schedule
                results_validation_results["len_raw_results"].append(
                    len(raw_results_df)
                )

                # Perform validation
                validated_df, errors = validator.validate_results(
                    session=session, df=raw_results_df
                )
                validated_df["is_validated"] = True
                validated_df["validation_timestamp"] = current_time
                metadata["num_results_processed"] += 1

                # Add length of validated schedule and number of errors
                results_validation_results["len_validated_results"].append(
                    len(validated_df)
                )
                results_validation_results["num_validation_errors"].append(len(errors))
                metadata["num_total_errors"] += len(errors)

                # Key to upload validated schedule to the silver layer
                validated_results_key = (
                    f"silver/{year}/{grand_prix}/{session}/results.parquet"
                )
                upload_status = s3_resource.upload_dataframe(
                    bucket="processed", key=validated_results_key, df=validated_df
                )

                # Add upload status to the validation results
                results_validation_results["upload_status"].append(upload_status)

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_results_key = (
        f"silver/validation_results/results/results_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    )
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_results_key,
        df=pd.DataFrame(results_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )


@asset(
    name="raw_laps_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the laps files downloaded from API",
)
def f1_raw_laps_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Checks and validates the raw laps files ingested from the API.
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_laps_processed": 0,
        "num_total_errors": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    years = [str(year) for year in range(2018, 2026)]

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    laps_validation_results = {
        "year": [],
        "grand_prix": [],
        "session": [],
        "len_raw_laps": [],
        "len_validated_laps": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    for year in years:
        # Log message for individual years
        _logger.info("| | Validating %s", year)

        grand_prix_names = s3_resource.list_directories(bucket="raw", prefix=f"{year}")

        for grand_prix in grand_prix_names:
            # Log message for individual grands prix
            _logger.info("| | | | Validating %s %s", year, grand_prix)

            session_names = s3_resource.list_directories(
                bucket="raw", prefix=f"{year}/{grand_prix}"
            )

            for session in session_names:
                # Log message for individual session
                _logger.info(
                    "| | | | | | Validating laps for %s %s %s",
                    year,
                    grand_prix,
                    session,
                )

                laps_validation_results["year"].append(year)
                laps_validation_results["grand_prix"].append(grand_prix)
                laps_validation_results["session"].append(session)

                # Key to download raw session laps
                raw_laps_key = f"{year}/{grand_prix}/{session}/laps.parquet"
                raw_laps_df = s3_resource.download_dataframe(
                    bucket="raw",
                    key=raw_laps_key,
                )

                # Add length of raw laps
                laps_validation_results["len_raw_laps"].append(len(raw_laps_df))

                # Perform validation
                validated_df, errors = validator.validate_laps(raw_laps_df)
                validated_df["is_validated"] = True
                validated_df["validation_timestamp"] = current_time
                metadata["num_laps_processed"] += 1

                # Add length of validated laps and number of errors
                laps_validation_results["len_validated_laps"].append(len(validated_df))
                laps_validation_results["num_validation_errors"].append(len(errors))
                metadata["num_total_errors"] += len(errors)

                # Key to upload validated laps to the silver layer
                validated_laps_key = (
                    f"silver/{year}/{grand_prix}/{session}/laps.parquet"
                )
                upload_status = s3_resource.upload_dataframe(
                    bucket="processed", key=validated_laps_key, df=validated_df
                )

                # Add upload status to the validation results
                laps_validation_results["upload_status"].append(upload_status)

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_laps_key = (
        f"silver/validation_results/laps/laps_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    )
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_laps_key,
        df=pd.DataFrame(laps_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )


@asset(
    name="raw_weather_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the weather files downloaded from API",
)
def f1_raw_weather_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Checks and validates the raw weather files ingested from the API.
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_weather_processed": 0,
        "num_total_errors": 0,
        "skipped_for_unavailablity": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    years = [str(year) for year in range(2018, 2026)]

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    weather_validation_results = {
        "year": [],
        "grand_prix": [],
        "session": [],
        "len_raw_weather": [],
        "len_validated_weather": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    for year in years:
        # Log message for individual years
        _logger.info("| | Validating %s", year)

        grand_prix_names = s3_resource.list_directories(bucket="raw", prefix=f"{year}")

        for grand_prix in grand_prix_names:
            # Log message for individual grands prix
            _logger.info("| | | | Validating %s %s", year, grand_prix)

            session_names = s3_resource.list_directories(
                bucket="raw", prefix=f"{year}/{grand_prix}"
            )

            for session in session_names:
                # Log message for individual session
                _logger.info(
                    "| | | | | | Validating weather for %s %s %s",
                    year,
                    grand_prix,
                    session,
                )

                weather_validation_results["year"].append(year)
                weather_validation_results["grand_prix"].append(grand_prix)
                weather_validation_results["session"].append(session)

                # Key to download raw session weather data
                raw_weather_key = f"{year}/{grand_prix}/{session}/weather.parquet"
                raw_weather_df = s3_resource.download_dataframe(
                    bucket="raw",
                    key=raw_weather_key,
                )

                # Add length of raw schedule
                if raw_weather_df is None:
                    weather_validation_results["len_raw_weather"].append(0)

                    metadata["skipped_for_unavailablity"] += 1

                    weather_validation_results["len_validated_weather"].append(0)
                    weather_validation_results["num_validation_errors"].append(0)
                    weather_validation_results["upload_status"].append(False)
                else:
                    weather_validation_results["len_raw_weather"].append(
                        len(raw_weather_df)
                    )

                    # Perform validation
                    validated_df, errors = validator.validate_weather(raw_weather_df)
                    validated_df["is_validated"] = True
                    validated_df["validation_timestamp"] = current_time
                    metadata["num_weather_processed"] += 1

                    # Add length of validated laps and number of errors
                    weather_validation_results["len_validated_weather"].append(
                        len(validated_df)
                    )
                    weather_validation_results["num_validation_errors"].append(
                        len(errors)
                    )
                    metadata["num_total_errors"] += len(errors)

                    # Key to upload validated laps to the silver layer
                    validated_weather_key = (
                        f"silver/{year}/{grand_prix}/{session}/weather.parquet"
                    )
                    upload_status = s3_resource.upload_dataframe(
                        bucket="processed", key=validated_weather_key, df=validated_df
                    )

                    # Add upload status to the validation results
                    weather_validation_results["upload_status"].append(upload_status)

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_weather_key = (
        f"silver/validation_results/weather/weather_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    )
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_weather_key,
        df=pd.DataFrame(weather_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )


@asset(
    name="raw_messages_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the messages files downloaded from API",
)
def f1_raw_messages_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Checks and validates the raw race control messages files ingested from the API.
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_messages_processed": 0,
        "num_total_errors": 0,
        "skipped_for_unavailablity": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    years = [str(year) for year in range(2018, 2026)]

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    messages_validation_results = {
        "year": [],
        "grand_prix": [],
        "session": [],
        "len_raw_messages": [],
        "len_validated_messages": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    for year in years:
        # Log message for individual years
        _logger.info("| | Validating %s", year)

        grand_prix_names = s3_resource.list_directories(bucket="raw", prefix=f"{year}")

        for grand_prix in grand_prix_names:
            # Log message for individual grands prix
            _logger.info("| | | | Validating %s %s", year, grand_prix)

            session_names = s3_resource.list_directories(
                bucket="raw", prefix=f"{year}/{grand_prix}"
            )

            for session in session_names:
                # Log message for individual session
                _logger.info(
                    "| | | | | | Validating race control messages for %s %s %s",
                    year,
                    grand_prix,
                    session,
                )

                messages_validation_results["year"].append(year)
                messages_validation_results["grand_prix"].append(grand_prix)
                messages_validation_results["session"].append(session)

                # Key to download raw session messages data
                raw_messages_key = f"{year}/{grand_prix}/{session}/messages.parquet"
                raw_messages_df = s3_resource.download_dataframe(
                    bucket="raw",
                    key=raw_messages_key,
                )

                # Add length of raw schedule
                if raw_messages_df is None:
                    messages_validation_results["len_raw_messages"].append(0)

                    metadata["skipped_for_unavailablity"] += 1

                    messages_validation_results["len_validated_messages"].append(0)
                    messages_validation_results["num_validation_errors"].append(0)
                    messages_validation_results["upload_status"].append(False)

                else:
                    messages_validation_results["len_raw_messages"].append(
                        len(raw_messages_df)
                    )

                    # Perform validation
                    validated_df, errors = validator.validate_race_control_messages(
                        raw_messages_df
                    )
                    validated_df["is_validated"] = True
                    validated_df["validation_timestamp"] = current_time
                    metadata["num_messages_processed"] += 1

                    # Add length of validated messages and number of errors
                    messages_validation_results["len_validated_messages"].append(
                        len(validated_df)
                    )
                    messages_validation_results["num_validation_errors"].append(
                        len(errors)
                    )
                    metadata["num_total_errors"] += len(errors)

                    # Key to upload validated messages to the silver layer
                    validated_messages_key = (
                        f"silver/{year}/{grand_prix}/{session}/messages.parquet"
                    )
                    upload_status = s3_resource.upload_dataframe(
                        bucket="processed", key=validated_messages_key, df=validated_df
                    )

                    # Add upload status to the validation results
                    messages_validation_results["upload_status"].append(upload_status)

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_messages_key = (
        f"silver/validation_results/messages/messages_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    )
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_messages_key,
        df=pd.DataFrame(messages_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )


@asset(
    name="raw_track_status_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the messages files downloaded from API",
)
def f1_raw_track_status_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Checks and validates the raw track status files ingested from the API.
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_track_status_processed": 0,
        "num_total_errors": 0,
        "skipped_for_unavailablity": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    years = [str(year) for year in range(2018, 2026)]

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    track_status_validation_results = {
        "year": [],
        "grand_prix": [],
        "session": [],
        "len_raw_track_status": [],
        "len_validated_track_status": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    for year in years:
        # Log message for individual years
        _logger.info("| | Validating %s", year)

        grand_prix_names = s3_resource.list_directories(bucket="raw", prefix=f"{year}")

        for grand_prix in grand_prix_names:
            # Log message for individual grands prix
            _logger.info("| | | | Validating %s %s", year, grand_prix)

            session_names = s3_resource.list_directories(
                bucket="raw", prefix=f"{year}/{grand_prix}"
            )

            for session in session_names:
                # Log message for individual session
                _logger.info(
                    "| | | | | | Validating race control track_status for %s %s %s",
                    year,
                    grand_prix,
                    session,
                )

                track_status_validation_results["year"].append(year)
                track_status_validation_results["grand_prix"].append(grand_prix)
                track_status_validation_results["session"].append(session)

                # Key to download raw session track_status data
                raw_track_status_key = (
                    f"{year}/{grand_prix}/{session}/track_status.parquet"
                )
                raw_track_status_df = s3_resource.download_dataframe(
                    bucket="raw",
                    key=raw_track_status_key,
                )

                # Add length of raw schedule
                if raw_track_status_df is None:
                    track_status_validation_results["len_raw_track_status"].append(0)

                    metadata["skipped_for_unavailablity"] += 1

                    track_status_validation_results[
                        "len_validated_track_status"
                    ].append(0)
                    track_status_validation_results["num_validation_errors"].append(0)
                    track_status_validation_results["upload_status"].append(False)

                else:
                    track_status_validation_results["len_raw_track_status"].append(
                        len(raw_track_status_df)
                    )

                    # Perform validation
                    validated_df, errors = validator.validate_track_status(
                        raw_track_status_df
                    )
                    validated_df["is_validated"] = True
                    validated_df["validation_timestamp"] = current_time
                    metadata["num_track_status_processed"] += 1

                    # Add length of validated track_status and number of errors
                    track_status_validation_results[
                        "len_validated_track_status"
                    ].append(len(validated_df))
                    track_status_validation_results["num_validation_errors"].append(
                        len(errors)
                    )
                    metadata["num_total_errors"] += len(errors)

                    # Key to upload validated track_status to the silver layer
                    validated_track_status_key = (
                        f"silver/{year}/{grand_prix}/{session}/track_status.parquet"
                    )
                    upload_status = s3_resource.upload_dataframe(
                        bucket="processed",
                        key=validated_track_status_key,
                        df=validated_df,
                    )

                    # Add upload status to the validation results
                    track_status_validation_results["upload_status"].append(
                        upload_status
                    )

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_track_status_key = f"silver/validation_results/track_status/track_status_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_track_status_key,
        df=pd.DataFrame(track_status_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )


@asset(
    name="raw_session_status_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the session_status files downloaded from API",
)
def f1_raw_session_status_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Checks and validates the raw race control session_status files ingested from the API.
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_session_status_processed": 0,
        "num_total_errors": 0,
        "skipped_for_unavailablity": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    years = [str(year) for year in range(2018, 2026)]

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    session_status_validation_results = {
        "year": [],
        "grand_prix": [],
        "session": [],
        "len_raw_session_status": [],
        "len_validated_session_status": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    for year in years:
        # Log message for individual years
        _logger.info("| | Validating %s", year)

        grand_prix_names = s3_resource.list_directories(bucket="raw", prefix=f"{year}")

        for grand_prix in grand_prix_names:
            # Log message for individual grands prix
            _logger.info("| | | | Validating %s %s", year, grand_prix)

            session_names = s3_resource.list_directories(
                bucket="raw", prefix=f"{year}/{grand_prix}"
            )

            for session in session_names:
                # Log message for individual session
                _logger.info(
                    "| | | | | | Validating race control session_status for %s %s %s",
                    year,
                    grand_prix,
                    session,
                )

                session_status_validation_results["year"].append(year)
                session_status_validation_results["grand_prix"].append(grand_prix)
                session_status_validation_results["session"].append(session)

                # Key to download raw session session_status data
                raw_session_status_key = (
                    f"{year}/{grand_prix}/{session}/session_status.parquet"
                )
                raw_session_status_df = s3_resource.download_dataframe(
                    bucket="raw",
                    key=raw_session_status_key,
                )

                # Add length of raw schedule
                if raw_session_status_df is None:
                    session_status_validation_results["len_raw_session_status"].append(
                        0
                    )

                    metadata["skipped_for_unavailablity"] += 1

                    session_status_validation_results[
                        "len_validated_session_status"
                    ].append(0)
                    session_status_validation_results["num_validation_errors"].append(0)
                    session_status_validation_results["upload_status"].append(False)

                else:
                    session_status_validation_results["len_raw_session_status"].append(
                        len(raw_session_status_df)
                    )

                    # Perform validation
                    validated_df, errors = validator.validate_session_status(
                        raw_session_status_df
                    )
                    validated_df["is_validated"] = True
                    validated_df["validation_timestamp"] = current_time
                    metadata["num_session_status_processed"] += 1

                    # Add length of validated session_status and number of errors
                    session_status_validation_results[
                        "len_validated_session_status"
                    ].append(len(validated_df))
                    session_status_validation_results["num_validation_errors"].append(
                        len(errors)
                    )
                    metadata["num_total_errors"] += len(errors)

                    # Key to upload validated session_status to the silver layer
                    validated_session_status_key = (
                        f"silver/{year}/{grand_prix}/{session}/session_status.parquet"
                    )
                    upload_status = s3_resource.upload_dataframe(
                        bucket="processed",
                        key=validated_session_status_key,
                        df=validated_df,
                    )

                    # Add upload status to the validation results
                    session_status_validation_results["upload_status"].append(
                        upload_status
                    )

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_session_status_key = f"silver/validation_results/session_status/session_status_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_session_status_key,
        df=pd.DataFrame(session_status_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )


@asset(
    name="raw_session_info_validation",
    group_name="raw_data_validation",
    compute_kind="validation",
    description="Validate the session_info files downloaded from API",
)
def f1_raw_session_info_validation(
    context: AssetExecutionContext,  # pylint: disable=unused-argument
    s3_resource: S3Resource,
) -> Output:
    """
    Checks and validates the raw race control session_info files ingested from the API.
    """

    # Initialize metadata for asset materialization
    metadata = {
        "num_session_info_processed": 0,
        "num_total_errors": 0,
        "skipped_for_unavailablity": 0,
    }

    current_time = datetime.now()

    # Log starting message
    _logger.info("| | Getting base level directories from the bucekt")

    # Get list of all directories in the raw data bucket
    years = [str(year) for year in range(2018, 2026)]

    # Initialize data validator
    validator = DataValidator()

    # Initialize validation results
    session_info_validation_results = {
        "year": [],
        "grand_prix": [],
        "session": [],
        "len_raw_session_info": [],
        "len_validated_session_info": [],
        "num_validation_errors": [],
        "upload_status": [],
    }

    for year in years:
        # Log message for individual years
        _logger.info("| | Validating %s", year)

        grand_prix_names = s3_resource.list_directories(bucket="raw", prefix=f"{year}")

        for grand_prix in grand_prix_names:
            # Log message for individual grands prix
            _logger.info("| | | | Validating %s %s", year, grand_prix)

            session_names = s3_resource.list_directories(
                bucket="raw", prefix=f"{year}/{grand_prix}"
            )

            for session in session_names:
                # Log message for individual session
                _logger.info(
                    "| | | | | | Validating race control session_info for %s %s %s",
                    year,
                    grand_prix,
                    session,
                )

                session_info_validation_results["year"].append(year)
                session_info_validation_results["grand_prix"].append(grand_prix)
                session_info_validation_results["session"].append(session)

                # Key to download raw session session_info data
                raw_session_info_key = (
                    f"{year}/{grand_prix}/{session}/session_info.parquet"
                )
                raw_session_info_df = s3_resource.download_dataframe(
                    bucket="raw",
                    key=raw_session_info_key,
                )

                # Add length of raw schedule
                if raw_session_info_df is None:
                    session_info_validation_results["len_raw_session_info"].append(0)

                    metadata["skipped_for_unavailablity"] += 1

                    session_info_validation_results[
                        "len_validated_session_info"
                    ].append(0)
                    session_info_validation_results["num_validation_errors"].append(0)
                    session_info_validation_results["upload_status"].append(False)

                else:
                    session_info_validation_results["len_raw_session_info"].append(
                        len(raw_session_info_df)
                    )

                    # Perform validation
                    validated_df, errors = validator.validate_session_info(
                        raw_session_info_df
                    )
                    validated_df["is_validated"] = True
                    validated_df["validation_timestamp"] = current_time
                    metadata["num_session_info_processed"] += 1

                    # Add length of validated session_info and number of errors
                    session_info_validation_results[
                        "len_validated_session_info"
                    ].append(len(validated_df))
                    session_info_validation_results["num_validation_errors"].append(
                        len(errors)
                    )
                    metadata["num_total_errors"] += len(errors)

                    # Key to upload validated session_info to the silver layer
                    validated_session_info_key = (
                        f"silver/{year}/{grand_prix}/{session}/session_info.parquet"
                    )
                    upload_status = s3_resource.upload_dataframe(
                        bucket="processed",
                        key=validated_session_info_key,
                        df=validated_df,
                    )

                    # Add upload status to the validation results
                    session_info_validation_results["upload_status"].append(
                        upload_status
                    )

    time_suffix = current_time.strftime("%Y%m%d_%H%M%S")
    validation_session_info_key = f"silver/validation_results/session_info/session_info_validation_{time_suffix}.parquet"  # pylint: disable=line-too-long
    s3_resource.upload_dataframe(
        bucket="processed",
        key=validation_session_info_key,
        df=pd.DataFrame(session_info_validation_results),
    )

    return Output(
        value=metadata,
        metadata=metadata,
    )
