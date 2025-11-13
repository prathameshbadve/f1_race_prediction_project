"""
Assets for the ingesting complete F1 seasons
"""

from typing import Dict, Any, List

from dagster import asset, Output, AssetExecutionContext, Config
from dagster_code.resources import FastF1Resource, S3Resource
from dagster_code.partitions import f1_season_partitions

from config.logging import get_logger

_logger = get_logger("data_ingestion.main")


# Ingest season schedules
@asset(
    name="raw_season_schedule",
    group_name="raw_season_data",
    compute_kind="fastf1",
    partitions_def=f1_season_partitions,
    description="Ingest the season schedule for an F1 season",
)
def f1_season_schedule(
    context: AssetExecutionContext,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest the schedule for a F1 season and upload to bucket storage as parquet.

    Uploads to: s3://f1-data-raw/schedules/season_{year}_schedule.parquet
    """

    year = context.partition_key

    # Metadata for output
    metadata = {
        "asset_params": {"year": year},
    }

    # Logger entry
    _logger.info(
        "| | Loading season schedule for %d",
        year,
    )

    # Build object key
    object_key = f"{year}/season_schedule.parquet"

    # Download the season schedule from bucket storage if it exists
    if s3_resource.file_exists(bucket="raw", key=object_key):
        _logger.info("| | File exists. Downloading: %s", object_key)
        season_schedule = s3_resource.download_dataframe(
            bucket="raw",
            key=object_key,
        )
        metadata["file_exists"] = True

    else:
        # File does not exist so no return and function moves to downloading data from API.
        _logger.debug("| | File does not exist, so loading from API")
        metadata["file_exists"] = False

        # Load schedule data from API
        season_schedule = fastf1_resource.get_season_schedule(int(year))

        # Save file to storage
        upload_status = s3_resource.upload_dataframe(
            bucket="raw",
            key=object_key,
            df=season_schedule,
        )
        metadata["upload_status"] = upload_status

    # Sample schedule to show in dagster UI
    season_sample = season_schedule.head(5)
    metadata["schedule_sample"] = season_sample.to_markdown()

    return Output(
        value={"year": year, "sample_file": season_sample.to_markdown()},
        metadata=metadata,
    )


class F1SessionConfig(Config):
    """Configuration for F1 session data ingestion"""

    year: int
    grand_prix_name: List[str]  # e.g., "Monaco Grand Prix"
    session_name: List[str] = ["Race", "Qualifying"]


@asset(
    name="raw_session_results",
    group_name="raw_session_data",
    compute_kind="fastf1",
    description="Ingest raw results of F1 sessions",
)
def f1_session_results_configurable(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 session results and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/results.parquet
    """

    year = config.year
    grand_prix_names = config.grand_prix_name
    session_names = config.session_name

    # Metadata for output
    metadata = {
        "asset_params": {
            "year": year,
            "grand_prix_names": grand_prix_names,
            "session_names": session_names,
        },
    }

    if len(grand_prix_names) == 0:
        grand_prix_names = fastf1_resource.get_grands_prix_from_season(year=year)

    # Update metadata
    metadata = {
        **metadata,
        "total_grand_prix": len(grand_prix_names),
        "total_session_types": len(session_names),
        "total_num_sessions": len(grand_prix_names) * len(session_names),
        "success": 0,
        "failure": 0,
        "new_files_uploaded": 0,
        "existing_files_used": 0,
    }

    # Logger entry
    _logger.info(
        "| | Loading session results for total %d sessions",
        len(grand_prix_names) * len(session_names),
    )

    for grand_prix_name in grand_prix_names:
        for session_name in session_names:
            object_key = f"{year}/{grand_prix_name}/{session_name}/results.parquet"

            try:
                # Download the session results from bucket storage if it exists
                if s3_resource.file_exists(bucket="raw", key=object_key):
                    _logger.info("| | File exists. Downloading: %s", object_key)
                    session_results = s3_resource.download_dataframe(
                        bucket="raw",
                        key=object_key,
                    )
                    metadata["existing_files_used"] += 1

                else:
                    # File does not exist so function downloads data from API
                    _logger.debug("| | File does not exist, so loading from API")

                    # Load results data from API
                    session_results = fastf1_resource.get_session_results(
                        year=year,
                        event=grand_prix_name,
                        session=session_name,
                    )

                    # Save file to storage
                    upload_status = s3_resource.upload_dataframe(
                        bucket="raw",
                        key=object_key,
                        df=session_results,
                    )

                    if upload_status:
                        metadata["new_files_uploaded"] += 1

                metadata["success"] += 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "| | Error while trying to load session results for %d %s %s: %s",
                    year,
                    grand_prix_name,
                    session_name,
                    str(e),
                )
                metadata["failure"] += 1

    return Output(
        value={"year": year},
        metadata=metadata,
    )


@asset(
    name="raw_session_laps",
    group_name="raw_session_data",
    compute_kind="fastf1",
    description="Ingest raw laps data of F1 sessions",
)
def f1_session_laps_configurable(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 session laps data and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/laps.parquet
    """

    year = config.year
    grand_prix_names = config.grand_prix_name
    session_names = config.session_name

    # Metadata for output
    metadata = {
        "asset_params": {
            "year": year,
            "grand_prix_names": grand_prix_names,
            "session_names": session_names,
        },
    }

    if len(grand_prix_names) == 0:
        grand_prix_names = fastf1_resource.get_grands_prix_from_season(year=year)

    # Update metadata
    metadata = {
        **metadata,
        "total_grand_prix": len(grand_prix_names),
        "total_session_types": len(session_names),
        "total_num_sessions": len(grand_prix_names) * len(session_names),
        "success": 0,
        "failure": 0,
        "new_files_uploaded": 0,
        "existing_files_used": 0,
    }

    # Logger entry
    _logger.info(
        "| | Loading session laps data for total %d sessions",
        len(grand_prix_names) * len(session_names),
    )

    for grand_prix_name in grand_prix_names:
        for session_name in session_names:
            object_key = f"{year}/{grand_prix_name}/{session_name}/laps.parquet"

            try:
                # Download the session laps data from bucket storage if it exists
                if s3_resource.file_exists(bucket="raw", key=object_key):
                    _logger.info("| | File exists. Downloading: %s", object_key)
                    session_results = s3_resource.download_dataframe(
                        bucket="raw",
                        key=object_key,
                    )
                    metadata["existing_files_used"] += 1

                else:
                    # File does not exist so function downloads data from API
                    _logger.debug("| | File does not exist, so loading from API")

                    # Load laps data from API
                    session_results = fastf1_resource.get_session_laps(
                        year=year,
                        event=grand_prix_name,
                        session=session_name,
                    )

                    # Save file to storage
                    upload_status = s3_resource.upload_dataframe(
                        bucket="raw",
                        key=object_key,
                        df=session_results,
                    )

                    if upload_status:
                        metadata["new_files_uploaded"] += 1

                metadata["success"] += 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "| | Error while trying to load session laps data for %d %s %s: %s",
                    year,
                    grand_prix_name,
                    session_name,
                    str(e),
                )
                metadata["failure"] += 1

    return Output(
        value={"year": year},
        metadata=metadata,
    )


@asset(
    name="raw_session_weather",
    group_name="raw_session_data",
    compute_kind="fastf1",
    description="Ingest raw weather data of F1 sessions",
)
def f1_session_weather_configurable(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 session weather data and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/weather.parquet
    """

    year = config.year
    grand_prix_names = config.grand_prix_name
    session_names = config.session_name

    # Metadata for output
    metadata = {
        "asset_params": {
            "year": year,
            "grand_prix_names": grand_prix_names,
            "session_names": session_names,
        },
    }

    if len(grand_prix_names) == 0:
        grand_prix_names = fastf1_resource.get_grands_prix_from_season(year=year)

    # Update metadata
    metadata = {
        **metadata,
        "total_grand_prix": len(grand_prix_names),
        "total_session_types": len(session_names),
        "total_num_sessions": len(grand_prix_names) * len(session_names),
        "success": 0,
        "failure": 0,
        "new_files_uploaded": 0,
        "existing_files_used": 0,
    }

    # Logger entry
    _logger.info(
        "| | Loading session weather data for total %d sessions",
        len(grand_prix_names) * len(session_names),
    )

    for grand_prix_name in grand_prix_names:
        for session_name in session_names:
            object_key = f"{year}/{grand_prix_name}/{session_name}/weather.parquet"

            try:
                # Download the session weather data from bucket storage if it exists
                if s3_resource.file_exists(bucket="raw", key=object_key):
                    _logger.info("| | File exists. Downloading: %s", object_key)
                    session_results = s3_resource.download_dataframe(
                        bucket="raw",
                        key=object_key,
                    )
                    metadata["existing_files_used"] += 1

                else:
                    # File does not exist so function downloads data from API
                    _logger.debug("| | File does not exist, so loading from API")

                    # Load weather data from API
                    session_results = fastf1_resource.get_session_weather(
                        year=year,
                        event=grand_prix_name,
                        session=session_name,
                    )

                    # Save file to storage
                    upload_status = s3_resource.upload_dataframe(
                        bucket="raw",
                        key=object_key,
                        df=session_results,
                    )

                    if upload_status:
                        metadata["new_files_uploaded"] += 1

                metadata["success"] += 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "| | Error while trying to load session weather data for %d %s %s: %s",
                    year,
                    grand_prix_name,
                    session_name,
                    str(e),
                )
                metadata["failure"] += 1

    return Output(
        value={"year": year},
        metadata=metadata,
    )


@asset(
    name="raw_session_race_control_messages",
    group_name="raw_session_data",
    compute_kind="fastf1",
    description="Ingest raw race control messages data of F1 sessions",
)
def f1_session_race_control_messages_configurable(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 session race control messages data and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/messages.parquet
    """

    year = config.year
    grand_prix_names = config.grand_prix_name
    session_names = config.session_name

    # Metadata for output
    metadata = {
        "asset_params": {
            "year": year,
            "grand_prix_names": grand_prix_names,
            "session_names": session_names,
        },
    }

    if len(grand_prix_names) == 0:
        grand_prix_names = fastf1_resource.get_grands_prix_from_season(year=year)

    # Update metadata
    metadata = {
        **metadata,
        "total_grand_prix": len(grand_prix_names),
        "total_session_types": len(session_names),
        "total_num_sessions": len(grand_prix_names) * len(session_names),
        "success": 0,
        "failure": 0,
        "new_files_uploaded": 0,
        "existing_files_used": 0,
    }

    # Logger entry
    _logger.info(
        "| | Loading session race control messages data for total %d sessions",
        len(grand_prix_names) * len(session_names),
    )

    for grand_prix_name in grand_prix_names:
        for session_name in session_names:
            object_key = f"{year}/{grand_prix_name}/{session_name}/messages.parquet"

            try:
                # Download the session race control messages from bucket storage if it exists
                if s3_resource.file_exists(bucket="raw", key=object_key):
                    _logger.info("| | File exists. Downloading: %s", object_key)
                    session_results = s3_resource.download_dataframe(
                        bucket="raw",
                        key=object_key,
                    )
                    metadata["existing_files_used"] += 1

                else:
                    # File does not exist so function downloads data from API
                    _logger.debug("| | File does not exist, so loading from API")

                    # Load race control messages data from API
                    session_results = fastf1_resource.get_session_race_control_messages(
                        year=year,
                        event=grand_prix_name,
                        session=session_name,
                    )

                    # Save file to storage
                    upload_status = s3_resource.upload_dataframe(
                        bucket="raw",
                        key=object_key,
                        df=session_results,
                    )

                    if upload_status:
                        metadata["new_files_uploaded"] += 1

                metadata["success"] += 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "| | Error while trying to load session race "
                    "control messages data for %d %s %s: %s",
                    year,
                    grand_prix_name,
                    session_name,
                    str(e),
                )
                metadata["failure"] += 1

    return Output(
        value={"year": year},
        metadata=metadata,
    )


@asset(
    name="raw_session_track_status",
    group_name="raw_session_data",
    compute_kind="fastf1",
    description="Ingest raw track status data of F1 sessions",
)
def f1_session_track_status_configurable(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 session track status data and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/track_status.parquet
    """

    year = config.year
    grand_prix_names = config.grand_prix_name
    session_names = config.session_name

    # Metadata for output
    metadata = {
        "asset_params": {
            "year": year,
            "grand_prix_names": grand_prix_names,
            "session_names": session_names,
        },
    }

    if len(grand_prix_names) == 0:
        grand_prix_names = fastf1_resource.get_grands_prix_from_season(year=year)

    # Update metadata
    metadata = {
        **metadata,
        "total_grand_prix": len(grand_prix_names),
        "total_session_types": len(session_names),
        "total_num_sessions": len(grand_prix_names) * len(session_names),
        "success": 0,
        "failure": 0,
        "new_files_uploaded": 0,
        "existing_files_used": 0,
    }

    # Logger entry
    _logger.info(
        "| | Loading session track status data for total %d sessions",
        len(grand_prix_names) * len(session_names),
    )

    for grand_prix_name in grand_prix_names:
        for session_name in session_names:
            object_key = f"{year}/{grand_prix_name}/{session_name}/track_status.parquet"

            try:
                # Download the session track status from bucket storage if it exists
                if s3_resource.file_exists(bucket="raw", key=object_key):
                    _logger.info("| | File exists. Downloading: %s", object_key)
                    session_results = s3_resource.download_dataframe(
                        bucket="raw",
                        key=object_key,
                    )
                    metadata["existing_files_used"] += 1

                else:
                    # File does not exist so function downloads data from API
                    _logger.debug("| | File does not exist, so loading from API")

                    # Load track status data from API
                    session_results = fastf1_resource.get_session_track_status(
                        year=year,
                        event=grand_prix_name,
                        session=session_name,
                    )

                    # Save file to storage
                    upload_status = s3_resource.upload_dataframe(
                        bucket="raw",
                        key=object_key,
                        df=session_results,
                    )

                    if upload_status:
                        metadata["new_files_uploaded"] += 1

                metadata["success"] += 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "| | Error while trying to load session track status data for %d %s %s: %s",
                    year,
                    grand_prix_name,
                    session_name,
                    str(e),
                )
                metadata["failure"] += 1

    return Output(
        value={"year": year},
        metadata=metadata,
    )


@asset(
    name="raw_session_session_status",
    group_name="raw_session_data",
    compute_kind="fastf1",
    description="Ingest raw session status data of F1 sessions",
)
def f1_session_status_configurable(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 session status data and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/session_status.parquet
    """

    year = config.year
    grand_prix_names = config.grand_prix_name
    session_names = config.session_name

    # Metadata for output
    metadata = {
        "asset_params": {
            "year": year,
            "grand_prix_names": grand_prix_names,
            "session_names": session_names,
        },
    }

    if len(grand_prix_names) == 0:
        grand_prix_names = fastf1_resource.get_grands_prix_from_season(year=year)

    # Update metadata
    metadata = {
        **metadata,
        "total_grand_prix": len(grand_prix_names),
        "total_session_types": len(session_names),
        "total_num_sessions": len(grand_prix_names) * len(session_names),
        "success": 0,
        "failure": 0,
        "new_files_uploaded": 0,
        "existing_files_used": 0,
    }

    # Logger entry
    _logger.info(
        "| | Loading session status data for total %d sessions",
        len(grand_prix_names) * len(session_names),
    )

    for grand_prix_name in grand_prix_names:
        for session_name in session_names:
            object_key = (
                f"{year}/{grand_prix_name}/{session_name}/session_status.parquet"
            )

            try:
                # Download the session status from bucket storage if it exists
                if s3_resource.file_exists(bucket="raw", key=object_key):
                    _logger.info("| | File exists. Downloading: %s", object_key)
                    session_results = s3_resource.download_dataframe(
                        bucket="raw",
                        key=object_key,
                    )
                    metadata["existing_files_used"] += 1

                else:
                    # File does not exist so function downloads data from API
                    _logger.debug("| | File does not exist, so loading from API")

                    # Load session status data from API
                    session_results = fastf1_resource.get_session_status(
                        year=year,
                        event=grand_prix_name,
                        session=session_name,
                    )

                    # Save file to storage
                    upload_status = s3_resource.upload_dataframe(
                        bucket="raw",
                        key=object_key,
                        df=session_results,
                    )

                    if upload_status:
                        metadata["new_files_uploaded"] += 1

                metadata["success"] += 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "| | Error while trying to load session status data for %d %s %s: %s",
                    year,
                    grand_prix_name,
                    session_name,
                    str(e),
                )
                metadata["failure"] += 1

    return Output(
        value={"year": year},
        metadata=metadata,
    )


@asset(
    name="raw_session_session_info",
    group_name="raw_session_data",
    compute_kind="fastf1",
    description="Ingest raw session info of F1 sessions",
)
def f1_session_session_info_configurable(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 session info and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/session_info.parquet
    """

    year = config.year
    grand_prix_names = config.grand_prix_name
    session_names = config.session_name

    # Metadata for output
    metadata = {
        "asset_params": {
            "year": year,
            "grand_prix_names": grand_prix_names,
            "session_names": session_names,
        },
    }

    if len(grand_prix_names) == 0:
        grand_prix_names = fastf1_resource.get_grands_prix_from_season(year=year)

    # Update metadata
    metadata = {
        **metadata,
        "total_grand_prix": len(grand_prix_names),
        "total_session_types": len(session_names),
        "total_num_sessions": len(grand_prix_names) * len(session_names),
        "success": 0,
        "failure": 0,
        "new_files_uploaded": 0,
        "existing_files_used": 0,
    }

    # Logger entry
    _logger.info(
        "| | Loading session info for total %d sessions",
        len(grand_prix_names) * len(session_names),
    )

    for grand_prix_name in grand_prix_names:
        for session_name in session_names:
            object_key = f"{year}/{grand_prix_name}/{session_name}/session_info.parquet"

            try:
                # Download the session info from bucket storage if it exists
                if s3_resource.file_exists(bucket="raw", key=object_key):
                    _logger.info("| | File exists. Downloading: %s", object_key)
                    session_results = s3_resource.download_dataframe(
                        bucket="raw",
                        key=object_key,
                    )
                    metadata["existing_files_used"] += 1

                else:
                    # File does not exist so function downloads data from API
                    _logger.debug("| | File does not exist, so loading from API")

                    # Load session info from API
                    session_results = fastf1_resource.get_session_info(
                        year=year,
                        event=grand_prix_name,
                        session=session_name,
                    )

                    # Save file to storage
                    upload_status = s3_resource.upload_dataframe(
                        bucket="raw",
                        key=object_key,
                        df=session_results,
                    )

                    if upload_status:
                        metadata["new_files_uploaded"] += 1

                metadata["success"] += 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(
                    "| | Error while trying to load session info for %d %s %s: %s",
                    year,
                    grand_prix_name,
                    session_name,
                    str(e),
                )
                metadata["failure"] += 1

    return Output(
        value={"year": year},
        metadata=metadata,
    )
