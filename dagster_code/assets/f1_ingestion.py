"""
Dagster assets for ingestion from FastF1 API
"""

from dagster import asset, Config, Output
from dagster_code.resources.fastf1_resource import FastF1Resource
from dagster_code.resources.s3_resource import S3Resource

from config.logging import get_logger

_logger = get_logger("data_ingestion.test_ingestion")


class F1SessionConfig(Config):
    """Configuration for F1 session data ingestion"""

    year: int
    grand_prix_name: str  # e.g., "Monaco Grand Prix"
    session_name: str  # e.g., "Race", "Qualifying", "Sprint"


@asset(compute_kind="fastf1")
def f1_session_results(
    config: F1SessionConfig,
    fastf1_resource: FastF1Resource,
    s3_resource: S3Resource,
) -> Output:
    """
    Ingest F1 session results and upload to S3 as parquet

    Uploads to: s3://f1-data-raw/{year}/{grand_prix_name}/{session_name}/results.parquet
    """

    # Load session
    _logger.info(
        "Loading session: %d %s %s",
        config.year,
        config.grand_prix_name,
        config.session_name,
    )
    session = fastf1_resource.get_session(
        config.year, config.grand_prix_name, config.session_name
    )
    session.load(
        laps=False,
        messages=False,
        weather=False,
        telemetry=False,
    )

    # Get results
    results = session.results

    # Define S3 path
    s3_key = (
        f"{config.year}/{config.grand_prix_name}/{config.session_name}/results.parquet"
    )

    # Upload to S3
    status = s3_resource.upload_dataframe_as_parquet(
        bucket="f1-data-raw",
        key=s3_key,
        df=results,
    )

    _logger.info(
        "Uploaded %d results to s3://f1-data-raw/%s",
        len(results),
        s3_key,
    )

    metadata = {
        "year": config.year,
        "grand_prix_name": config.grand_prix_name,
        "session_name": config.session_name,
        "successful": status,
    }

    if len(results) <= 20:
        metadata["results_summary"] = results.to_markdown(index=False)

    return Output(
        value={
            "year": config.year,
            "grand_prix_name": config.grand_prix_name,
            "session_name": config.session_name,
            "successful": status,
        },
        metadata=metadata,
    )
