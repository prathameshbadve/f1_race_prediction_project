"""
Dagster definitions
"""

from dagster import Definitions
from dagster_code.assets.f1_ingestion import f1_session_results
from dagster_code.resources import S3Resource, FastF1Resource

from config.logging import setup_logging

setup_logging()

defs = Definitions(
    assets=[f1_session_results],
    resources={
        "fastf1_resource": FastF1Resource.from_env(),
        "s3_resource": S3Resource.from_env(),
    },
)
