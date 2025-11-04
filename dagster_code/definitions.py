"""
Dagster definitions
"""

from dagster import Definitions, EnvVar
from dagster_code.assets.f1_ingestion import f1_session_results
from dagster_code.resources import FastF1Resource, S3Resource


defs = Definitions(
    assets=[f1_session_results],
    resources={
        "fastf1_resource": FastF1Resource(),
        "s3_resource": S3Resource(
            aws_access_key_id=EnvVar("MINIO_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("MINIO_SECRET_KEY"),
            endpoint_url=EnvVar("MINIO_ENDPOINT"),
        ),
    },
)
