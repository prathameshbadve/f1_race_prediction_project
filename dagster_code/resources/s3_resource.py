"""
Dagter resource for S3/Minio bucket storage
"""

import logging
from io import BytesIO
from typing import Optional

import pandas as pd
from dagster import ConfigurableResource
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from config.settings import StorageConfig
from config.logging import get_logger


class S3Resource(ConfigurableResource):
    """Resource for interacting with S3"""

    _logger: logging.Logger = get_logger("dagster.s3_resource")

    def get_config(self) -> StorageConfig:
        """Get fully loaded pydantic configuration from env variables"""

        return StorageConfig.from_env()

    def get_client(self):
        """
        Get boto3 S3 client
        Called automatically by Dagster before job execution.
        """

        config = self.get_config()

        kwargs = {
            "service_name": "s3",
            "endpoint_url": config.endpoint,
            "aws_access_key_id": config.access_key,
            "aws_secret_access_key": config.secret_key,
            "region_name": config.region,
            "config": Config(signature_version="s3v4"),
        }

        return boto3.client(**kwargs)

    @classmethod
    def from_env(cls):
        """Factory to create S3Resource from environment variables."""

        return cls(config=StorageConfig.from_env())

    def upload_dataframe_as_csv(
        self,
        bucket: str,
        key: str,
        df: pd.DataFrame,
    ) -> bool:
        """
        Upload a pandas DataFrame as a CSV file.

        Args:
            bucket: "raw", "processed" or "custom_bucket_name: Should be already created."
            key: Storage path (e.g., "2024/round_01/race_results.csv")
            df: pandas dataframe to store

        Returns:
            True if successful, False otherwise
        """

        if df is None or df.empty:
            return False

        config = self.get_config()
        client = self.get_client()

        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        try:
            # Convert DataFrame to Parquet in memory
            buffer = BytesIO()
            df.to_csv(
                buffer,
                index=False,
            )

            # Get size for logging
            buffer.seek(0)

            # Upload to MinIO
            client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=buffer,
            )
            return True

        except Exception as e:  # pylint: disable=broad-exception-caught
            print(e)
            return False

    def upload_dataframe_as_parquet(
        self,
        bucket: str,
        key: str,
        df: pd.DataFrame,
    ) -> bool:
        """
        Upload a pandas DataFrame as a Parquet file.

        Args:
            bucket: "raw", "processed" or "custom_bucket_name: Should be already created."
            key: Storage path (e.g., "2024/round_01/race_results.parquet")
            df: pandas dataframe to store

        Returns:
            True if successful, False otherwise
        """

        if df is None or df.empty:
            return False

        config = self.get_config()
        client = self.get_client()

        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        try:
            # Convert DataFrame to Parquet in memory
            buffer = BytesIO()
            df.to_parquet(
                buffer,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )

            # Get size for logging
            buffer.seek(0)

            # Upload to MinIO
            client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=buffer,
            )
            return True

        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error: {str(e)}")
            return False

    def download_dataframe(
        self,
        bucket: str,
        key: str,
    ) -> Optional[pd.DataFrame]:
        """
        Downloads a pandas DataFrame from the bucket

        Args:
            bucket: "raw", "processed" or "custom_bucket_name: Should be already created."
            key: Storage path (e.g., "2024/round_01/race_results.parquet")

        Returns:
            df: pandas dataframe to store
        """

        client = self.get_client()

        file_format = key.split(".")[-1]

        try:
            # Download object
            response = client.get_object(
                Bucket=bucket,
                Key=key,
            )

            if response.get("ContentLength", 0) == 0:
                # todo: add log message
                return None

            # Read Parquet from bytes
            buffer = BytesIO(response["Body"].read())

            if file_format == "parquet":
                df = pd.read_parquet(buffer, engine="pyarrow")
            elif file_format == "csv":
                df = pd.read_csv(buffer)
            elif file_format == "json":
                df = pd.read_json(buffer)
            elif file_format in {"pkl", "pickle"}:
                df = pd.read_pickle(buffer)
            else:
                # todo: add log message
                return None

            # Automatically find all timedelta columns and replace NaT with None
            timedelta_cols = df.select_dtypes(include=["timedelta64"]).columns
            for col in timedelta_cols:
                df[col] = df[col].replace({pd.NaT: None})

            # todo: add log message
            return df

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                # todo: add log message
                print(e)
                # self.logger.warning("| | | | | | Object not found: %s", object_key)
            else:
                # todo: add log message
                print(e)
                # self.logger.error(
                #     "| | | | | | ❌ Failed to download %s: %s", object_key, e
                # )
            return None

        except Exception as e:  # pylint: disable=broad-except
            # todo: add log message
            print(e)
            return None

        finally:
            if "response" in locals():
                response["Body"].close()
