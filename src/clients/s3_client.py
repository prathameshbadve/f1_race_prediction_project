"""
S3 Client for use outside dagster
"""

import json
from io import BytesIO
from typing import Optional, List

import pandas as pd
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from config.settings import StorageConfig
from config.logging import get_logger


class S3Client(StorageConfig):
    """Client to interact with the bucket storage"""

    _logger = get_logger("s3_client")

    @classmethod
    def from_env(cls):
        """Factiry to create S3 client from env variables"""

        return cls(config=StorageConfig.from_env())

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

    def _list_buckets(self) -> List[str]:
        """Returns the list of bucket names on the bucket server"""

        client = self.get_client()

        response = client.list_buckets()

        buckets = [x["Name"] for x in response["Buckets"]]

        return buckets

    def bucket_exists(self, bucket: str) -> bool:
        """Returns True/False based on if the bucket exists or not"""

        buckets = self._list_buckets()

        if bucket in buckets:
            self._logger.info("| | Bucket %s exists", bucket)
            return True

        self._logger.warning("| | Bucket %s does not exist", bucket)
        return False

    def download_dataframe(
        self,
        bucket: str,
        key: str,
    ) -> Optional[pd.DataFrame]:
        """
        Downloads pandas DataFrame from the bucket

        Args:
            bucket: "raw", "processed" or "custom_bucket_name: Should be already created."
            key: Storage path (e.g., "2024/round_01/race_results.parquet")

        Returns:
            df: pandas dataframe to store
        """

        # Fetch storage config for bucket names
        config = self.get_config()

        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        # Check if the bucket exists, if it does not return False
        if not self.bucket_exists(bucket=bucket_name):
            self._logger.error(
                "| | Bucket %s does not exist. Can't download %s",
                bucket_name,
                key,
            )
            return False

        # Bucket exists, so fetch client and attempt upload
        self._logger.debug("| | Bucket %s exists", bucket_name)
        client = self.get_client()

        # Extract file format from the object key
        file_format = key.split(".")[-1]
        self._logger.debug("| | Identified file format = %s", file_format)

        try:
            # Download object
            response = client.get_object(
                Bucket=bucket_name,
                Key=key,
            )

            if response.get("ContentLength", 0) == 0:
                self._logger.warning("| | Empty file fetched from %s", key)
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
                self._logger.error("| | Unsupported file format: %s", file_format)
                return None

            # Automatically find all timedelta columns and replace NaT with None
            timedelta_cols = df.select_dtypes(include=["timedelta64"]).columns
            for col in timedelta_cols:
                df[col] = df[col].replace({pd.NaT: None})

            self._logger.info(
                "| | Successfully downloaded dataframe containing %d rows from %s",
                len(df),
                key,
            )
            return df

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                self._logger.error("| | No file with key exists: %s", key)
            else:
                self._logger.error("| | ❌ Failed to download %s: %s", key, str(e))
            return None

        except Exception as e:  # pylint: disable=broad-except
            self._logger.error("| | ❌ Failed to download %s: %s", key, str(e))
            print(e)
            return None

        finally:
            if "response" in locals():
                response["Body"].close()

    def upload_dataframe(
        self,
        bucket: str,
        key: str,
        df: pd.DataFrame,
    ) -> bool:
        """
        Upload a pandas DataFrame to storage bucket.

        Args:
            bucket: "raw", "processed" or "custom_bucket_name: Should be already created."
            key: Storage path (e.g., "2024/round_01/race_results.csv")
            df: pandas dataframe to store

        Returns:
            True if successful, False otherwise
        """

        # Check if the df is none or empty
        if df is None or df.empty:
            self._logger.warning("| | Dataframe is empty and cannot be uploaded.")
            return False

        # Fetch storage config for bucket names
        config = self.get_config()

        # Set bucket name based on the argument passed to bucket
        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        # Check if the bucket exists, if it does not return False
        if not self.bucket_exists(bucket=bucket_name):
            self._logger.error(
                "| | Bucket %s does not exist. Can't upload %s",
                bucket_name,
                key,
            )
            return False

        # Bucket exists, so fetch client and attempt upload
        self._logger.debug("| | Bucket %s exists", bucket_name)
        client = self.get_client()

        # Extract file format from the object key
        file_format = key.split(".")[-1]
        self._logger.debug("| | Identified file format = %s", file_format)

        # Block to handle CSV upload
        if file_format == "csv":
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
                self._logger.info(
                    "| | Dataframe with %d rows uploaded as CSV successfully: %s",
                    len(df),
                    key,
                )
                return True

            except Exception as e:  # pylint: disable=broad-exception-caught
                self._logger.error(
                    "| | Failed to upload dataframe to %s: %s", key, str(e)
                )
                return False

        # Block to handle Parquet upload
        elif file_format == "parquet":
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
                self._logger.info(
                    "| | Dataframe with %d rows uploaded as Parquet successfully: %s",
                    len(df),
                    key,
                )
                return True

            except Exception as e:  # pylint: disable=broad-exception-caught
                self._logger.error(
                    "| | Failed to upload dataframe to %s: %s", key, str(e)
                )
                return False

        # Log error for unsupported file format
        else:
            self._logger.error("| | Unsupported file format: %s", file_format)

    def upload_file(self, bucket: str, key: str, json_data) -> bool:
        """Upload file to storage bucket"""

        # Fetch storage config for bucket names
        config = self.get_config()

        # Set bucket name based on the argument passed to bucket
        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        # Check if the bucket exists, if it does not return False
        if not self.bucket_exists(bucket=bucket_name):
            self._logger.error(
                "| | Bucket %s does not exist. Can't upload %s",
                bucket_name,
                key,
            )
            return False

        # Bucket exists, so fetch client and attempt upload
        self._logger.debug("| | Bucket %s exists", bucket_name)
        client = self.get_client()

        # Extract file format from the object key
        file_format = key.split(".")[-1]
        self._logger.debug("| | Identified file format = %s", file_format)

        if file_format == "json":
            try:
                client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=json_data,
                    ContentType="application/json",
                )

            except Exception as e:  # pylint: disable=broad-except
                self._logger.error("| | Failed to upload file to %s: %s", key, str(e))
                return False

    def download_file(self, bucket: str, key: str):
        """Downloads file from bucket"""

        # Fetch storage config for bucket names
        config = self.get_config()

        # Set bucket name based on the argument passed to bucket
        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        # Check if the bucket exists, if it does not return False
        if not self.bucket_exists(bucket=bucket_name):
            self._logger.error(
                "| | Bucket %s does not exist. Can't upload %s",
                bucket_name,
                key,
            )
            return False

        # Bucket exists, so fetch client and attempt upload
        self._logger.debug("| | Bucket %s exists", bucket_name)
        client = self.get_client()

        # Extract file format from the object key
        file_format = key.split(".")[-1]
        self._logger.debug("| | Identified file format = %s", file_format)

        if file_format == "json":
            try:
                response = client.get_object(Bucket=bucket_name, Key=key)
                data = response["Body"].read().decode("utf-8")
                return json.loads(data)

            except Exception as e:  # pylint: disable=broad-except
                self._logger.error("| | Failed to upload file to %s: %s", key, str(e))
                return None

    def file_exists(self, bucket: str, key: str):
        """Checks and returns boolean depending on if the file exists or not"""

        # Fetch storage config for bucket names
        config = self.get_config()

        # Set bucket name based on the argument passed to bucket
        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        # Check if the bucket exists, if it does not return False
        if not self.bucket_exists(bucket=bucket_name):
            self._logger.error(
                "| | Bucket %s does not exist. Can't upload %s",
                bucket_name,
                key,
            )
            return False

        # Bucket exists, so fetch client and attempt upload
        self._logger.debug("| | Bucket %s exists", bucket_name)
        client = self.get_client()

        try:
            response = client.head_object(
                Bucket=bucket_name,
                Key=key,
            )
            self._logger.info(
                "| | File exists at %s, size=%d bytes", key, response["ContentLength"]
            )
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self._logger.warning("| | File does not exist %s: %s", key, str(e))
            else:
                self._logger.error("| | ❌ Error checking file %s: %s", key, str(e))
            return False

    def list_directories(
        self,
        bucket: str,
        prefix: str,
    ) -> Optional[List[str]]:
        """
        Returns the list of first level directories in the bucket (bucket) with prefix (prefix)

        Args:
            bucket: "raw", "processed" or "custom_bucket_name: Should be already created."

        Returns:
            List[str]: e.g. ['2023/', '2024/']
        """

        # Fetch storage config for bucket names
        config = self.get_config()

        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        # Check if the bucket exists, if it does not return False
        if not self.bucket_exists(bucket=bucket_name):
            self._logger.error(
                "| | Bucket %s does not exist. Cannot list objects",
                bucket_name,
            )
            return None

        # Bucket exists, so fetch client and attempt upload
        self._logger.debug("| | Bucket %s exists", bucket_name)
        client = self.get_client()

        if prefix == "":
            pass
        elif not prefix.endswith("/"):
            prefix += "/"

        try:
            paginator = client.get_paginator("list_objects_v2")
            result = []

            for page in paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter="/",  # this groups "directories"
            ):
                for cp in page.get("CommonPrefixes", []):
                    full = cp["Prefix"]
                    # Strip the full prefix and trailing slash to get only the child name
                    name = full[len(prefix) :].strip("/")
                    result.append(name)

            return result

        except Exception as e:  # pylint: disable=broad-except
            self._logger.error("| | Error while getting directories %s", str(e))
            return None

    def list_files(
        self,
        bucket: str,
        prefix: str = "",
    ) -> Optional[List[str]]:
        """
        Returns list of files in the bucket with given prefix

        Args:
            bucket: "raw", "processed"
            prefix: str, e.g. "2024/Italian Grand Prix/Race"

        Returns:
            List[str]: e.g. ["results.parquet", "laps.parquet"]
        """

        # Fetch storage config for bucket names
        config = self.get_config()

        if bucket == "raw":
            bucket_name = config.raw_data_bucket
        elif bucket == "processed":
            bucket_name = config.processed_data_bucket
        else:
            bucket_name = bucket

        # Check if the bucket exists, if it does not return False
        if not self.bucket_exists(bucket=bucket_name):
            self._logger.error(
                "| | Bucket %s does not exist. Cannot list objects",
                bucket_name,
            )
            return None

        # Bucket exists, so fetch client and attempt upload
        self._logger.debug("| | Bucket %s exists", bucket_name)
        client = self.get_client()

        if prefix == "":
            pass
        elif not prefix.endswith("/"):
            prefix += "/"

        try:
            paginator = client.get_paginator("list_objects_v2")
            result = []

            for page in paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter="/",  # this groups "directories"
            ):
                for content in page.get("Contents", []):
                    full = content["Key"]
                    # Strip the full prefix and trailing slash to get only the child name
                    name = full[len(prefix) :].strip("/")
                    result.append(name)

            return result

        except Exception as e:  # pylint: disable=broad-except
            self._logger.error("| | Error while getting files %s", str(e))
            return None
