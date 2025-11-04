"""
Dagter resource for S3/Minio bucket storage
"""

from io import BytesIO

import pandas as pd
from dagster import ConfigurableResource
import boto3
from botocore.client import Config


class S3Resource(ConfigurableResource):
    """Resource for interacting with S3"""

    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    region_name: str = "ap-south-1"
    endpoint_url: str = ""

    def get_client(self):
        """Get boto3 S3 client"""

        kwargs = {
            "service_name": "s3",
            "region_name": self.region_name,
            "config": Config(signature_version="s3v4"),
        }

        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url

        return boto3.client(**kwargs)

    def upload_dataframe_as_parquet(
        self, bucket: str, key: str, df: pd.DataFrame
    ) -> bool:
        """
        Upload a pandas DataFrame as a Parquet file.

        Args:
            df: DataFrame to upload
            object_key: Storage path (e.g., "2024/round_01/race_results.parquet")
            compression: Parquet compression method (snappy, gzip, brotli, none)

        Returns:
            True if successful, False otherwise
        """

        client = self.get_client()

        if df is None or df.empty:
            return False

        try:
            # Convert DataFrame to Parquet in memory
            buffer = BytesIO()
            df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)

            # Get size for logging
            buffer.seek(0)

            # Upload to MinIO
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer,
            )
            return True

        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error: {str(e)}")
            return False
