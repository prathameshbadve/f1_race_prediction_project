"""
Unit tests for dagster_code/resources/s3_resource.py
"""
# pylint: disable=unused-argument

from io import BytesIO
from unittest.mock import MagicMock, patch, Mock

import pytest
import pandas as pd
from botocore.exceptions import ClientError
from dagster_code.resources.s3_resource import S3Resource


class TestS3Resource:
    """Tests for S3Resource"""

    @pytest.fixture
    def s3_resource(self, mock_env_vars):
        """Create S3Resource instance"""

        return S3Resource()

    def test_get_config(self, s3_resource, mock_env_vars):
        """Test get_config returns StorageConfig"""

        config = s3_resource.get_config()
        assert config.endpoint == "http://localhost:9000"
        assert config.access_key == "test_user"
        assert config.secret_key == "test_password"

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_get_client(self, mock_boto_client, s3_resource):
        """Test get_client creates boto3 client with correct parameters"""

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        client = s3_resource.get_client()  # pylint: disable=unused-variable # noqa: F841

        mock_boto_client.assert_called_once()
        call_kwargs = mock_boto_client.call_args[1]
        assert call_kwargs["service_name"] == "s3"
        assert call_kwargs["endpoint_url"] == "http://localhost:9000"
        assert call_kwargs["aws_access_key_id"] == "test_user"
        assert call_kwargs["aws_secret_access_key"] == "test_password"

    def test_from_env(self, mock_env_vars):
        """Test from_env factory method"""

        resource = S3Resource.from_env()
        assert isinstance(resource, S3Resource)

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_upload_dataframe_as_csv_success(
        self, mock_boto_client, s3_resource, sample_dataframe
    ):
        """Test successful CSV upload"""

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        result = s3_resource.upload_dataframe_as_csv(
            bucket="raw",
            key="test/data.csv",
            df=sample_dataframe,
        )

        assert result is True
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-raw-bucket"
        assert call_kwargs["Key"] == "test/data.csv"

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_upload_dataframe_as_parquet_success(
        self, mock_boto_client, s3_resource, sample_dataframe
    ):
        """Test successful Parquet upload"""

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        result = s3_resource.upload_dataframe_as_parquet(
            bucket="processed",
            key="test/data.parquet",
            df=sample_dataframe,
        )

        assert result is True
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-processed-bucket"
        assert call_kwargs["Key"] == "test/data.parquet"

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_upload_dataframe_as_parquet_custom_bucket(
        self, mock_boto_client, s3_resource, sample_dataframe
    ):
        """Test Parquet upload to custom bucket"""

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        result = s3_resource.upload_dataframe_as_parquet(
            bucket="custom-bucket",
            key="test/data.parquet",
            df=sample_dataframe,
        )

        assert result is True
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "custom-bucket"

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_upload_empty_dataframe(self, mock_boto_client, s3_resource):
        """Test upload with empty DataFrame returns False"""

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        empty_df = pd.DataFrame()
        result = s3_resource.upload_dataframe_as_parquet(
            bucket="raw",
            key="test/data.parquet",
            df=empty_df,
        )

        assert result is False
        mock_client.put_object.assert_not_called()

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_upload_none_dataframe(self, mock_boto_client, s3_resource):
        """Test upload with None DataFrame returns False"""

        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        result = s3_resource.upload_dataframe_as_parquet(
            bucket="raw",
            key="test/data.parquet",
            df=None,
        )

        assert result is False
        mock_client.put_object.assert_not_called()

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_upload_dataframe_exception(
        self, mock_boto_client, s3_resource, sample_dataframe
    ):
        """Test upload handles exceptions gracefully"""

        mock_client = MagicMock()
        mock_client.put_object.side_effect = Exception("Upload failed")
        mock_boto_client.return_value = mock_client

        result = s3_resource.upload_dataframe_as_parquet(
            bucket="raw",
            key="test/data.parquet",
            df=sample_dataframe,
        )

        assert result is False

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_download_dataframe_parquet_success(
        self, mock_boto_client, s3_resource, sample_dataframe
    ):
        """Test successful Parquet download"""

        mock_client = MagicMock()

        # Create buffer with parquet data
        buffer = BytesIO()
        sample_dataframe.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        mock_response = {
            "Body": Mock(read=Mock(return_value=buffer.getvalue())),
            "ContentLength": len(buffer.getvalue()),
        }
        mock_client.get_object.return_value = mock_response
        mock_boto_client.return_value = mock_client

        df = s3_resource.download_dataframe(
            bucket="raw",
            key="test/data.parquet",
        )

        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(sample_dataframe)
        assert list(df.columns) == list(sample_dataframe.columns)

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_download_dataframe_csv_success(
        self, mock_boto_client, s3_resource, sample_dataframe
    ):
        """Test successful CSV download"""

        mock_client = MagicMock()

        # Create buffer with CSV data
        buffer = BytesIO()
        sample_dataframe.to_csv(buffer, index=False)
        buffer.seek(0)

        mock_response = {
            "Body": Mock(read=Mock(return_value=buffer.getvalue())),
            "ContentLength": len(buffer.getvalue()),
        }
        mock_client.get_object.return_value = mock_response
        mock_boto_client.return_value = mock_client

        df = s3_resource.download_dataframe(
            bucket="raw",
            key="test/data.csv",
        )

        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(sample_dataframe)

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_download_dataframe_not_found(self, mock_boto_client, s3_resource):
        """Test download handles NoSuchKey error"""

        mock_client = MagicMock()
        error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
        mock_client.get_object.side_effect = ClientError(error_response, "GetObject")
        mock_boto_client.return_value = mock_client

        df = s3_resource.download_dataframe(
            bucket="raw",
            key="test/nonexistent.parquet",
        )

        assert df is None

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_download_dataframe_empty_content(self, mock_boto_client, s3_resource):
        """Test download handles empty content"""

        mock_client = MagicMock()
        mock_response = {
            "Body": Mock(read=Mock(return_value=b"")),
            "ContentLength": 0,
        }
        mock_client.get_object.return_value = mock_response
        mock_boto_client.return_value = mock_client

        df = s3_resource.download_dataframe(
            bucket="raw",
            key="test/empty.parquet",
        )

        assert df is None

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_download_dataframe_unsupported_format(self, mock_boto_client, s3_resource):
        """Test download returns None for unsupported format"""

        mock_client = MagicMock()
        mock_response = {
            "Body": Mock(read=Mock(return_value=b"some data")),
            "ContentLength": 100,
        }
        mock_client.get_object.return_value = mock_response
        mock_boto_client.return_value = mock_client

        df = s3_resource.download_dataframe(
            bucket="raw",
            key="test/data.txt",
        )

        assert df is None

    @patch("dagster_code.resources.s3_resource.boto3.client")
    def test_download_dataframe_with_timedelta(self, mock_boto_client, s3_resource):
        """Test download handles timedelta columns with NaT"""

        mock_client = MagicMock()

        # Create DataFrame with timedelta column containing NaT
        df_with_timedelta = pd.DataFrame(
            {
                "name": ["A", "B", "C"],
                "duration": pd.to_timedelta([1, pd.NaT, 3], unit="s"),
            }
        )

        # Create buffer with parquet data
        buffer = BytesIO()
        df_with_timedelta.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        mock_response = {
            "Body": Mock(read=Mock(return_value=buffer.getvalue())),
            "ContentLength": len(buffer.getvalue()),
        }
        mock_client.get_object.return_value = mock_response
        mock_boto_client.return_value = mock_client

        df = s3_resource.download_dataframe(
            bucket="raw",
            key="test/data.parquet",
        )

        assert df is not None
        # NaT should be replaced with None
        assert df["duration"].iloc[1] is None
