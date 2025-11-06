"""
Unit tests for dagster_code/assets/f1_ingestion.py
"""
# pylint: disable=unused-argument

from unittest.mock import MagicMock
import pytest
import pandas as pd
from dagster import Output
from dagster_code.assets.f1_ingestion import (
    f1_session_results,
    F1SessionConfig,
)


class TestF1SessionConfig:
    """Tests for F1SessionConfig"""

    def test_f1_session_config_creation(self):
        """Test creating F1SessionConfig with all fields"""

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Monaco Grand Prix",
            session_name="Race",
        )

        assert config.year == 2024
        assert config.grand_prix_name == "Monaco Grand Prix"
        assert config.session_name == "Race"

    def test_f1_session_config_qualifying(self):
        """Test F1SessionConfig for qualifying session"""

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Bahrain Grand Prix",
            session_name="Qualifying",
        )

        assert config.session_name == "Qualifying"

    def test_f1_session_config_sprint(self):
        """Test F1SessionConfig for sprint session"""

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="São Paulo Grand Prix",
            session_name="Sprint",
        )

        assert config.session_name == "Sprint"


class TestF1SessionResults:
    """Tests for f1_session_results asset"""

    @pytest.fixture
    def mock_fastf1_resource(self):
        """Create mock FastF1Resource"""

        resource = MagicMock()
        mock_session = MagicMock()
        mock_session.results = pd.DataFrame(
            {
                "DriverNumber": [1, 44, 55],
                "BroadcastName": ["M. VERSTAPPEN", "L. HAMILTON", "C. SAINZ"],
                "Position": [1, 2, 3],
                "Points": [25, 18, 15],
            }
        )
        resource.get_session.return_value = mock_session
        return resource

    @pytest.fixture
    def mock_s3_resource(self):
        """Create mock S3Resource"""

        resource = MagicMock()
        resource.upload_dataframe_as_parquet.return_value = True
        return resource

    def test_f1_session_results_success(self, mock_fastf1_resource, mock_s3_resource):
        """Test successful f1_session_results execution"""

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Monaco Grand Prix",
            session_name="Race",
        )

        result = f1_session_results(
            config=config,
            fastf1_resource=mock_fastf1_resource,
            s3_resource=mock_s3_resource,
        )

        # Verify session was loaded
        mock_fastf1_resource.get_session.assert_called_once_with(
            2024, "Monaco Grand Prix", "Race"
        )

        # Verify session.load was called with correct parameters
        mock_session = mock_fastf1_resource.get_session.return_value
        mock_session.load.assert_called_once_with(
            laps=False,
            messages=False,
            weather=False,
            telemetry=False,
        )

        # Verify upload was called
        mock_s3_resource.upload_dataframe_as_parquet.assert_called_once()
        call_kwargs = mock_s3_resource.upload_dataframe_as_parquet.call_args[1]
        assert call_kwargs["bucket"] == "f1-data-raw"
        assert call_kwargs["key"] == "2024/Monaco Grand Prix/Race/results.parquet"

        # Verify output
        assert isinstance(result, Output)
        assert result.value["year"] == 2024
        assert result.value["grand_prix_name"] == "Monaco Grand Prix"
        assert result.value["session_name"] == "Race"
        assert result.value["successful"] is True

    def test_f1_session_results_qualifying(
        self, mock_fastf1_resource, mock_s3_resource
    ):
        """Test f1_session_results for qualifying session"""

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Bahrain Grand Prix",
            session_name="Qualifying",
        )

        result = f1_session_results(
            config=config,
            fastf1_resource=mock_fastf1_resource,
            s3_resource=mock_s3_resource,
        )

        # Verify correct S3 key
        call_kwargs = mock_s3_resource.upload_dataframe_as_parquet.call_args[1]
        assert (
            call_kwargs["key"] == "2024/Bahrain Grand Prix/Qualifying/results.parquet"
        )

        assert result.value["session_name"] == "Qualifying"

    def test_f1_session_results_upload_failure(
        self, mock_fastf1_resource, mock_s3_resource
    ):
        """Test f1_session_results when upload fails"""

        mock_s3_resource.upload_dataframe_as_parquet.return_value = False

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Monaco Grand Prix",
            session_name="Race",
        )

        result = f1_session_results(
            config=config,
            fastf1_resource=mock_fastf1_resource,
            s3_resource=mock_s3_resource,
        )

        # Verify output shows failure
        assert result.value["successful"] is False

    def test_f1_session_results_includes_metadata(
        self, mock_fastf1_resource, mock_s3_resource
    ):
        """Test that f1_session_results includes metadata"""

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Monaco Grand Prix",
            session_name="Race",
        )

        result = f1_session_results(
            config=config,
            fastf1_resource=mock_fastf1_resource,
            s3_resource=mock_s3_resource,
        )

        # Verify metadata
        assert "year" in result.metadata
        assert "grand_prix_name" in result.metadata
        assert "session_name" in result.metadata
        assert "successful" in result.metadata
        assert result.metadata["year"] == 2024

    def test_f1_session_results_small_dataset_includes_summary(
        self, mock_fastf1_resource, mock_s3_resource
    ):
        """Test that small datasets include results_summary in metadata"""

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Monaco Grand Prix",
            session_name="Race",
        )

        result = f1_session_results(
            config=config,
            fastf1_resource=mock_fastf1_resource,
            s3_resource=mock_s3_resource,
        )

        # Results has 3 rows (< 20), so should include summary
        assert "results_summary" in result.metadata

    def test_f1_session_results_large_dataset_no_summary(
        self, mock_fastf1_resource, mock_s3_resource
    ):
        """Test that large datasets don't include results_summary"""

        # Create large results DataFrame
        mock_session = MagicMock()
        mock_session.results = pd.DataFrame(
            {
                "DriverNumber": list(range(1, 22)),
                "Position": list(range(1, 22)),
            }
        )
        mock_fastf1_resource.get_session.return_value = mock_session

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Monaco Grand Prix",
            session_name="Race",
        )

        result = f1_session_results(
            config=config,
            fastf1_resource=mock_fastf1_resource,
            s3_resource=mock_s3_resource,
        )

        # Results has 21 rows (> 20), so should NOT include summary
        assert "results_summary" not in result.metadata

    def test_f1_session_results_different_years(
        self, mock_fastf1_resource, mock_s3_resource
    ):
        """Test f1_session_results with different years"""

        for year in [2022, 2023, 2024]:
            config = F1SessionConfig(
                year=year,
                grand_prix_name="Monaco Grand Prix",
                session_name="Race",
            )

            result = f1_session_results(
                config=config,
                fastf1_resource=mock_fastf1_resource,
                s3_resource=mock_s3_resource,
            )

            assert result.value["year"] == year
            call_kwargs = mock_s3_resource.upload_dataframe_as_parquet.call_args[1]
            assert call_kwargs["key"].startswith(f"{year}/")

    def test_f1_session_results_verifies_dataframe_passed(
        self, mock_fastf1_resource, mock_s3_resource
    ):
        """Test that the actual results DataFrame is passed to S3"""

        expected_df = pd.DataFrame(
            {
                "DriverNumber": [1, 44],
                "BroadcastName": ["M. VERSTAPPEN", "L. HAMILTON"],
            }
        )
        mock_session = MagicMock()
        mock_session.results = expected_df
        mock_fastf1_resource.get_session.return_value = mock_session

        config = F1SessionConfig(
            year=2024,
            grand_prix_name="Monaco Grand Prix",
            session_name="Race",
        )

        f1_session_results(
            config=config,
            fastf1_resource=mock_fastf1_resource,
            s3_resource=mock_s3_resource,
        )

        # Verify the DataFrame passed to upload
        call_kwargs = mock_s3_resource.upload_dataframe_as_parquet.call_args[1]
        uploaded_df = call_kwargs["df"]
        pd.testing.assert_frame_equal(uploaded_df, expected_df)
