"""
Pytest configuration and shared fixtures
"""

from unittest.mock import MagicMock
import pytest
import pandas as pd
from dotenv import load_dotenv

# Load test environment variables
load_dotenv()


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing"""

    env_vars = {
        "ENVIRONMENT": "development",
        "MINIO_ENDPOINT": "http://localhost:9000",
        "MINIO_ROOT_USER": "test_user",
        "MINIO_ROOT_PASSWORD": "test_password",
        "RAW_DATA_BUCKET": "test-raw-bucket",
        "PROCESSED_DATA_BUCKET": "test-processed-bucket",
        "REGION": "ap-south-1",
        "SECURE": "False",
        "FASTF1_CACHE_ENABLED": "True",
        "FASTF1_CACHE_DIR": "data/external/fastf1_cache",
        "FASTF1_FORCE_RENEW_CACHE": "False",
        "FASTF1_LOG_LEVEL": "INFO",
        "FASTF1_REQUEST_TIMEOUT": "30",
        "FASTF1_MAX_RETRIES": "3",
        "FASTF1_RETRY_DELAY": "5",
        "FASTF1_INCLUDE_TESTING": "False",
        "FASTF1_ENABLE_LAPS": "True",
        "FASTF1_ENABLE_WEATHER": "True",
        "FASTF1_ENABLE_MESSAGES": "True",
        "FASTF1_ENABLE_TELEMETRY": "False",
        "FASTF1_INCLUDE_PRACTICE": "False",
        "FASTF1_INCLUDE_SPRINT_QUALIFYING": "False",
        "FASTF1_INCLUDE_SPRINT": "False",
        "FASTF1_INCLUDE_QUALIFYING": "True",
        "FASTF1_INCLUDE_RACE": "True",
        "LOG_DIR": "monitoring/logs/",
        "LOG_LEVEL": "INFO",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing"""

    sample_df = pd.DataFrame(
        {
            "DriverNumber": [1, 44, 55],
            "BroadcastName": ["M. VERSTAPPEN", "L. HAMILTON", "C. SAINZ"],
            "Abbreviation": ["VER", "HAM", "SAI"],
            "Position": [1, 2, 3],
            "Points": [25, 18, 15],
        }
    )
    return sample_df


@pytest.fixture
def mock_boto3_client():
    """Create a mock boto3 S3 client"""

    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mock_client.get_object.return_value = {
        "Body": MagicMock(),
        "ContentLength": 1000,
    }
    return mock_client


@pytest.fixture
def mock_fastf1_session():
    """Create a mock FastF1 session"""

    mock_session = MagicMock()
    mock_session.results = pd.DataFrame(
        {
            "DriverNumber": [1, 44],
            "BroadcastName": ["M. VERSTAPPEN", "L. HAMILTON"],
            "Position": [1, 2],
        }
    )
    mock_session.load.return_value = None
    return mock_session


@pytest.fixture
def temp_test_dir(tmp_path):
    """Create a temporary directory for testing"""

    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    return test_dir
