"""
Unit tests for config/settings.py
"""
# pylint: disable=unused-argument

from pathlib import Path

from config.settings import (
    Environment,
    BaseConfig,
    StorageConfig,
    FastF1Config,
)


class TestEnvironment:
    """Tests for Environment enum"""

    def test_environment_values(self):
        """Test that Environment enum has correct values"""

        assert Environment.DEVELOPMENT.value == "development"
        assert Environment.PRODUCTION.value == "production"

    def test_environment_is_string_enum(self):
        """Test that Environment is a string enum"""

        assert isinstance(Environment.DEVELOPMENT, str)
        assert isinstance(Environment.PRODUCTION, str)


class TestBaseConfig:
    """Tests for BaseConfig"""

    def test_base_config_default_environment(self, monkeypatch):
        """Test BaseConfig uses default environment when not set"""

        monkeypatch.delenv("ENVIRONMENT", raising=False)
        config = BaseConfig()
        assert config.environment == Environment.DEVELOPMENT.value

    def test_base_config_from_env(self, monkeypatch):
        """Test BaseConfig reads environment from env variable"""

        monkeypatch.setenv("ENVIRONMENT", "production")
        config = BaseConfig()
        assert config.environment == "production"

    def test_base_config_has_project_root(self):
        """Test BaseConfig includes project_root"""

        config = BaseConfig()
        assert config.project_root is not None
        assert isinstance(config.project_root, Path)


class TestStorageConfig:
    """Tests for StorageConfig"""

    def test_storage_config_from_env(self, mock_env_vars):
        """Test StorageConfig.from_env() loads all values correctly"""

        config = StorageConfig.from_env()

        assert config.endpoint == "http://localhost:9000"
        assert config.access_key == "test_user"
        assert config.secret_key == "test_password"
        assert config.raw_data_bucket == "test-raw-bucket"
        assert config.processed_data_bucket == "test-processed-bucket"
        assert config.region == "ap-south-1"
        assert config.secure is False

    def test_storage_config_secure_true(self, monkeypatch, mock_env_vars):
        """Test StorageConfig with secure=true"""

        monkeypatch.setenv("SECURE", "true")
        config = StorageConfig.from_env()
        assert config.secure is True

    def test_storage_config_default_region(self, monkeypatch):
        """Test StorageConfig uses default region"""

        monkeypatch.delenv("REGION", raising=False)
        monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
        config = StorageConfig.from_env()
        assert config.region == "ap-south-1"

    def test_storage_config_custom_region(self, monkeypatch):
        """Test StorageConfig with custom region"""

        monkeypatch.setenv("REGION", "us-east-1")
        monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
        config = StorageConfig.from_env()
        assert config.region == "us-east-1"

    def test_storage_config_missing_values(self, monkeypatch):
        """Test StorageConfig handles missing environment variables"""

        # Clear all env vars
        for key in ["MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"]:
            monkeypatch.delenv(key, raising=False)

        config = StorageConfig.from_env()
        assert config.endpoint is None
        assert config.access_key is None
        assert config.secret_key is None


class TestFastF1Config:
    """Tests for FastF1Config"""

    def test_fastf1_config_from_env(self, mock_env_vars):
        """Test FastF1Config.from_env() loads all values correctly"""

        config = FastF1Config.from_env()

        # Cache settings
        assert config.cache_enabled is True
        assert config.cache_dir == "data/external/fastf1_cache"
        assert config.force_renew_cache is False

        # Log settings
        assert config.log_level == "INFO"

        # Connection settings
        assert config.request_timeout == 30
        assert config.max_retries == 3
        assert config.retry_delay == 5

        # Testing sessions
        assert config.include_testing is False

        # Data features
        assert config.enable_laps is True
        assert config.enable_weather is True
        assert config.enable_messages is True
        assert config.enable_telemetry is False

        # Sessions
        assert config.include_practice is False
        assert config.include_sprint_qualifying is False
        assert config.include_sprint is False
        assert config.include_qualifying is True
        assert config.include_race is True

    def test_fastf1_config_cache_disabled(self, monkeypatch):
        """Test FastF1Config with cache disabled"""

        monkeypatch.setenv("FASTF1_CACHE_ENABLED", "False")
        config = FastF1Config.from_env()
        assert config.cache_enabled is False

    def test_fastf1_config_all_sessions_enabled(self, monkeypatch):
        """Test FastF1Config with all sessions enabled"""

        session_vars = {
            "FASTF1_INCLUDE_PRACTICE": "True",
            "FASTF1_INCLUDE_SPRINT_QUALIFYING": "True",
            "FASTF1_INCLUDE_SPRINT": "True",
            "FASTF1_INCLUDE_QUALIFYING": "True",
            "FASTF1_INCLUDE_RACE": "True",
        }
        for key, value in session_vars.items():
            monkeypatch.setenv(key, value)

        config = FastF1Config.from_env()
        assert config.include_practice is True
        assert config.include_sprint_qualifying is True
        assert config.include_sprint is True
        assert config.include_qualifying is True
        assert config.include_race is True

    def test_fastf1_config_integer_conversion(self, monkeypatch):
        """Test FastF1Config converts string env vars to integers"""

        monkeypatch.setenv("FASTF1_REQUEST_TIMEOUT", "60")
        monkeypatch.setenv("FASTF1_MAX_RETRIES", "5")
        monkeypatch.setenv("FASTF1_RETRY_DELAY", "10")

        config = FastF1Config.from_env()
        assert config.request_timeout == 60
        assert config.max_retries == 5
        assert config.retry_delay == 10
        assert isinstance(config.request_timeout, int)

    def test_fastf1_config_log_level_debug(self, monkeypatch):
        """Test FastF1Config with DEBUG log level"""

        monkeypatch.setenv("FASTF1_LOG_LEVEL", "DEBUG")
        config = FastF1Config.from_env()
        assert config.log_level == "DEBUG"

    def test_fastf1_config_enable_telemetry(self, monkeypatch):
        """Test FastF1Config with telemetry enabled"""

        monkeypatch.setenv("FASTF1_ENABLE_TELEMETRY", "True")
        config = FastF1Config.from_env()
        assert config.enable_telemetry is True
