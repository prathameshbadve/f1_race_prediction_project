"""
Unit tests for dagster_code/resources/fastf1_resource.py
"""
# pylint: disable=unused-argument, protected-access

from unittest.mock import MagicMock, patch, Mock
import pytest
from dagster import InitResourceContext
from dagster_code.resources.fastf1_resource import FastF1Resource


class TestFastF1Resource:
    """Tests for FastF1Resource"""

    @pytest.fixture
    def fastf1_resource(self, mock_env_vars):
        """Create FastF1Resource instance"""

        return FastF1Resource()

    def test_get_config(self, fastf1_resource, mock_env_vars):
        """Test get_config returns FastF1Config"""

        config = fastf1_resource.get_config()
        assert config.cache_enabled is True
        assert config.cache_dir == "data/external/fastf1_cache"
        assert config.log_level == "INFO"

    def test_from_env(self, mock_env_vars):
        """Test from_env factory method"""

        resource = FastF1Resource.from_env()
        assert isinstance(resource, FastF1Resource)

    @patch("dagster_code.resources.fastf1_resource.Cache")
    @patch("dagster_code.resources.fastf1_resource.fastf1.set_log_level")
    @patch("dagster_code.resources.fastf1_resource.ensure_directory")
    def test_setup_for_execution_with_cache_enabled(
        self,
        mock_ensure_dir,
        mock_set_log_level,
        mock_cache,
        fastf1_resource,
        mock_env_vars,
    ):
        """Test setup_for_execution with cache enabled"""

        mock_context = Mock(spec=InitResourceContext)

        fastf1_resource.setup_for_execution(mock_context)

        # Verify cache was enabled
        mock_cache.enable_cache.assert_called_once()
        call_kwargs = mock_cache.enable_cache.call_args[1]
        assert "cache_dir" in call_kwargs
        assert call_kwargs["force_renew"] is False

        # Verify log level was set
        mock_set_log_level.assert_called_once_with("INFO")

        # Verify directory was ensured
        mock_ensure_dir.assert_called_once()

    @patch("dagster_code.resources.fastf1_resource.Cache")
    @patch("dagster_code.resources.fastf1_resource.fastf1.set_log_level")
    def test_setup_for_execution_with_cache_disabled(
        self,
        mock_set_log_level,
        mock_cache,
        mock_env_vars,
        monkeypatch,
    ):
        """Test setup_for_execution with cache disabled"""

        monkeypatch.setenv("FASTF1_CACHE_ENABLED", "False")
        resource = FastF1Resource()
        mock_context = Mock(spec=InitResourceContext)

        resource.setup_for_execution(mock_context)

        # Verify cache was disabled
        mock_cache.set_disabled.assert_called_once()
        mock_cache.enable_cache.assert_not_called()

        # Verify log level was still set
        mock_set_log_level.assert_called_once()

    @patch("dagster_code.resources.fastf1_resource.Cache")
    @patch("dagster_code.resources.fastf1_resource.fastf1.set_log_level")
    def test_setup_for_execution_with_force_renew(
        self,
        mock_set_log_level,
        mock_cache,
        mock_env_vars,
        monkeypatch,
    ):
        """Test setup_for_execution with force_renew enabled"""

        monkeypatch.setenv("FASTF1_FORCE_RENEW_CACHE", "True")
        resource = FastF1Resource()
        mock_context = Mock(spec=InitResourceContext)

        resource.setup_for_execution(mock_context)

        # Verify cache was enabled with force_renew=True
        call_kwargs = mock_cache.enable_cache.call_args[1]
        assert call_kwargs["force_renew"] is True

    def test_get_session_names_default(self, fastf1_resource):
        """Test _get_session_names with default configuration"""

        session_names = fastf1_resource._get_session_names()

        assert "Race" in session_names
        assert "Qualifying" in session_names
        assert "Sprint" not in session_names
        assert "Practice 1" not in session_names

    def test_get_session_names_all_sessions(self, mock_env_vars, monkeypatch):
        """Test _get_session_names with all sessions enabled"""
        session_vars = {
            "FASTF1_INCLUDE_PRACTICE": "True",
            "FASTF1_INCLUDE_SPRINT_QUALIFYING": "True",
            "FASTF1_INCLUDE_SPRINT": "True",
            "FASTF1_INCLUDE_QUALIFYING": "True",
            "FASTF1_INCLUDE_RACE": "True",
        }
        for key, value in session_vars.items():
            monkeypatch.setenv(key, value)

        resource = FastF1Resource()
        session_names = resource._get_session_names()

        assert "Race" in session_names
        assert "Qualifying" in session_names
        assert "Sprint" in session_names
        assert (
            "Sprint Shootout" in session_names or "Sprint Qualifying" in session_names
        )
        assert "Practice 1" in session_names
        assert "Practice 2" in session_names
        assert "Practice 3" in session_names

    def test_get_session_names_only_race(self, mock_env_vars, monkeypatch):
        """Test _get_session_names with only race enabled"""
        monkeypatch.setenv("FASTF1_INCLUDE_QUALIFYING", "False")
        monkeypatch.setenv("FASTF1_INCLUDE_RACE", "True")

        resource = FastF1Resource()
        session_names = resource._get_session_names()

        assert "Race" in session_names
        assert "Qualifying" not in session_names

    @patch("dagster_code.resources.fastf1_resource.fastf1.get_session")
    def test_get_session(self, mock_get_session, fastf1_resource):
        """Test get_session calls fastf1.get_session with correct parameters"""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        result = fastf1_resource.get_session(
            year=2024,
            event="Monaco Grand Prix",
            session="Race",
        )

        mock_get_session.assert_called_once_with(2024, "Monaco Grand Prix", "Race")
        assert result == mock_session

    @patch("dagster_code.resources.fastf1_resource.fastf1.get_session")
    def test_get_session_with_round_number(self, mock_get_session, fastf1_resource):
        """Test get_session with round number instead of event name"""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        result = fastf1_resource.get_session(
            year=2024,
            event=6,
            session="Qualifying",
        )

        mock_get_session.assert_called_once_with(2024, 6, "Qualifying")
        assert result == mock_session

    @patch("dagster_code.resources.fastf1_resource.fastf1.get_session")
    def test_get_session_sprint(self, mock_get_session, fastf1_resource):
        """Test get_session for sprint session"""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session

        result = fastf1_resource.get_session(
            year=2024,
            event="São Paulo Grand Prix",
            session="Sprint",
        )

        mock_get_session.assert_called_once_with(2024, "São Paulo Grand Prix", "Sprint")
        assert result == mock_session
