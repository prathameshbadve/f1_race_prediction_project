"""
Unit tests for src/utils/helpers.py
"""

from pathlib import Path
from src.utils.helpers import get_project_root, ensure_directory


class TestGetProjectRoot:
    """Tests for get_project_root function"""

    def test_get_project_root_finds_pyproject_toml(self, tmp_path, monkeypatch):
        """Test that get_project_root finds the directory with pyproject.toml"""

        # Create a mock project structure
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "pyproject.toml").touch()

        # Create subdirectory
        sub_dir = project_dir / "src" / "utils"
        sub_dir.mkdir(parents=True)

        # Change to subdirectory
        monkeypatch.chdir(sub_dir)

        # Test
        root = get_project_root()
        assert root == project_dir
        assert (root / "pyproject.toml").exists()

    def test_get_project_root_from_root_directory(self, tmp_path, monkeypatch):
        """Test that get_project_root works when already in root"""

        # Create pyproject.toml in current directory
        (tmp_path / "pyproject.toml").touch()
        monkeypatch.chdir(tmp_path)

        root = get_project_root()
        assert root == tmp_path

    def test_get_project_root_nested_structure(self, tmp_path, monkeypatch):
        """Test with deeply nested directory structure"""

        # Create nested structure
        project_dir = tmp_path / "workspace" / "projects" / "f1_project"
        deep_dir = project_dir / "src" / "utils" / "nested" / "deep"
        deep_dir.mkdir(parents=True)
        (project_dir / "pyproject.toml").touch()

        monkeypatch.chdir(deep_dir)

        root = get_project_root()
        assert root == project_dir


class TestEnsureDirectory:
    """Tests for ensure_directory function"""

    def test_ensure_directory_creates_new_directory(self, tmp_path):
        """Test that ensure_directory creates a new directory"""

        new_dir = tmp_path / "new_directory"
        assert not new_dir.exists()

        ensure_directory(new_dir)

        assert new_dir.exists()
        assert new_dir.is_dir()

    def test_ensure_directory_creates_nested_directories(self, tmp_path):
        """Test that ensure_directory creates nested directories"""

        nested_dir = tmp_path / "level1" / "level2" / "level3"
        assert not nested_dir.exists()

        ensure_directory(nested_dir)

        assert nested_dir.exists()
        assert nested_dir.is_dir()
        assert nested_dir.parent.exists()

    def test_ensure_directory_with_existing_directory(self, tmp_path):
        """Test that ensure_directory doesn't raise error for existing directory"""

        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()

        # Should not raise any exception
        ensure_directory(existing_dir)

        assert existing_dir.exists()
        assert existing_dir.is_dir()

    def test_ensure_directory_with_path_object(self, tmp_path):
        """Test ensure_directory works with Path objects"""

        new_dir = Path(tmp_path) / "path_object_dir"

        ensure_directory(new_dir)

        assert new_dir.exists()
        assert isinstance(new_dir, Path)

    def test_ensure_directory_with_string_path(self, tmp_path):
        """Test ensure_directory works with string paths"""

        new_dir = tmp_path / "string_path_dir"

        ensure_directory(str(new_dir))

        assert new_dir.exists()
