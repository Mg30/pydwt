import os
import shutil
import pytest
from pydbt.core.project import Project
import os



@pytest.fixture(scope="session")
def setup():
    project_name = "test_project"
    models_folder = "models"
    dags_folder = "dags"
    proj = Project(name=project_name, models_folder=models_folder, dags_folder=dags_folder, workflow=None)
    proj.new("test_project")
    yield proj
    # Clean up the test project directory
    shutil.rmtree(project_name)
    os.remove("settings.yml")

def test_project_dir_exist(setup):
    # Test that the project directories were created
    assert os.path.exists("test_project")

def test_project_dir_models_exists(setup):
    # Test that the project directories were created
    assert os.path.exists(os.path.join("test_project", "models"))


def test_project_dir_dags_exists(setup):
    # Test that the project directories were created
    assert os.path.exists(os.path.join("test_project", "dags"))

def test_project_settings_exists(setup):
    # Test that the project directories were created
    assert os.path.exists("settings.yml")
    assert os.path.exists(os.path.join("test_project", "models", "example.py"))

def test_project_example_exists(setup):
    # Test that the project directories were created
    assert os.path.exists("settings.yml")
    assert os.path.exists(os.path.join("test_project", "models", "example.py"))