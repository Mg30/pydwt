from typer.testing import CliRunner
from pydbt.app import app, container
import os
import shutil
import pytest
import unittest.mock






@pytest.fixture(scope="session")
def setup():
    container.database_client.override(unittest.mock.Mock())
    yield None
    # clean up
    shutil.rmtree("my_project")
    os.remove("settings.yml")


def test_new(setup):
    runner = CliRunner()
    result = runner.invoke(app, ["new", "my_project"])
    assert result.exit_code == 0

def test_run(setup):
    runner = CliRunner()
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0


def test_export_dag(setup):
    runner = CliRunner()
    result = runner.invoke(app, ["export-dag"])
    assert result.exit_code == 0