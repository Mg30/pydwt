from typer.testing import CliRunner
from pydwt.app import app, container
import os
import shutil
import pytest
from pydwt.context.connection import Connection

args = {"url": "sqlite:///:memory:", "echo": True}

container.database_client.override(Connection(args))


@pytest.fixture(scope="module")
def setup():
    yield None
    # clean up
    shutil.rmtree("my_project")
    os.remove("settings.yml")


def test_new(setup):
    runner = CliRunner()
    result = runner.invoke(app, ["new", "my_project"])
    assert result.exit_code == 0


def test_export_dag(setup):
    runner = CliRunner()
    result = runner.invoke(app, ["export-dag"])
    assert result.exit_code == 0


def test_connection_testing(setup):
    runner = CliRunner()
    result = runner.invoke(app, ["test-connection"])
    assert result.exit_code == 0