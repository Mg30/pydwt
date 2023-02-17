import os
import sys

import typer
import yaml
from dependency_injector.wiring import register_loader_containers

from pydbt.core.containers import Container

app = typer.Typer()
container = Container()

# Load configuration from YAML file
with open("settings.yml") as f:
    config = yaml.safe_load(f)
container.config.from_dict(config)

# Register container and dependencies
sys.path.append(os.getcwd())
register_loader_containers(container)

# Get a project handler instance from the container
project_handler = container.project_factory()

# Define command-line interface using Typer
@app.command()
def new(project_name: str):
    """Create a new PyDBT project."""
    project_handler.new(project_name)


@app.command()
def run():
    """Run the workflow DAG for the current project."""
    project_handler.run()


@app.command()
def export_dag():
    """Export the workflow DAG for the current project."""
    project_handler.export_dag()


if __name__ == "__main__":
    # Run the command-line interface
    try:
        app()
    except Exception as e:
        # Display a helpful error message if a known error occurs
        if "config" in str(e):
            print("Error: Failed to load configuration from settings.yml.")
            print("Make sure the file exists and contains valid YAML data.")
        else:
            # Display the full error message for unknown errors
            print(f"Error: {e}")