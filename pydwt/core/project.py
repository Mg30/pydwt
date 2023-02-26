import importlib
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

import yaml

from pydwt.core.workflow import Workflow


@dataclass
class Project:
    """
    Project class to manage the DAG-based workflows.

    Attributes:
        workflow (Workflow): Workflow object to execute DAG.
        name (str): Name of the project.
        models_folder (str): Name of the folder that contains the models (default: "models").
        dags_folder (str): Name of the folder to store the DAGs (default: "dags").
    """

    workflow: Workflow
    name: str
    models_folder: str = field(default="models")
    dags_folder: str = field(default="dags")

    def __post_init__(self) -> None:
        # Add the current working directory to the system path to allow importing modules from the project.
        sys.path.append(os.getcwd())

        # Configure the logging.
        logging.basicConfig(
            format="%(levelname)s %(asctime)s: %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
            level=logging.INFO,
        )

    def new(self, project_name: str) -> None:
        """
        Create a new project directory structure.

        Args:
            project_name (str): Name of the project.
        """
        self._create_models_directory(project_name)
        self._create_tasks_example(project_name)
        self._create_dags_directory(project_name)
        self._create_settings(project_name)

    def import_all_models(self) -> None:
        """Import all the models from the models folder of the project."""
        models_folders = os.path.join(self.name, "models")
        models = [
            file.split(".")[0]
            for file in os.listdir(models_folders)
            if file.endswith(".py")
        ]
        for model in models:
            importlib.import_module(f"{self.name}.models.{model}")

    def run(self) -> None:
        """Run the DAG-based workflow."""
        self.import_all_models()
        self.workflow.dag.build_dag()
        self.workflow.run()

    def export_dag(self) -> None:
        """Export the DAG to a PNG image file."""
        self.import_all_models()
        dag_file_name = os.path.join(
            self.name,
            self.dags_folder,
            f'dag_{datetime.now().strftime("%Y%m%d_%H:%M:%S")}',
        )
        self.workflow.dag.build_dag()
        self.workflow.export_dag(dag_file_name)

    def _create_models_directory(self, project_name: str) -> None:
        """
        Create the models directory if it does not exist.

        Args:
            project_name (str): Name of the project.
        """
        models_project = os.path.join(project_name, self.models_folder)
        if not os.path.exists(models_project):
            os.makedirs(models_project)

    def _create_dags_directory(self, project_name: str) -> None:
        """
        Create the DAGs directory if it does not exist.

        Args:
            project_name (str): Name of the project.
        """
        dag_folder = os.path.join(project_name, self.dags_folder)
        if not os.path.exists(dag_folder):
            os.makedirs(dag_folder)

    def _create_settings(self, project_name: str) -> None:
        """Create a default settings file for the project.

        Args:
            project_name (str): Name of the project to create settings for.
        """
        settings_projects = os.path.join("settings.yml")
        settings: Dict = {
            "project": {
                "name": project_name,
            },
            "tasks": {"task_one": {"materialize": "view"}},
            "sources": {"one": {"table": "table_name", "schema": "some_schema"}},
            "connection": {
                "db": "",
                "host": "",
                "port": 0,
                "password": "",
                "user": "",
                "sql_alchemy_driver": "",
            },
        }
        if not os.path.exists(settings_projects):
            with open(settings_projects, "w") as file:
                yaml.safe_dump(settings, file)

    def _create_tasks_example(self, project_name: str) -> None:
        """Create an example task file for the project.

        Args:
            project_name (str): Name of the project to create the example task for.
        """
        models_project = os.path.join(project_name, self.models_folder)
        example_file = os.path.join(models_project, "example.py")
        if not os.path.exists(example_file):
            with open(example_file, "w") as f:
                f.write(
                    """
from pydwt.core.task import Task
from dependency_injector.wiring import inject, Provide
from pydwt.core.containers import Container

@Task()
@inject
def task_one(config:dict = Provide[Container.config.tasks.task_one]):
    print(config)

@Task(depends_on=[task_one])
def task_two():
    print("somme processing")    

"""
                )
