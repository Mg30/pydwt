import os
from dataclasses import dataclass, field
from pydbt.core.workflow import Workflow
from datetime import datetime
import sys
import logging
import yaml
import importlib

@dataclass
class Project(object):
    workflow: Workflow
    name: str
    models_folder: str = field(default="models")
    dags_folder: str = field(default="dags")

    def __post_init__(self):
        sys.path.append(os.getcwd())
        logging.basicConfig(
            format="%(levelname)s %(asctime)s: %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
            level=logging.INFO,
        )

    def new(self, project_name: str):
        # create the models folder if it does not exist
        self._create_models_directory(project_name)
        # create the example.py file if it does not exist
        self._create_tasks_example(project_name)
        self._create_dags_directory(project_name)
        self._create_settings(project_name)

    def import_all_models(self):
        models_folders = os.path.join(self.name, "models")
        models = [
            file.split(".")[0]
            for file in os.listdir(models_folders)
            if file.endswith(".py")
        ]
        for model in models:
            importlib.import_module(f"{self.name}.models.{model}")

    def run(self):
        self.import_all_models()
        self.workflow.dag.build_dag()
        self.workflow.run()

    def export_dag(self):
        self.import_all_models()
        dag_file_name = os.path.join(
            self.name,
            self.dags_folder,
            f'dag_{datetime.now().strftime("%Y%m%d_%H:%M:%S")}',
        )
        self.workflow.dag.build_dag()
        self.workflow.export_dag(dag_file_name)


    def _create_models_directory(self, project_name: str):
        models_project = os.path.join(project_name, self.models_folder)
        if not os.path.exists(models_project):
            os.makedirs(models_project)

    def _create_dags_directory(self, project_name):
        dag_folder = os.path.join(project_name, self.dags_folder)
        if not os.path.exists(dag_folder):
            os.makedirs(dag_folder)

    def _create_settings(self, project_name):
        settings_projects = os.path.join("settings.yml")
        settings = {
            "project": {
                "name": project_name,
                "use_cache": False,
                "executor": "ThreadExecutor",
                "cache_strategy": "LocalCache",
            },
            "tasks": {
                f"{project_name}.models.example.task_one": {"materialize": "view"}
            },
            "sources": {"one": {"table": "table_name", "schema": "some_schema"}},
            "connection": {
                "db": "",
                "host": "",
                "port": 0,
                "password": "user",
                "sql_alchemy_driver": "",
            },
        }
        if not os.path.exists(settings_projects):
            with open(settings_projects, "w") as file:
                yaml.safe_dump(settings, file)

    def _create_tasks_example(self, project_name):
        models_project = os.path.join(project_name, self.models_folder)
        example_file = os.path.join(models_project, "example.py")
        if not os.path.exists(example_file):
            with open(example_file, "w") as f:
                f.write(
                    """
from pydbt.core.task import Task

@Task()
def task_one():
    pass

@Task(depends_on=[task_one])
def task_two():
    pass
"""
                )
