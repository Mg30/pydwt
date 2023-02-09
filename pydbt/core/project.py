import os
from dataclasses import dataclass, field
from pydbt.core.workflow import Workflow
from datetime import datetime
import importlib
import sys
import logging


@dataclass
class Project(object):
    models_folder: str = field(default="models")
    dags_folder: str = field(default="dags")
    _workflow: Workflow = field(init=False)

    def __post_init__(self):
        sys.path.append(os.getcwd())
        logging.basicConfig(
            format="%(levelname)s %(asctime)s: %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
            level=logging.INFO,
        )

    @property
    def project_name(self):
        self._load_settings()
        return getattr(self._settings_module, "PROJECT_NAME")

    @property
    def executor(self):
        return getattr(self._settings_module, "EXECUTOR")

    @property
    def use_cache(self):
        return getattr(self._settings_module, "USE_CACHE")

    @property
    def cache_strategy(self):
        cache_strategy_name = getattr(self._settings_module, "CACHE_STRATEGY")
        cache_module = importlib.import_module("pydbt.core.cache")
        cache_strategy_class = getattr(cache_module, cache_strategy_name)
        return cache_strategy_class

    def new(self, project_name: str):
        # create the models folder if it does not exist
        self._create_models_directory(project_name)
        # create the example.py file if it does not exist
        self._create_tasks_example(project_name)
        self._create_dags_directory(project_name)
        self._create_settings(project_name)

    def _init_workflow(self):
        self._import_all_models()

        self._workflow = Workflow(
            cache_strategy=self.cache_strategy,
            use_cache=self.use_cache,
            executor_type=self.executor,
        )

    def run(self):
        self._init_workflow()
        self._workflow.run()

    def export_dag(self):
        self._init_workflow()
        dag_file_name = os.path.join(
            self.project_name,
            self.dags_folder,
            f'dag_{datetime.now().strftime("%Y%m%d_%H:%M:%S")}',
        )
        self._workflow.export_dag(dag_file_name)

    def _import_all_models(self):
        models_folders = os.path.join(self.project_name, self.models_folder)
        models = [
            file.split(".")[0]
            for file in os.listdir(models_folders)
            if file.endswith(".py")
        ]
        for model in models:
            importlib.import_module(f"{self.project_name}.{self.models_folder}.{model}")

    def _create_models_directory(self, project_name):
        models_project = os.path.join(project_name, self.models_folder)
        if not os.path.exists(models_project):
            os.makedirs(models_project)

    def _create_dags_directory(self, project_name):
        dag_folder = os.path.join(project_name, self.dags_folder)
        if not os.path.exists(dag_folder):
            os.makedirs(dag_folder)

    def _load_settings(self):
        self._settings_module = importlib.import_module(name="settings")

    def _create_settings(self, project_name):
        settings_projects = os.path.join("settings.py")
        if not os.path.exists(settings_projects):
            with open(settings_projects, "w") as file:
                file.write(
                    f"""
PROJECT_NAME = "{project_name}"
EXECUTOR = "ThreadExecutor"
USE_CACHE = True
CACHE_STRATEGY = "LocalCache"
"""
                )

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