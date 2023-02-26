import functools
import logging
import traceback

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Dict, List

from dependency_injector.wiring import Provide

from pydwt.core.containers import Container
from pydwt.core.schedule import Daily, ScheduleInterface
from pydwt.core.workflow import Workflow


@dataclass
class BaseTask(ABC):
    """
    Abstract base class for representing a task in a DAG.

    :param depends_on: List of other tasks that this task depends on
    :param runs_on: Schedule for running this task. Default is `Daily()`
    :param retry: Number of times to retry this task in case of failure
    :param ttl_minutes: Time-to-live in minutes. If a positive value is provided, the task will only run
                        if the time elapsed since the last run is greater than or equal to this value.
    """

    depends_on: List[Callable] = field(default_factory=list)
    runs_on: ScheduleInterface = field(default_factory=Daily)
    retry: int = 0
    name: str = field(init=False)
    _task: Callable = field(init=False, default=None)
    _count_call: int = 0
    workflow: Workflow = Provide[Container.workflow_factory]
    config: Dict = Provide[Container.config]
    sources: Dict = Provide[Container.datasources]

    @property
    def depends_on_name(self):
        return (
            [f"{func.__module__}.{func.__name__}" for func in self.depends_on]
            if self.depends_on
            else []
        )

    @property
    def runs_on_name(self):
        return str(type(self.runs_on).__name__)

    def __call__(self, func: Callable):
        """
        Decorator for registering a task in the DAG.
        """
        self._task = func
        self.name = f"{func.__module__}.{func.__name__}"

        logging.info(f"registering task {self.name}")
        self.workflow.tasks.append(self)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    @abstractmethod
    def run(self):
        """
        Run this task.
        """
        raise NotImplementedError

    @abstractmethod
    def _run_task_with_retry(self):
        raise NotImplementedError


@dataclass
class Task(BaseTask):
    """
    Class representing a task in a DAG.

    :param depends_on: List of other tasks that this task depends on
    :param runs_on: Schedule for running this task. Default is `Daily()`
    :param retry: Number of times to retry this task in case of failure
    :param ttl_minutes: Time-to-live in minutes. If a positive value is provided, the task will only run
                        if the time elapsed since the last run is greater than or equal to this value.
    """

    workflow: Workflow = Provide[Container.workflow_factory]
    config: Dict = Provide[Container.config]
    sources: Dict = Provide[Container.datasources]

    def run(self):
        """
        Run this task.
        """
        if not self.runs_on.is_scheduled():
            logging.info(f"task {self.name} is not scheduled to be run: skipping")
            return

        logging.info(f"task {self.name} is scheduled to be run")
        self._run_task_with_retry()

    def __eq__(self, other):
        if isinstance(other, Task):
            return (
                set(self.depends_on_name) == set(other.depends_on_name)
                and self.runs_on_name == other.runs_on_name
                and self.retry == other.retry
                and self.name == other.name
            )
        return False

    def _run_task_with_retry(self):
        self._count_call = 0
        for n in range(self.retry + 1):
            try:
                self._count_call += 1
                self._task()
                break
            except Exception as e:
                if n == self.retry:
                    logging.error(
                        f"task  {self.name} failed after {self.retry} attempts: {traceback.print_exc()}"
                    )

                else:
                    logging.info(f"retrying task {self.name} try number: {n}")

