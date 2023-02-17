import functools
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Dict, List

from dependency_injector.wiring import Provide

from pydbt.core.containers import Container
from pydbt.core.schedule import Daily, ScheduleInterface
from pydbt.core.workflow import Workflow


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
    ttl_minutes: int = 0
    name: str = field(init=False)
    _next_run: datetime = field(init=False, default=None)
    _task: Callable = field(init=False, default=None)
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

    @property
    def is_ttl_elapsed(self):
        return self._next_run < datetime.now()

    def set_next_run(self):
        """
        Set the time for the next run of this task.
        """
        now = datetime.now()
        self._next_run = now + timedelta(minutes=self.ttl_minutes)
        logging.info(
            f"task {self.name}: found ttl definition next run: { self._next_run.strftime('%Y%m%d_%H:%M:%S')}"
        )

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

    def __eq__(self, other):
        if isinstance(other, BaseTask):
            return (
                set(self.depends_on_name) == set(other.depends_on_name)
                and self.runs_on_name == other.runs_on_name
                and self.retry == other.retry
                and self.ttl_minutes == other.ttl_minutes
                and self.name == other.name
            )
        return False

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

        if self.ttl_minutes > 0 and not self._next_run:
            self._run_task_with_retry()
            self.set_next_run()
        elif self._next_run:
            if not self.is_ttl_elapsed:
                logging.info(f"task {self.name}: ttl not elasped: skipping")
            else:
                self._run_task_with_retry()
                self.set_next_run()
        else:
            self._run_task_with_retry()

    def _run_task_with_retry(self):
        self._count_call = 0
        for n in range(self.retry + 1):
            try:
                task_config = self.config["tasks"][self.name]
                self._count_call += 1
                self._task(task_config, self.sources)
                break
            except Exception as e:
                if n == self.retry:
                    logging.error(
                        f"task  {self.name} failed after {self.retry} attempts: {e}"
                    )

                else:
                    logging.info(f"retrying task {self.name} try number: {n}")


@dataclass
class AsyncTask(BaseTask):
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

    async def run(self):
        """
        Run this task.
        """
        if not self.runs_on.is_scheduled():
            logging.info(f"task {self.name} is not scheduled to be run: skipping")
            return

        logging.info(f"task {self.name} is scheduled to be run")

        if self.ttl_minutes > 0 and not self._next_run:
            await self._run_task_with_retry()
            self.set_next_run()
        elif self._next_run:
            if not self.is_ttl_elapsed:
                logging.info(f"task {self.name}: ttl not elasped: skipping")
            else:
                await self._run_task_with_retry()
                self.set_next_run()
        else:
            await self._run_task_with_retry()

    async def _run_task_with_retry(self):
        self._count_call = 0
        for n in range(self.retry + 1):
            try:
                self._count_call += 1
                await self._task()
                break
            except Exception as e:
                if n == self.retry:
                    logging.error(
                        f"task  {self.name} failed after {self.retry} attempts: {e}"
                    )

                else:
                    logging.info(f"retrying task {self.name} try number: {n}")
