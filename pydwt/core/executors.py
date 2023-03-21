import queue
import threading
from abc import ABC
from dataclasses import dataclass, field
from typing import Any, List
from pydwt.core.enums import Status
import logging


class AbstractExecutor(ABC):
    nb_workers: int
    _queue: Any
    _tasks: List = field(init=False)

    @property
    def tasks(self):
        return self._tasks

    @tasks.setter
    def tasks(self, value):
        self._tasks = value

    def run(self):
        raise NotImplementedError

    def worker(self):
        raise NotImplementedError


@dataclass
class ThreadExecutor(AbstractExecutor):
    dag: Any
    nb_workers: int = 2
    _queue: queue.Queue = field(init=False, default_factory=queue.Queue)

    def run(self) -> None:
        """Run all workers"""
        for task in self.tasks:
            self._queue.put(task)

        for _ in range(0, self.nb_workers):
            threading.Thread(target=self.worker, daemon=True).start()
        self._queue.join()

    def worker(self) -> None:
        """Pull a task from the queue and process"""
        while not self._queue.empty():
            task = self._queue.get()
            try:
                parents_status = self.dag.check_parents_status(task)
                if parents_status == Status.ERROR:
                    logging.error(
                        f"task {task.name} can not be run because\
                        some parent are in ERROR"
                    )
                    task.status = Status.ERROR
                elif parents_status == Status.PENDING:
                    logging.info(f"task {task.name} is pending")
                    self._queue.put(task)
                else:
                    task.run()
            except Exception as e:
                logging.error(f"task {task.name} failed with error: {e}")
                task.status = Status.ERROR
            finally:
                self._queue.task_done()
