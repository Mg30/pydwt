from dataclasses import dataclass, field
from typing import List
import threading
import queue


@dataclass
class ThreadExecutor(object):

    tasks: List
    nb_threads: int = 2
    _queue: queue.Queue = field(init=False, default_factory=queue.Queue)

    def __post_init__(self) -> None:
        """Put all tasks to the queue"""
        for task in self.tasks:
            self._queue.put(task)

    def run(self) -> None:
        """Run all workers"""

        for _ in range(0, self.nb_threads):
            threading.Thread(target=self.worker, daemon=True).start()
        self._queue.join()

    def worker(self) -> None:
        """Pull a task from the queue and process"""
        while not self._queue.empty():
            task = self._queue.get()
            task.run()
            self._queue.task_done()

