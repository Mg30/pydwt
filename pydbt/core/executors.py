from dataclasses import dataclass, field
from typing import List, Any
import threading
import queue
import asyncio
from abc import ABC


class AbstractExecutor(object):
    tasks: List
    nb_workers: int
    _queue: Any

    def run(self):
        raise NotImplementedError

    def worker(self):
        raise NotImplementedError


@dataclass
class ThreadExecutor(AbstractExecutor):
    tasks: List
    nb_workers: int = 2
    _queue: queue.Queue = field(init=False, default_factory=queue.Queue)

    def __post_init__(self) -> None:
        """Put all tasks to the queue"""
        for task in self.tasks:
            self._queue.put(task)

    def run(self) -> None:
        """Run all workers"""

        for _ in range(0, self.nb_workers):
            threading.Thread(target=self.worker, daemon=True).start()
        self._queue.join()

    def worker(self) -> None:
        """Pull a task from the queue and process"""
        while not self._queue.empty():
            task = self._queue.get()
            task.run()
            self._queue.task_done()


@dataclass
class AsyncExecutor(AbstractExecutor):
    tasks: List
    nb_workers: int = 2
    _queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    def __post_init__(self):
        for task in self.tasks:
            self._queue.put_nowait(task)

    async def run(self):
        """Rull all workers"""
        tasks = []

        for _ in range(0, self.nb_workers):
            task = asyncio.create_task(self.worker())
            tasks.append(task)

        await self._queue.join()
        # Cancel our worker tasks.
        for task in tasks:
            task.cancel()
        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

    async def worker(self):
        while True:
            # Get a "work item" out of the queue.
            task = await self._queue.get()
            await task.run()
            self._queue.task_done()
