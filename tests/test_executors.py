from unittest.mock import MagicMock
from pydwt.core.executors import ThreadExecutor


class TestThreadExecutor:
    def test_thread_executor_runs_all_tasks(self):
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        tasks = [mock_task1, mock_task2]

        executor = ThreadExecutor(tasks=tasks)
        executor.run()

        mock_task1.run.assert_called_once()
        mock_task2.run.assert_called_once()

    def test_thread_executor_runs_tasks_in_parallel(self):
        mock_task1 = MagicMock()
        mock_task2 = MagicMock()
        tasks = [mock_task1, mock_task2]

        executor = ThreadExecutor(tasks=tasks, nb_workers=2)
        executor.run()

        assert mock_task1.run.call_count == 1
        assert mock_task2.run.call_count == 1

    def test_thread_executor_runs_no_tasks_if_none_are_provided(self):
        executor = ThreadExecutor(tasks=[])
        executor.run()