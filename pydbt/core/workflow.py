from datetime import datetime
import matplotlib.pyplot as plt
import logging
import networkx as nx
from dataclasses import dataclass, field
from typing import List, ClassVar
from pydbt.core.executors import ThreadExecutor
from pydbt.core.cache import CacheInterface
from pydbt.core.dag import Dag
import os
import re


@dataclass
class Workflow(object):
    """Class for running a directed acyclic graph of tasks.

    Attributes:
        tasks (List[Type[Task]]): List of tasks to run in the DAG.
        dag (Dag): DAG object for the tasks.
        executor (ThreadExecutor): Thread executor for running tasks in parallel.
    """

    tasks: ClassVar[List] = field(default=[], init=False)
    dag: Dag = field(init=False)
    executor: ThreadExecutor = field(init=False)
    artifact_name: str = field(default=datetime.now().strftime("%Y%m%d_%H:%M:%S"))
    use_cache: bool = False
    cache_strategy: CacheInterface = field(default=None)
    _base_dir: str = field(default="state")

    def __post_init__(self) -> None:
        """Create a DAG object after initialization."""
        # create the base directory for cache if it does not exist
        os.makedirs(self._base_dir, exist_ok=True)

        # get the latest run task from the cache directory
        latest_file = self.get_latest_run_tasks(self._base_dir)

        # check if cache strategy is provided and use_cache flag is enabled
        if self.cache_strategy and self.use_cache:
            logging.info("cache enabled, fetching latest file")

            # initialize artifact file name, set it to None by default
            artifact_file = None

            # check if there is a latest file available
            if latest_file:
                logging.info(f"using latest run {latest_file}")
                artifact_file = latest_file
            else:
                logging.info(f"No cache file found")
                # set artifact file name to the default
                artifact_file = f"{self._base_dir}/{self.artifact_name}.pkl"

            # set file path
            file_path = f"{self._base_dir}/{artifact_file}"
            self.cache_strategy = self.cache_strategy(file_path)

            # get the old tasks from cache
            olds_tasks = (
                self.cache_strategy.artifact if self.cache_strategy.artifact else []
            )

            # compare the new and old tasks
            if olds_tasks:
                self.tasks = self.compare_tasks(olds_tasks, self.tasks)

        self.dag = Dag(self.tasks)

    def run(self) -> None:
        """Run the tasks in the DAG."""
        # Get a dictionary of all the task levels in the DAG
        levels = self.dag.levels

        # Iterate through each level in the DAG
        for level, task_indexes in levels.items():
            # Skip the first level (level 0) as it contains the root task
            if level != 0:
                logging.info(f"exploring dag level {level}")

                # Create a list of tasks in the current level
                tasks = [task for i, task in enumerate(self.tasks) if i in task_indexes]

                # Get the names of all the tasks in the current level
                tasks_names = [task.name for task in tasks]

                # Calculate the number of threads to use for the tasks set
                nb_threads = len(tasks)
                logging.info(
                    f"collected tasks set {tasks_names} using {nb_threads} threads"
                )

                executor = ThreadExecutor(tasks, nb_threads=nb_threads)
                executor.run()

                # If caching is enabled, dump the current task artifact
                if self.cache_strategy and self.use_cache:
                    self.cache_strategy.dump(
                        artifact=self.tasks,
                        path=f"{self._base_dir}/task_{self.artifact_name}.pkl",
                    )

    def export_dag(self, path: str) -> None:
        """Export the DAG to a PNG image file.

        Args:
            path (str): Path to the directory where the image file should be saved.
        """
        graph = self.dag.graph
        node_names = nx.get_node_attributes(graph, "name")
        pos = nx.spring_layout(graph)
        nx.draw(graph, pos=pos)
        nx.draw_networkx_labels(graph, pos=pos, labels=node_names)
        plt.savefig(f"{path}/dag.png")

    def extract_timestamp(self, file_name):
        # Extract the timestamp string from the file name using a regular expression
        match = re.search(r"_(\d{8}_\d{2}:\d{2}:\d{2})", file_name)
        timestamp_str = match.group(1)

        # Convert the timestamp string to a datetime object
        timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H:%M:%S")

        return timestamp

    def get_latest_run_tasks(self, folder_path: str) -> str:
        """Retrieve the name of the latest run task from a folder.

        Args:
            folder_path (str): The path of the folder to search for the latest run task.

        Returns:
            str: The name of the latest run task file, if found. Returns `None` if no task files are present in the folder.
        """

        # Get a list of all the .pkl files in the folder
        file_list = [f for f in os.listdir(folder_path) if f.endswith(".pkl")]

        # Check if there are no files in the folder
        if not file_list:
            return None

        # Use a list comprehension to extract the timestamps of all the files
        timestamps = [self.extract_timestamp(f) for f in file_list]

        # Find the file with the latest timestamp
        latest_file_index = timestamps.index(max(timestamps))
        latest_file = file_list[latest_file_index]

        return latest_file

    def compare_tasks(self, olds: List, news: List) -> List:
        """Check if the task name is not in the added_names list. If the name is not in the added_names list:
        it finds the corresponding old_task in the olds list with the same name.

        Compare the old_task with the current task to see if they are different.

            If they are different, log that the task is different and add it to the updated list.
            If the old_task and task are equal, log that the task is equal to the previous run and keeping the old_task.
            Also, update the old_task._task with the current task._task and add the old_task to the updated list.

        Args:
            olds (List[Tasks]): tasks list from cache
            news (List[Tasks]): recomputed tasks

        Returns:
            List[Tasks]: delta between olds and news
        """
        logging.info("comparing tasks")

        olds_name = [task.name for task in olds]
        news_name = [task.name for task in news]

        deleted = [task.name for task in olds if task.name not in news_name]
        olds = [task for task in olds if task.name in news_name]
        added = [task for task in news if task.name not in olds_name]
        added_names = [t.name for t in added]

        if deleted:
            logging.info(f"Deleted tasks from previous run: {deleted}")
        if added:
            logging.info(f"New tasks added: {added_names}")
        else:
            logging.info("No added task")
        updated = []

        for task in news:
            if task.name not in added_names:
                old_task = next(t for t in olds if t.name == task.name)
                if old_task != task:
                    logging.info(f"{task.name} is different updating task")
                    updated.append(task)
                else:
                    logging.info(
                        f"{task.name} is equal to previous run keeping old task"
                    )
                    old_task._task = task._task
                    updated.append(old_task)
        return [*added, *updated]
