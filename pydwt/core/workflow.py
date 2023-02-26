import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

import matplotlib.pyplot as plt
import networkx as nx

from pydwt.core.dag import Dag
from pydwt.core.executors import ThreadExecutor


@dataclass
class Workflow(object):
    """Class for running a directed acyclic graph of tasks.

    Attributes:
        tasks (List[Type[Task]]): List of tasks to run in the DAG.
        dag (Dag): DAG object for the tasks.
    """

    tasks: List = field(default_factory=list, init=False)
    dag: Dag = field(init=False)

    def __post_init__(self) -> None:
        """Create a DAG object after initialization."""

        # check if cache strategy is provided and use_cache flag is enabled
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
                nb_workers = len(tasks)
                logging.info(
                    f"collected tasks set {tasks_names} using {nb_workers} threads"
                )
                executor = ThreadExecutor(tasks, nb_workers=nb_workers)
                executor.run()

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
        plt.savefig(f"{path}")
