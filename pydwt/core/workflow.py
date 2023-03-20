import logging
import time

from dataclasses import dataclass, field
from typing import List

import matplotlib.pyplot as plt
import networkx as nx

from pydwt.core.dag import Dag
from pydwt.core.executors import AbstractExecutor


@dataclass
class Workflow(object):
    """Class for running a directed acyclic graph of tasks.

    Attributes:
        tasks (List[Type[Task]]): List of tasks to run in the DAG.
        dag (Dag): DAG object for the tasks.
    """

    tasks: List = field(default_factory=list, init=False)
    dag: Dag
    executor: AbstractExecutor

    def __post_init__(self) -> None:
        """Create a DAG object after initialization."""

        self.dag.tasks = self.tasks
        self.executor.tasks = self.tasks

    def run(self) -> None:
        """Run the tasks in the DAG."""
        self.dag.build_dag()
        start_time_workflow = time.time()
        self.executor.run()
        elapsed_time_workflow = time.time() - start_time_workflow
        logging.info(f"workflow completed in {elapsed_time_workflow:.2f} seconds")

    def run_with_name_and_deps(self, task_name: str) -> None:
        """Run the tasks in the DAG."""
        self.dag.build_dag()
        self.dag.filter_dag(task_name)

        self.tasks = self.dag.tasks
        self.executor.tasks = self.tasks

        start_time_workflow = time.time()
        self.executor.run()
        elapsed_time_workflow = time.time() - start_time_workflow
        logging.info(f"workflow completed in {elapsed_time_workflow:.2f} seconds")

    def run_with_name_no_deps(self, task_name: str) -> None:
        """Run the tasks in the DAG."""
        task = next(task for task in self.tasks if task.name == task_name)
        task.run()

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
