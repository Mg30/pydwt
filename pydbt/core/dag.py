import networkx as nx
from typing import Dict, List


class Dag(object):
    """DAG class to handle graph creation, traversal and saving the output.

    Attributes:
        tasks (List): List of tasks to build the dag from.
        graph (nx.DiGraph): Directed Graph that holds the task relationships.
    """

    def __init__(self, tasks: List) -> None:
        """Build the DAG after object initialization."""
        self.tasks = tasks
        self.graph = nx.DiGraph()
        self.source = "s"
        self.build_dag()

    @property
    def levels(self) -> Dict[int, List[int]]:
        """Get the level of each task in the dag.

        Returns:
            Dict[int, List[int]]: Dictionary of levels and their corresponding task indexes.
        """
        self.build_level()
        return self.nodes_by_level

    def build_dag(self) -> None:
        """Build the directed acyclic graph from the tasks and their dependencies."""
        edges = []
        tasks_names = [t.name for t in self.tasks]
        for index, _ in enumerate(tasks_names):
            task = self.tasks[index]
            self.graph.add_node(index, name=task.name)

            if task.depends_on:
                task_edge = [
                    (tasks_names.index(f"{func.__module__}.{func.__name__}"), index)
                    for func in task.depends_on
                ]
                edges = [*edges, *task_edge]
            else:
                edges.append((self.source, index))

        self.graph.add_edges_from(edges)

    def build_level(self) -> None:
        """Assign levels to nodes based on the breadth-first search."""
        # Perform breadth-first search on the graph
        bfs_tree = nx.bfs_tree(self.graph, self.source)
        # Assign levels to nodes based on their distance from the root node
        level = nx.shortest_path_length(bfs_tree, self.source)
        self.nodes_by_level = {}
        for node, node_level in level.items():
            if node_level not in self.nodes_by_level:
                self.nodes_by_level[node_level] = []
            self.nodes_by_level[node_level].append(node)
