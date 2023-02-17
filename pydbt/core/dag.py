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
        node_names = {}

        for i, task in enumerate(self.tasks):
            node_names[i] = task.name
            self.graph.add_node(i, name=task.name)

            if task.depends_on:
                for dep_func in task.depends_on:
                    dep_name = f"{dep_func.__module__}.{dep_func.__name__}"
                    if dep_name in node_names.values():
                        dep_index = next(index for index, name in node_names.items() if name == dep_name)
                    else:
                        dep_index = len(node_names)
                        node_names[dep_index] = dep_name
                        self.graph.add_node(dep_index, name=dep_name)

                    edges.append((dep_index, i))
            else:
                edges.append((self.source, i))

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
