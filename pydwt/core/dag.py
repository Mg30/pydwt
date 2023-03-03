import networkx as nx
from typing import Dict, List
from pydwt.core.enums import Status


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
        self.node_index = {}
        self.node_names = {}

    def build_dag(self) -> None:
        """Build the directed acyclic graph from the tasks and their dependencies."""
        edges = []

        for i, task in enumerate(self.tasks):
            self.node_names[i] = task.name
            self.node_index[task.name] = i
            self.graph.add_node(i, name=task.name)

            if task.depends_on:
                for dep_func in task.depends_on:
                    dep_name = f"{dep_func.__module__}.{dep_func.__name__}"
                    if dep_name in self.node_names.values():
                        dep_index = next(
                            index
                            for index, name in self.node_names.items()
                            if name == dep_name
                        )
                    else:
                        dep_index = len(self.node_names)
                        self.node_names[dep_index] = dep_name
                        self.graph.add_node(dep_index, name=dep_name)

                    edges.append((dep_index, i))
            else:
                edges.append((self.source, i))

        self.graph.add_edges_from(edges)

    def build_level(self, target: str = None) -> Dict:
        """Assign levels to nodes in the dag using the breadth-first search.

        Args:
            target (str): Optional target node. If provided, the search will be performed up to this node.

        Returns:
            Dict: Dictionary of levels and their corresponding node indexes.

        Raises:
            KeyError: If target node is not found in the graph.
        """
        nodes_by_level = {}
        # If a target node is provided, get its index in the node_index dictionary
        node_index = self.node_index.get(target, None)
        # Perform breadth-first search on the graph
        bfs_tree = nx.bfs_tree(self.graph, self.source)

        level = None

        if node_index:
            level = {}
            path = nx.shortest_path(self.graph, self.source, node_index)
            print(path)
            for i,node in enumerate(path):
                level[node] = i

        else:
        # Assign levels to nodes based on their distance from the root node
            level = nx.shortest_path_length(bfs_tree, self.source)
        for node, node_level in level.items():
            if node_level not in nodes_by_level:
                nodes_by_level[node_level] = []
            nodes_by_level[node_level].append(node)
        return nodes_by_level

    def check_parents_status(self, task):
        """Check if all parent tasks have the attribute status set to success.

        Args:
            task: The task to check.

        Returns:
            bool: True if all parent tasks have the attribute status set to success, False otherwise.
        """
        node_index = self.node_index[task.name]
        for parent_index in self.graph.predecessors(node_index):
            if parent_index == "s":
                continue
            parent = self.tasks[parent_index]
            if parent.status == Status.ERROR or parent.status == Status.PENDING:
                return False
        return True
