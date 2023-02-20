from pydbt.core.task import Task
from pydbt.core.dag import Dag
import pytest
import unittest.mock

from pydbt.core.containers import Container


container = Container()
container.database_client.override(unittest.mock.Mock())
container.wire(modules=["pydbt.core.task"])





@pytest.fixture
def dag():

    def fake_task_one():
        pass
    def fake_task_two():
        pass

    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)
    dag = Dag(tasks=[task1, task2])
    dag.build_dag()
    return dag

def test_build_dag_nodes(dag):
    """Test that the dag is built correctly."""
    assert list(dag.graph.nodes()) == [0, 1, "s"]


def test_build_dag_edges(dag):
    """Test that the dag is built correctly."""
    assert list(dag.graph.edges()) == [(0, 1), ("s", 0)]


def test_build_dag_node_name(dag):
    """Test that the dag is built correctly."""
    assert dag.graph.nodes[0]["name"] == "tests.test_dags.fake_task_one"



def test_build_level(dag):
    """Test that the levels are assigned correctly."""
    dag.build_level()
    assert dag.levels == {0: ["s"], 1: [0], 2: [1]}
