import networkx as nx
from pydbt.core.task import Task
from pydbt.core.dag import Dag
import pytest
import functools

from pydbt.core.containers import Container


container = Container()
container.wire(modules=["pydbt.core.task"])


@pytest.fixture
def fake_task_one():
    def inner_func():
        pass

    return inner_func


@pytest.fixture
def fake_task_two():
    def inner_func_bis():
        pass

    return inner_func_bis


def test_build_dag(fake_task_one, fake_task_two):
    """Test that the dag is built correctly."""
    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)

    dag = Dag(tasks=[task1, task2])
    dag.build_dag()

    assert list(dag.graph.nodes()) == [0, 1, "s"]
    assert list(dag.graph.edges()) == [(0, 1), ("s", 0)]
    assert dag.graph.nodes[0]["name"] == "tests.test_dags.inner_func"
    assert dag.graph.nodes[1]["name"] == "tests.test_dags.inner_func_bis"


def test_build_level(fake_task_one, fake_task_two):
    """Test that the levels are assigned correctly."""
    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)

    dag = Dag(tasks=[task1, task2])
    dag.build_dag()
    dag.build_level()

    assert dag.levels == {0: ["s"], 1: [0], 2: [1]}
