from pydwt.core.task import Task
from pydwt.core.dag import Dag
import pytest
import unittest.mock

from pydwt.core.containers import Container
from pydwt.core.enums import Status

container = Container()
container.database_client.override(unittest.mock.Mock())
container.wire(modules=["pydwt.core.task"])


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
    dag = Dag()
    dag.tasks = [task1, task2]
    dag.build_dag()
    return dag


def test_dag_init():
    def fake_task_one():
        pass

    def fake_task_two():
        pass

    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)
    dag = Dag()
    dag.tasks = [task1, task2]
    assert dag


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
    levels = dag.build_level()
    assert levels == {0: ["s"], 1: [0], 2: [1]}


def test_dag_check_parents_status_error():
    def fake_task_one():
        raise ValueError("fake error")

    def fake_task_two():
        pass

    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)
    dag = Dag()
    dag.tasks = [task1, task2]
    dag.build_dag()
    task1.run()
    task2.run()
    assert dag.check_parents_status(task2) == Status.ERROR


def test_dag_check_parents_status_success():
    def fake_task_one():
        pass

    def fake_task_two():
        pass

    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)
    dag = Dag()
    dag.tasks = [task1, task2]
    dag.build_dag()
    task1.run()
    task2.run()
    assert dag.check_parents_status(task2) == Status.SUCCESS


def test_dag_build_level_task_name():
    def fake_task_one():
        pass

    def fake_task_two():
        pass

    def fake_task_three():
        pass

    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)

    task3 = Task(depends_on=[fake_task_one])
    task3(fake_task_three)

    dag = Dag()
    dag.tasks = [task1, task2, task3]
    dag.build_dag()
    levels = dag.build_level(target="tests.test_dags.fake_task_three")
    assert levels == {0: ["s"], 1: [0], 2: [2]}


def test_filter_dag():
    def fake_task_one():
        pass

    def fake_task_two():
        pass

    def fake_task_three():
        pass

    def fake_task_four():
        pass

    def fake_task_five():
        pass

    task1 = Task(retry=2)
    task1(fake_task_one)

    task2 = Task(depends_on=[fake_task_one])
    task2(fake_task_two)

    task3 = Task(depends_on=[fake_task_one])
    task3(fake_task_three)

    task4 = Task(depends_on=[fake_task_two, fake_task_three])
    task4(fake_task_four)

    task5 = Task(depends_on=[fake_task_two])
    task5(fake_task_five)

    dag = Dag()
    dag.tasks = [task1, task2, task3, task4, task5]
    dag.build_dag()
    dag.filter_dag(task_name="tests.test_dags.fake_task_four")
    taks_name = [task.name for task in dag.tasks]
    assert taks_name == [
        "tests.test_dags.fake_task_one",
        "tests.test_dags.fake_task_two",
        "tests.test_dags.fake_task_three",
        "tests.test_dags.fake_task_four",
    ]
