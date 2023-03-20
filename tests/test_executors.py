import unittest
from pydwt.core.containers import Container
from pydwt.core.task import Task
from pydwt.core.dag import Dag
from pydwt.core.executors import ThreadExecutor
import pytest


container = Container()
container.wire(modules=["pydwt.core.task"])


@pytest.fixture
def fake_task_one():
    def inner_func_one():
        pass

    return inner_func_one


@pytest.fixture
def fake_task_two():
    def inner_func_two():
        pass

    return inner_func_two


@pytest.fixture
def fake_task_three():
    def inner_func_third():
        raise ValueError("fake error")

    return inner_func_third


def test_thread_executor_runs_all_tasks(fake_task_one, fake_task_two):
    task = Task(retry=2)
    task(fake_task_one)

    task2 = Task()
    task2(fake_task_two)
    tasks = [task, task2]
    dag = Dag()
    dag.tasks = tasks
    dag.build_dag()
    executor = ThreadExecutor(dag)
    executor.tasks = tasks
    executor.run()

    assert task2._count_call == 1
    assert task._count_call == 1


def test_thread_executor_no_when_parent_is_error(fake_task_one, fake_task_two, fake_task_three):
    task = Task(retry=2)
    task(fake_task_one)

    task3 = Task()
    task3(fake_task_three)

    task2 = Task(depends_on=[fake_task_one, fake_task_three])
    task2(fake_task_two)

    tasks = [task2, task3, task]

    dag = Dag()
    dag.tasks = tasks
    dag.build_dag()

    executor = ThreadExecutor(dag)
    executor.tasks = tasks

    executor.run()

    assert task2._count_call == 0

