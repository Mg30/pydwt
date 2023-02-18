from pydbt.core.task import Task
import pytest
import unittest.mock
from pydbt.core.containers import Container


container = Container()
container.database_client.override(unittest.mock.Mock())
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



def test_task_not_eq(fake_task_one, fake_task_two):


    task = Task(retry=2)
    task(fake_task_one)

    task2 = Task()
    task2(fake_task_two)

    assert task != task2

def test_task_no_retry_run_once(fake_task_one):
    task = Task()
    task(fake_task_one)
    task.run()
    assert task._count_call == 1