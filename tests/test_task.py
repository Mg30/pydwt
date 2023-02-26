from pydwt.core.task import Task
import pytest
from unittest import mock
from pydwt.core.containers import Container
from pydwt.core.schedule import Monthly

container = Container()
container.database_client.override(mock.Mock())
container.wire(modules=["pydwt.core.task"])

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


def test_task_not_scheduled(fake_task_one):
    task = Task(runs_on=Monthly())
    task(fake_task_one)
    task.run()
    assert task._count_call == 0

def test_task_is_scheduled(fake_task_one):
    task = Task()
    task(fake_task_one)
    task.run()
    assert task._count_call == 1

def test_task_eq(fake_task_one):
    task = Task(retry=2)
    task(fake_task_one)

    task2 = Task(retry=2)
    task2(fake_task_one)

    assert task == task2