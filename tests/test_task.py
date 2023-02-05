from pydbt.core.task import Task
import time
import pytest


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
    task = Task(retry=2, ttl_minutes=30)
    task(fake_task_one)

    task2 = Task()
    task2(fake_task_two)

    assert task != task2

def test_task_no_retry_run_once(fake_task_one):
    task = Task()
    task(fake_task_one)
    task.run()
    assert task._count_call == 1

def test_task_run_with_retry():
    task = Task(retry=2)

    def task1():
        raise Exception("test exception")

    task(task1)
    task.run()
    assert task._count_call == 3

def test_ttl_expired(fake_task_one):

    task = Task(ttl_minutes=0.1) # 6 sec
    task(fake_task_one)
    task.run()
    time.sleep(10)

    assert task.is_ttl_elapsed