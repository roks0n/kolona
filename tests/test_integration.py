import asyncio
from time import time

import pytest

from kolona import Workers, task, workers
from kolona.constants import RUNTIME_ONEOFF
from kolona.exceptions import RetryTask
from kolona.task import Task


@pytest.mark.asyncio
async def test_with_5_tasks_no_exceptions():
    """
    Add 5 tasks to the queue and create 1 workers, all tasks should be processed.
    """
    queue = asyncio.Queue()

    @task(queue=queue)
    async def runner():
        return True

    for _ in range(5):
        await runner.enqueue()

    workers = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 5
    await asyncio.gather(*workers.get())
    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_with_multiple_workers():
    """
    Add 5 tasks to the queue and create 2 workers, all tasks should be processed.
    """
    queue = asyncio.Queue()

    @task(queue=queue)
    async def runner():
        return True

    for _ in range(5):
        await runner.enqueue()

    workers = Workers(queue, "worker", count=2, runtime=RUNTIME_ONEOFF)

    assert workers.count() == 2
    assert queue.qsize() == 5
    await asyncio.gather(*workers.get())
    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_with_retries_exceeded(mocker):
    """
    Queue 1 tasks with max_retry set to 2. All retries should fail resulting in:
    - 2 calls to log.info
    - 1 call to log.warning
    """
    queue = asyncio.Queue()

    log_spy = mocker.spy(workers, "log")

    @task(queue=queue, max_retries=2, retry_intervals=[1, 1, 1])
    async def runner():
        raise Exception("Whoopsy")

    await runner.enqueue()

    worker = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 1
    await asyncio.gather(*worker.get())

    assert log_spy.info.call_count == 2
    for n, l in enumerate(log_spy.info.mock_calls, start=1):
        assert l.args[0] == f"Error occured when processing task runner, retrying: {n}/2"

    assert log_spy.warning.call_count == 1
    for n, l in enumerate(log_spy.warning.mock_calls, start=1):
        assert l.args[0] == "Error processing task runner in last 2 attempts: Whoopsy"

    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_with_more_than_3_retries_exceeded(mocker):
    """
    Queue 1 tasks with max_retry set to 6. All retries should fail resulting in:
    - 6 calls to log.info
    - 1 call to log.warning
    """
    queue = asyncio.Queue()

    log_spy = mocker.spy(workers, "log")

    @task(queue=queue, max_retries=6, retry_intervals=[1, 1, 1])
    async def runner():
        raise Exception("Whoopsy")

    await runner.enqueue()

    worker = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 1
    await asyncio.gather(*worker.get())

    assert log_spy.info.call_count == 6
    for n, l in enumerate(log_spy.info.mock_calls, start=1):
        assert l.args[0] == f"Error occured when processing task runner, retrying: {n}/6"

    assert log_spy.warning.call_count == 1
    for n, l in enumerate(log_spy.warning.mock_calls, start=1):
        assert l.args[0] == "Error processing task runner in last 6 attempts: Whoopsy"

    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_with_retries_ok(mocker):
    """
    Make first process call raise an exception which should trigger a task retry. On the 1st retry
    the task should succeed. This should cause:
    - 1 call to log.info
    """
    queue = asyncio.Queue()

    log_spy = mocker.spy(workers, "log")

    @task(queue=queue, retry_intervals=[1, 1, 1])
    async def runner():
        return True

    # make sure the 1st process call raises an exception
    process_spy = mocker.spy(Task, "process")
    process_spy.side_effect = [Exception("derp"), True]

    for _ in range(1):
        await runner.enqueue()

    worker = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 1
    await asyncio.gather(*worker.get())

    assert log_spy.info.call_count == 1
    for n, l in enumerate(log_spy.info.mock_calls, start=1):
        assert l.args[0] == f"Error occured when processing task runner, retrying: {n}/3"

    assert log_spy.warning.call_count == 0
    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_with_manually_triggered_retries(mocker):
    queue = asyncio.Queue()

    log_spy = mocker.spy(workers, "log")

    @task(queue=queue, retry_intervals=[0.1, 0.1, 0.1])
    async def runner(item, name=None):
        raise RetryTask

    for _ in range(1):
        await runner.enqueue(1337, name="Rough")

    worker = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 1
    await asyncio.gather(*worker.get())

    assert log_spy.info.call_count == 3
    for n, l in enumerate(log_spy.info.mock_calls, start=1):
        assert l.args[0] == f"Retrying task runner: {n}/3"

    assert log_spy.warning.call_count == 1
    for n, l in enumerate(log_spy.warning.mock_calls, start=1):
        # check if extra kwargs are set as expected
        assert l.kwargs["extra"] == {"arg_0": 1337, "name": "Rough"}

    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_with_task_args(mocker):
    queue = asyncio.Queue()

    log_spy = mocker.spy(workers, "log")

    @task(queue=queue, retry_intervals=[0.1, 0.1, 0.1])
    async def runner(item, name=None):
        assert item
        assert name is not None
        raise RetryTask

    for _ in range(1):
        await runner.enqueue(1337, name="Rough")

    worker = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 1
    await asyncio.gather(*worker.get())

    assert log_spy.info.call_count == 3
    for n, l in enumerate(log_spy.info.mock_calls, start=1):
        assert l.args[0] == f"Retrying task runner: {n}/3"

    assert log_spy.warning.call_count == 1
    for n, l in enumerate(log_spy.warning.mock_calls, start=1):
        # check if extra kwargs are set as expected
        assert l.kwargs["extra"] == {"arg_0": 1337, "name": "Rough"}

    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_decorated_task_retains_original_props():
    """
    Decorated taks retains __name__, __doc__ and other magic attributes.
    """
    queue = asyncio.Queue()

    @task(queue=queue)
    async def runner():
        """docs"""
        return True

    assert runner.__name__ == "runner"
    assert runner.__doc__ == "docs"


@pytest.mark.asyncio
async def test_delay():
    queue = asyncio.Queue()

    @task(queue=queue)
    async def runner():
        pass

    await runner.enqueue(delay=2)

    worker = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 1

    start = time()
    await asyncio.gather(*worker.get())
    stop = time()

    assert stop - start > 2
