import asyncio

import pytest

from kolona import workers
from kolona.constants import RUNTIME_ONEOFF
from kolona.task import Task, task
from kolona.workers import Workers


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
        raise Exception("Woopsy")

    await runner.enqueue()

    worker = Workers(queue, "worker", count=1, runtime=RUNTIME_ONEOFF)

    assert queue.qsize() == 1
    await asyncio.gather(*worker.get())

    assert log_spy.info.call_count == 2
    for n, l in enumerate(log_spy.info.mock_calls, start=1):
        assert l.args[0] == f"Failed to process, retrying task: {n}/2"

    assert log_spy.warning.call_count == 1
    for n, l in enumerate(log_spy.warning.mock_calls, start=1):
        assert l.args[0] == "Failed to retry task in 2 attempts"

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
        assert l.args[0] == f"Failed to process, retrying task: {n}/3"

    assert log_spy.warning.call_count == 0
    assert queue.qsize() == 0
