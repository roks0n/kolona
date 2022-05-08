import asyncio
from typing import List

from kolona.constants import RUNTIME_INFINITY, RUNTIME_ONEOFF
from kolona.logger import log
from kolona.task import Task

QUEUE: asyncio.Queue = asyncio.Queue()


class Workers:
    loop = None
    _workers: List[asyncio.tasks.Task] = []
    _queue: asyncio.Queue
    _runtime: int

    def __init__(self, queue: asyncio.Queue, name: str, count=1, runtime=RUNTIME_INFINITY):
        self._queue = queue
        self.loop = asyncio.get_event_loop()

        self._runtime = runtime

        workers = []
        for nr in range(1, 1 + count):
            n = f"{name}-{nr}"
            workers.append(asyncio.create_task(self.processor(n), name=n))
        self._workers = workers

    async def processor(self, name):
        log.debug(f"Starting worker: {name}")

        while True:
            if self._runtime == RUNTIME_ONEOFF and self._queue.empty():
                break

            task: Task = await self._queue.get()
            if not task.is_ready():
                # task is not ready to be processed, add it back to the queue
                await self._queue.put(task)
                continue

            try:
                await task.process()
            except Exception as e:
                log.exception(e)
                if task.can_retry():
                    await task.retry()
                    log.info(
                        f"Failed to process, retrying task: {task.retry_attempt}/{task.max_retries}"
                    )
                    continue
                else:
                    # task has been retried several times without successfully being processed,
                    # don't process this task anymore
                    task.done()
                    log.warning(f"Failed to retry task in {task.max_retries} attempts")
                    continue
            else:
                # mark task as done when successfully processed
                task.done()

    def get(self):
        return self._workers

    def count(self):
        return len(self._workers)
