import asyncio
from functools import update_wrapper
from time import time
from typing import Callable, List, Optional, Union
from uuid import uuid4

Seconds = Union[float, int]


class GlobalTask:
    """
    GlobalTask class used to instantiate a task using a function decorator
    """

    retry_attempt = 0
    max_retries: int
    retry_intervals: List[int] = [3, 5, 15]
    last_attempt_time = 0
    delay: Seconds = 0
    func: Optional[Callable] = None
    queue: Optional[asyncio.Queue] = None

    def __init__(
        self,
        func: Callable,
        max_retries: int,
        queue: asyncio.Queue = None,
        last_attempt_time=0,
        retry_attempt=0,
        retry_intervals: List = None,
        delay: Seconds = 0,
    ):

        self.max_retries = max_retries
        self.retry_intervals = retry_intervals if retry_intervals else self.retry_intervals

        if max_retries < 3:
            self.retry_intervals = self.retry_intervals[:max_retries]
        elif max_retries > 3:
            self.retry_intervals = self.retry_intervals + [
                self.retry_intervals[-1] for _ in range(max_retries - len(self.retry_intervals))
            ]

        self.queue = queue
        self.func = func
        self.retry_attempt = retry_attempt
        self.last_attempt_time = last_attempt_time
        self.delay = delay

    async def enqueue(
        self,
        *args,
        queue: asyncio.Queue = None,
        delay: Optional[Seconds] = None,
        id: str = None,
        **kwargs,
    ) -> str:
        """
        Enqueue a task adds a self-contained `Task` item into the queue with its own context
        """
        q = queue if queue else self.queue

        if not q:
            raise Exception("No queue specified")

        if delay is None:
            delay = self.delay

        task_item = Task(
            self.func,
            queue=q,
            task_args=args,
            task_kwargs=kwargs,
            retry_attempt=self.retry_attempt,
            last_attempt_time=self.last_attempt_time,
            max_retries=self.max_retries,
            retry_intervals=self.retry_intervals,
            delay=delay,
            id=id,
        )
        await q.put(task_item)
        return task_item.id


class Task(GlobalTask):
    """
    Task class holding context and methods for each individual `Task`
    """

    created_at: float

    def __init__(self, *args, delay: Seconds, **kwargs):
        # generate a new task id if one is not already assigned
        task_id = kwargs.pop("id")
        self.id: str = task_id if task_id else str(uuid4())

        # get task specific args and kwargs and don't pass them to the parent as it doesn't know
        # what to do with it
        task_args = kwargs.pop("task_args")
        task_kwargs = kwargs.pop("task_kwargs")

        super().__init__(*args, **kwargs)

        self.args = task_args
        self.kwargs = task_kwargs
        self.created_at = time()
        self.delay = delay

    async def process(self):
        """
        Process this specific task
        """
        await self.func(*self.args, **self.kwargs)

    def is_ready(self):
        """
        Check if task is ready to be processed.
          * If a task was delayed it's not ready until the time passes.
          * After retrying a `Task` there is a cooldown period
            during which task can not be processed.
        """
        if time() < self.created_at + self.delay:
            return False

        if self.last_attempt_time == 0 or self.retry_attempt == 0:
            return True

        backoff_time = self.retry_intervals[self.retry_attempt - 1]
        if (int(time()) - self.last_attempt_time) <= backoff_time:
            return False

        return True

    def done(self):
        """
        Mark this specific task as complete
        """
        self.queue.task_done()

    async def retry(self):
        """
        Retry this task using the same context data
        """
        self.retry_attempt += 1
        self.last_attempt_time = int(time())
        self.done()
        await self.enqueue(*self.args, **self.kwargs, id=self.id)

    def can_retry(self):
        return self.retry_attempt < self.max_retries


def task(
    queue: asyncio.Queue = None,
    max_retries: int = 3,
    retry_intervals=None,
    delay: Seconds = 0,
):
    """
    @task decorator wraps a function into a GlobalTask which can create a self-contained task object
    """

    def wrapper(func):
        task = GlobalTask(
            func,
            queue=queue,
            max_retries=max_retries,
            retry_intervals=retry_intervals,
            delay=delay,
        )
        update_wrapper(task, func)
        return task

    return wrapper
