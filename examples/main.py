"""
This is an example how to create a task queue using Kolona.
"""

import asyncio
import copy
from datetime import datetime
from time import time

from kolona import Workers, task

# create a queue
QUEUE_CRON: asyncio.Queue = asyncio.Queue()
QUEUE_MAIN: asyncio.Queue = asyncio.Queue()
COUNTER = 0


class ConnectionDerp(Exception):
    pass


@task(queue=QUEUE_MAIN, max_retries=2)
async def purchase_listener(counter):
    """
    a task that "simulates" some sort of work being done with the 1sec sleep
    """

    print(f"{datetime.now()} purchase_processor: starting: {counter}")
    await asyncio.sleep(1)
    print(f"{datetime.now()} purchase_processor: finished: {counter}")


async def connection_error():
    """
    for testing purposes of this example, we want to trigger an exception that causes the process
    to reinitialize all the workers
    """

    while True:
        global COUNTER
        if COUNTER == 3:
            raise ConnectionDerp
        await asyncio.sleep(1)


async def cron_worker():
    """
    example of a cronjob worker that triggers a purchase_listener task periodically
    """

    jobs = [
        {
            "task": purchase_listener,
            "interval": 0.5,
            "last_run": 0,
        }
    ]

    while True:
        global COUNTER
        for job in jobs:
            last_run = job["last_run"]
            interval = job["interval"]
            if int(time()) - last_run >= interval:
                # this will update last_run attribute for this specific item in the jobs dict
                job["last_run"] = int(time())
                COUNTER += 1
                print(f"queueing task: {COUNTER}")
                await job["task"].enqueue(copy.deepcopy(COUNTER))
        await asyncio.sleep(0.1)


async def main():
    while True:
        workers = []
        try:
            if len(workers) == 0:
                # start a cron worker that schedules some tasks, connection_error is added to
                # simulate a disturbance that would cause all tasks to be reinitialized again
                workers.extend(
                    [
                        asyncio.create_task(cron_worker()),
                        asyncio.create_task(connection_error()),
                    ]
                )

                # start 3 workers that process tasks from the QUEUE_MAIN
                w = Workers(queue=QUEUE_MAIN, name="worker", count=3).get()
                workers.extend(w)

            await asyncio.gather(*workers)
        except ConnectionDerp:
            print("reconnecting ...")
        finally:
            # cleanup workers so you don't accidentally run 2x the workers in parallel, If you don't
            # clean them up you will end up with two cron_worker's running in parallel in case of a
            # `ConnectionDerp` error!
            for w in workers:
                w.cancel()
            continue


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        exit()
