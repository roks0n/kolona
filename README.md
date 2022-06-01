# Kolona

A minimalistic in-memory async Python Task queue.

## Install

`pip install kolona`

## Features

- Retry tasks
- Multiple workers
- In-memor only (no 3rd party message brokers required)
- Python native using asyncio
- Statically typed

## Example

```py
import asyncio

from kolona import task, Workers

# create a queue
QUEUE = asyncio.Queue()


@task(queue=QUEUE, max_retries=2)
async def purchase_listener(*args):
    print(f"purchase_listener -> args: {args}")


@task(queue=QUEUE, max_retries=3)
async def account_checker(*args):
    print(f"account_checker doing checking")


async def main():
    # queue 1 account_checker task
    await account_checker.enqueue()

    # queue 3 purchase_listener tasks, pass an argument into it
    for x in range(3):
        print(f"Queueing: task-{x}")
        await purchase_listener.enqueue(f"task-{x}")

    # start 3 workers
    workers = Workers(queue=QUEUE, name="worker", count=3)

    # block on workers
    await asyncio.gather(*workers.get())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```
