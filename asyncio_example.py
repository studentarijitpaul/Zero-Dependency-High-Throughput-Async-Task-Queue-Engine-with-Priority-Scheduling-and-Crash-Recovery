import asyncio


async def worker(worker_id: int, queue: asyncio.Queue):
    while True:
        task_name = await queue.get()

        try:
            print(f"worker{worker_id} processing {task_name}")
            await asyncio.sleep(3)

        finally:
            queue.task_done()


async def main():
    queue = asyncio.Queue()

    # Add jobs
    for i in range(100):
        queue.put_nowait(f"job{i}")

    # Start 3 workers
    workers = [
        asyncio.create_task(worker(i, queue))
        for i in range(3)
    ]

    # Wait until all jobs are completed
    await queue.join()

    # Stop workers
    for worker_task in workers:
        worker_task.cancel()

    # Wait for workers to finish cancellation
    await asyncio.gather(
        *workers,
        return_exceptions=True
    )


asyncio.run(main())