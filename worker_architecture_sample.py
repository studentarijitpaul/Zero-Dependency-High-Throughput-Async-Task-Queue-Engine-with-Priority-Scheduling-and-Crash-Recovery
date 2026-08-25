import asyncio

async def worker(name: str,queue: asyncio.PriorityQueue):
    while True:
        priority,count,task_data = await queue.get()
        try:
            print(f"{name} exectuing task {task_data}")
            await asyncio.sleep(2)
        finally:
            queue.task_done()
async def main():
    queue = asyncio.PriorityQueue()
    workers = [
        asyncio.create_task(worker(f"worker-{i}",queue))
        for i in range(2)
        ]

    await queue.put((1,0,'urgent task'))
    await queue.put((5,1,'brackground task'))

    await queue.join()
    for w in workers:
        w.cancel()


asyncio.run(main())
            
