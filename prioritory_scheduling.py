import heapq
import time
import uuid

class PriorityTaskQueue:
    def __init__(self):
        self._heap = []

    def push(self, priority: int, payload: dict):
        # Tie-breakers: priority -> timestamp -> unique UUID string
        item = (priority, time.time(), str(uuid.uuid4()), payload)
        heapq.heappush(self._heap, item)

    def pop(self) -> dict:
        priority, timestamp, task_id, payload = heapq.heappop(self._heap)
        return payload

pq = PriorityTaskQueue()
pq.push(priority=5, payload={"download": "low_pri"})
pq.push(priority=1, payload={"game": "high_pri"})
print(pq.pop())  # Outputs high_pri task payload