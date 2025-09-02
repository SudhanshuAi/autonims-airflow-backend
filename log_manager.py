import queue
from typing import Dict, Set
from threading import Lock


class LogManager:
    def __init__(self):
        self.subscribers: Dict[str, Set[queue.Queue]] = {}
        self._lock = Lock()

    def subscribe(self, job_id: str) -> queue.Queue:
        with self._lock:
            if job_id not in self.subscribers:
                self.subscribers[job_id] = set()
            log_queue = queue.Queue()
            self.subscribers[job_id].add(log_queue)
            return log_queue

    def unsubscribe(self, job_id: str, log_queue: queue.Queue):
        with self._lock:
            if job_id in self.subscribers and log_queue in self.subscribers[job_id]:
                self.subscribers[job_id].remove(log_queue)
                if not self.subscribers[job_id]:
                    del self.subscribers[job_id]

    def publish_log(self, job_id: str, log_entry: str):
        with self._lock:
            if job_id in self.subscribers:
                dead_queues = set()
                for log_queue in self.subscribers[job_id]:
                    try:
                        log_queue.put_nowait({"type": "log", "content": log_entry})
                    except queue.Full:
                        dead_queues.add(log_queue)

                # Clean up dead queues
                for dead_queue in dead_queues:
                    self.unsubscribe(job_id, dead_queue)
