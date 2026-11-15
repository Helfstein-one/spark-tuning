"""Autonomous tuning daemon processing multi-repository optimization queues."""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional
from spark_tuning.daemon.scanner import TargetRepository


@dataclass
class TuningEvent:
    event_id: str
    event_type: str  # push, pr_opened, schedule
    repository: TargetRepository
    priority: int = 10  # Lower number = higher priority


class AutonomousTuningDaemon:
    """Background worker daemon executing the tuning loop across multi-tenant repositories."""

    def __init__(self, worker_threads: int = 2):
        self.worker_threads = worker_threads
        self.job_queue: queue.PriorityQueue = queue.PriorityQueue()
        self.is_running = False
        self.processed_events: List[str] = []

    def dispatch_event(self, event: TuningEvent) -> None:
        # Enqueue with priority tuple: (priority, event)
        self.job_queue.put((event.priority, event))

    def process_next_sync(self, handler: Optional[Callable[[TuningEvent], None]] = None) -> Optional[TuningEvent]:
        if self.job_queue.empty():
            return None
        _, event = self.job_queue.get()
        if handler:
            handler(event)
        self.processed_events.append(event.event_id)
        return event
