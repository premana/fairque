"""Queue implementation for FairQueue."""

from fairque.queue.async_queue import AsyncTaskQueue
from fairque.queue.queue import TaskQueue

__all__ = ["TaskQueue", "AsyncTaskQueue"]
