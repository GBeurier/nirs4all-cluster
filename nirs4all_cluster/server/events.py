"""Event recording + in-process pub/sub for live streaming.

Every event is persisted (so ``GET /events`` can paginate history) and also
fanned out to any live WebSocket/SSE subscribers for the job. The broker is an
asyncio fan-out keyed by ``job_id``; it holds bounded per-subscriber queues so a
slow client cannot block the server (it drops oldest on overflow).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from .db import Database

_QUEUE_MAX = 1000


class EventBroker:
    def __init__(self, db: Database):
        self.db = db
        self._subscribers: dict[str, set[asyncio.Queue]] = {}
        # Global subscribers receive *every* event regardless of job_id (the
        # dashboard's live feed). Separate from the per-job fan-out.
        self._global: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Record the server event loop so sync route handlers (run in a thread
        pool, where there is no running loop) can still schedule broadcasts."""
        self._loop = loop

    async def subscribe(self, job_id: str) -> asyncio.Queue:
        async with self._lock:
            self._subscribers.setdefault(job_id, set())
            queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
            self._subscribers[job_id].add(queue)
            return queue

    async def unsubscribe(self, job_id: str, queue: asyncio.Queue) -> None:
        async with self._lock:
            subs = self._subscribers.get(job_id)
            if subs and queue in subs:
                subs.discard(queue)
                if not subs:
                    self._subscribers.pop(job_id, None)

    async def subscribe_global(self) -> asyncio.Queue:
        async with self._lock:
            queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
            self._global.add(queue)
            return queue

    async def unsubscribe_global(self, queue: asyncio.Queue) -> None:
        async with self._lock:
            self._global.discard(queue)

    def emit(
        self,
        *,
        level: str,
        type: str,
        message: str = "",
        job_id: str | None = None,
        task_id: str | None = None,
        worker_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist an event and schedule a non-blocking broadcast.

        Safe to call from sync code (route handlers run in the event loop). The
        broadcast is best-effort: if there is no running loop the event is still
        persisted.
        """
        event_id = self.db.add_event(
            level=level,
            type=type,
            message=message,
            job_id=job_id,
            task_id=task_id,
            worker_id=worker_id,
            data=data,
        )
        payload = {
            "id": event_id,
            "job_id": job_id,
            "task_id": task_id,
            "worker_id": worker_id,
            "ts": time.time(),
            "level": level,
            "type": type,
            "message": message,
            "data": data or {},
        }
        # Sync FastAPI handlers run in a threadpool (no running loop), so schedule
        # onto the recorded server loop thread-safely. Async callers and unit tests
        # without a loop simply skip the live broadcast. Every event reaches global
        # subscribers; job-scoped events also reach that job's subscribers.
        try:
            running = asyncio.get_running_loop()
            running.call_soon(self._broadcast, payload)
        except RuntimeError:
            if self._loop is not None and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._broadcast, payload)
        return payload

    def _broadcast(self, payload: dict[str, Any]) -> None:
        job_id = payload.get("job_id")
        queues = set(self._global)
        if job_id is not None:
            queues |= self._subscribers.get(job_id, set())
        for queue in list(queues):
            if queue.full():
                try:
                    queue.get_nowait()  # drop oldest
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                pass
