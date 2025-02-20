"""
Notification Multiplexer used to listen to notifications on one channel and broadcast.
"""

from __future__ import annotations

import asyncio
import typing as t
from abc import ABC, abstractmethod
from collections import defaultdict


if t.TYPE_CHECKING:
    Q = asyncio.Queue[dict]


class Multiplexer(ABC):
    def __init__(self) -> None:
        self._subscriptions: t.Dict[str, t.Set[Q]] = defaultdict(set)
        self._queues: t.Dict[Q, t.Set[str]] = defaultdict(set)
        self._daemon_task: t.Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if not self._daemon_task:
                self._daemon_task = asyncio.create_task(self._start())

    @abstractmethod
    async def _start(self) -> None: ...

    async def _close(self) -> None:
        pass

    async def close(self) -> None:
        async with self._lock:
            if self._daemon_task:
                self._daemon_task.cancel()
                self._daemon_task = None

            await self._close()
            self._subscriptions.clear()
            self._queues.clear()

    async def listen(self, *channels: str, timeout: float | None = None) -> t.AsyncGenerator:
        queue = await self.subscribe(*channels)

        try:
            while self._daemon_task and not self._daemon_task.done():
                try:
                    yield await asyncio.wait_for(queue.get(), timeout if timeout else 1.0)
                    queue.task_done()
                except asyncio.TimeoutError:
                    if timeout:
                        raise
        finally:
            await self.unsubscribe(queue)

    def publish(self, channel: str, message: t.Any) -> None:
        for queue in self._subscriptions[channel]:
            queue.put_nowait(message)

    async def subscribe(self, *channels: str) -> Q:
        await self.start()
        queue: Q = asyncio.Queue()
        for channel in channels:
            self._queues[queue].add(channel)
            self._subscriptions[channel].add(queue)
        return queue

    async def unsubscribe(self, queue: Q) -> None:
        for channel in self._queues.pop(queue, []):
            self._subscriptions[channel].remove(queue)

        if not self._queues:
            await self.close()
