"""Deliver pending discovery events from PostgreSQL outbox to Kafka."""

import asyncio
import logging

from kafka_component import KafkaProducerComponent
from prometheus_component import Metrics
from python_components import Component

from cinema_tracker.storage import PostgresStore

logger = logging.getLogger(__name__)


class OutboxPublisher(Component):
    def __init__(
        self,
        store: PostgresStore,
        producer: KafkaProducerComponent,
        metrics: Metrics | None = None,
        retry_seconds: float = 5.0,
    ) -> None:
        self.using([])
        self.store = store
        self.producer = producer
        self.retry_seconds = retry_seconds
        self._task: asyncio.Task | None = None
        self._published = (
            metrics.counter("events_published_total", "Published events") if metrics else None
        )
        self._failed = (
            metrics.counter("publish_failures_total", "Failed publishes") if metrics else None
        )

    async def publish_once(self, limit: int = 100) -> int:
        sent = 0
        for event in await self.store.pending_events(limit):
            try:
                await self.producer.send(event.topic, event.payload, key=event.key)
            except Exception as exc:  # noqa: BLE001 - external producer errors must retain outbox row
                await self.store.record_publish_failure(event.id, str(exc))
                if self._failed is not None:
                    self._failed.inc()
                break
            await self.store.mark_published(event.id)
            if self._published is not None:
                self._published.inc()
            sent += 1
        return sent

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def shutdown(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                await self.publish_once()
            except Exception:
                logger.exception("Outbox publish cycle failed")
                if self._failed is not None:
                    self._failed.inc()
            await asyncio.sleep(self.retry_seconds)
