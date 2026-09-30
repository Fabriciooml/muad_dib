from uuid import uuid4

import pytest

from cinema_tracker.publish import OutboxPublisher
from cinema_tracker.storage import OutboxEvent


class FakeStore:
    def __init__(self, event: OutboxEvent):
        self.event = event
        self.published = []
        self.failures = []

    async def pending_events(self, limit: int):
        return [self.event] if not self.published else []

    async def mark_published(self, event_id):
        self.published.append(event_id)

    async def record_publish_failure(self, event_id, error):
        self.failures.append((event_id, error))


class FakeProducer:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    async def send(self, topic, payload, *, key):
        if self.fail:
            raise RuntimeError("broker unavailable")
        self.sent.append((topic, payload, key))


@pytest.mark.asyncio
async def test_publisher_marks_event_only_after_acknowledged_send():
    event = OutboxEvent(
        id=uuid4(),
        topic="cinema.sessions.discovered.v1",
        key=b"stable-session",
        payload={"event_id": "stable-event"},
    )
    store = FakeStore(event)
    producer = FakeProducer()

    published = await OutboxPublisher(store, producer).publish_once()

    assert published == 1
    assert producer.sent == [(event.topic, event.payload, event.key)]
    assert store.published == [event.id]
    assert store.failures == []


@pytest.mark.asyncio
async def test_failed_send_leaves_event_pending_for_retry():
    event = OutboxEvent(uuid4(), "cinema.sessions.discovered.v1", b"stable", {"event_id": "1"})
    store = FakeStore(event)
    producer = FakeProducer(fail=True)
    publisher = OutboxPublisher(store, producer)

    assert await publisher.publish_once() == 0
    assert store.published == []
    assert store.failures == [(event.id, "broker unavailable")]
    producer.fail = False
    assert await publisher.publish_once() == 1
    assert store.published == [event.id]
