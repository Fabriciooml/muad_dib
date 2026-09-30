from datetime import UTC, date, datetime, time
from uuid import uuid4

import pytest

from cinema_tracker.domain import MovieNotFound, ProviderTarget, Session, Watch
from cinema_tracker.poll import SessionPoller


class FakeProvider:
    def __init__(self):
        self.fetch_count = 0

    async def resolve_movie(self, title):
        raise AssertionError("cached movie ID should avoid catalog request")

    async def fetch_sessions(self, movie_id):
        self.fetch_count += 1
        return [
            Session(
                provider="cineart",
                movie_id=movie_id,
                city="Belo Horizonte",
                cinema_id="25",
                cinema="Cineart Boulevard",
                room="Sala 06 IMAX",
                date=date(2026, 12, 15),
                time=time(14, 0),
                timezone="America/Sao_Paulo",
                format="IMAX",
                language="Legendado",
                purchase_url="https://example.com/buy",
            )
        ]


class FakeStore:
    def __init__(self, watches):
        self.watches = watches
        self.writes = []

    async def list_due_watches(self, now):
        return self.watches

    async def apply_poll_results(self, writes, completed_at):
        self.writes = writes
        return len({item.key for write in writes for item in write.sessions})


@pytest.mark.asyncio
async def test_due_overlapping_watches_fetch_once_and_write_one_session():
    now = datetime.now(UTC)
    target = ProviderTarget("cineart", "23469", "resolved")
    watches = [
        Watch(
            id=uuid4(),
            movie_title="Duna - Parte Três",
            providers=("cineart",),
            targets=(target,),
            city=None,
            cinema="Cineart Boulevard",
            room="Sala 06 IMAX",
            enabled=True,
            poll_interval_minutes=15,
            next_poll_at=now,
            revision=1,
        )
        for _ in range(2)
    ]
    provider = FakeProvider()
    store = FakeStore(watches)

    inserted = await SessionPoller(store, {"cineart": provider}).poll_once(now)

    assert inserted == 1
    assert provider.fetch_count == 1
    assert len(store.writes) == 2
    assert all(len(write.sessions) == 1 for write in store.writes)


@pytest.mark.asyncio
async def test_dynamic_watch_keeps_missing_provider_pending_while_other_succeeds():
    class PendingProvider:
        async def resolve_movie(self, title):
            raise MovieNotFound(title)

        async def fetch_sessions(self, movie_id):
            raise AssertionError("pending movie cannot be fetched")

    now = datetime.now(UTC)
    watch = Watch(
        id=uuid4(),
        movie_title="Duna - Parte Três",
        providers=None,
        targets=(ProviderTarget("cineart", "23469", "resolved"),),
        city=None,
        cinema=None,
        room=None,
        enabled=True,
        poll_interval_minutes=15,
        next_poll_at=now,
        revision=1,
    )
    store = FakeStore([watch])

    assert (
        await SessionPoller(
            store, {"cineart": FakeProvider(), "future": PendingProvider()}
        ).poll_once(now)
        == 1
    )

    statuses = {target.provider: target.resolution_status for target in store.writes[0].targets}
    assert statuses == {"cineart": "resolved", "future": "pending_not_found"}
