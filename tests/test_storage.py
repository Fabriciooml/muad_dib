import json
import os
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.config import Config
from jsonschema import Draft202012Validator
from postgres_component import PostgresComponent
from sqlalchemy import select
from testcontainers.community.postgres import PostgresContainer

from alembic import command
from cinema_tracker.domain import ProviderTarget, Session, Watch
from cinema_tracker.schema import sessions
from cinema_tracker.storage import PollWrite, PostgresStore


@pytest.fixture(scope="module")
def database_url():
    with PostgresContainer("postgres:16-alpine") as container:
        url = container.get_connection_url(driver="asyncpg")
        os.environ["DATABASE_URL"] = url
        command.upgrade(Config("alembic.ini"), "head")
        command.upgrade(Config("alembic.ini"), "head")
        yield url


@pytest.mark.asyncio
async def test_watch_round_trip_with_pending_provider(database_url):
    database = PostgresComponent(url=database_url)
    await database.start()
    try:
        store = PostgresStore(database)
        watch = Watch(
            id=uuid4(),
            movie_title="Duna - Parte Três",
            providers=None,
            targets=(ProviderTarget("cineart", None, "pending_not_found"),),
            city=None,
            cinema="Cineart Boulevard",
            room="Sala 06 IMAX",
            enabled=True,
            poll_interval_minutes=15,
            next_poll_at=datetime.now(UTC),
            revision=1,
        )
        await store.create_watch(watch)
        assert await store.get_watch(watch.id) == watch
    finally:
        await database.shutdown()


@pytest.mark.asyncio
async def test_session_insert_is_unique_and_creates_one_outbox_event(database_url):
    database = PostgresComponent(url=database_url)
    await database.start()
    try:
        store = PostgresStore(database)
        observed_at = datetime.now(UTC)
        session = Session(
            provider="cineart",
            movie_id="23469",
            city="Belo Horizonte",
            cinema_id="25",
            cinema="Cineart Boulevard",
            room="Sala 06 IMAX",
            date=date(2026, 12, 15),
            time=time(14, 0),
            timezone="America/Sao_Paulo",
            format="IMAX",
            language="Legendado",
            purchase_url="https://example.com/first",
        )

        assert await store.insert_discoveries([session], observed_at) == 1
        assert await store.insert_discoveries([session], observed_at) == 0
        assert (
            await store.insert_discoveries(
                [Session(**{**session.__dict__, "purchase_url": "https://example.com/new"})],
                observed_at,
            )
            == 0
        )
        assert await store.existing_session_keys([session.key]) == {session.key}
        events = await store.pending_events(10)
        assert len(events) == 1
        assert events[0].payload["purchase_url"] == "https://example.com/first"
        assert events[0].payload["event_id"] == str(events[0].id)
        output_schema = json.loads(Path("schemas/output.v1.schema.json").read_text())
        Draft202012Validator.check_schema(output_schema)
        Draft202012Validator(output_schema).validate(events[0].payload)
        async with database.session() as connection:
            current_url = (
                await connection.execute(
                    select(sessions.c.purchase_url).where(
                        sessions.c.provider == session.provider,
                        sessions.c.movie_id == session.movie_id,
                        sessions.c.cinema_id == session.cinema_id,
                        sessions.c.room_key == session.key.room_key,
                        sessions.c.session_date == session.date,
                        sessions.c.session_time == session.time,
                    )
                )
            ).scalar_one()
        assert current_url == "https://example.com/new"
    finally:
        await database.shutdown()


@pytest.mark.asyncio
async def test_replace_watch_resets_schedule_and_revision(database_url):
    database = PostgresComponent(url=database_url)
    await database.start()
    try:
        store = PostgresStore(database)
        now = datetime.now(UTC)
        watch = Watch(
            id=uuid4(),
            movie_title="Duna - Parte Três",
            providers=("cineart",),
            targets=(ProviderTarget("cineart", "23469", "resolved"),),
            city=None,
            cinema=None,
            room=None,
            enabled=True,
            poll_interval_minutes=15,
            next_poll_at=now,
            revision=1,
        )
        await store.create_watch(watch)
        assert watch.id in {item.id for item in await store.list_due_watches(now)}

        changed = replace(
            watch,
            enabled=False,
            poll_interval_minutes=5,
            next_poll_at=now,
            revision=2,
        )
        await store.replace_watch(changed)
        assert await store.get_watch(watch.id) == changed
        assert watch.id not in {item.id for item in await store.list_due_watches(now)}
        await store.delete_watch(watch.id)
        assert await store.get_watch(watch.id) is None
    finally:
        await database.shutdown()


@pytest.mark.asyncio
async def test_stale_poll_result_cannot_discover_or_reschedule(database_url):
    database = PostgresComponent(url=database_url)
    await database.start()
    try:
        store = PostgresStore(database)
        now = datetime.now(UTC)
        watch = Watch(
            id=uuid4(),
            movie_title="Duna - Parte Três",
            providers=("cineart",),
            targets=(ProviderTarget("cineart", "23469", "resolved"),),
            city=None,
            cinema=None,
            room=None,
            enabled=True,
            poll_interval_minutes=15,
            next_poll_at=now,
            revision=1,
        )
        await store.create_watch(watch)
        changed = replace(watch, enabled=False, revision=2)
        await store.replace_watch(changed)
        session = Session(
            provider="cineart",
            movie_id="23469",
            city="Belo Horizonte",
            cinema_id="25",
            cinema="Cineart Boulevard",
            room="Sala 06 IMAX",
            date=date(2026, 12, 16),
            time=time(17, 20),
            timezone="America/Sao_Paulo",
            format="IMAX",
            language="Legendado",
            purchase_url="https://example.com/late",
        )
        before = await store.pending_count()
        assert (
            await store.apply_poll_results([PollWrite(watch, (session,), watch.targets)], now) == 0
        )
        assert await store.pending_count() == before
        assert await store.existing_session_keys([session.key]) == set()
        assert await store.get_watch(watch.id) == changed

        reenabled = replace(changed, enabled=True, revision=3)
        assert await store.replace_watch(reenabled)
        assert (
            await store.apply_poll_results(
                [PollWrite(reenabled, (session,), reenabled.targets)], now
            )
            == 1
        )
        saved = await store.get_watch(watch.id)
        assert saved is not None
        assert saved.next_poll_at == now + timedelta(minutes=15)
        assert await store.existing_session_keys([session.key]) == {session.key}
    finally:
        await database.shutdown()
