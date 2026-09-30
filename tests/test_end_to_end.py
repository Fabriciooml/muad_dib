import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from aiokafka import AIOKafkaConsumer
from alembic.config import Config
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from alembic import command
from cinema_tracker.app import Settings, build_app
from cinema_tracker.cineart import CineartProvider


@pytest.mark.asyncio
async def test_watch_discovers_fixture_sessions_once_and_publishes_to_kafka():
    fixtures = Path(__file__).parent / "fixtures"

    def respond(request: httpx.Request) -> httpx.Response:
        name = "catalog_em_breve.html" if request.url.path == "/em-breve" else "catalog_empty.html"
        if request.url.path == "/filme/23469":
            name = "duna_23469.html"
        return httpx.Response(200, text=(fixtures / name).read_text())

    with (
        PostgresContainer("postgres:16-alpine") as postgres,
        KafkaContainer("confluentinc/cp-kafka:7.5.0") as kafka,
    ):
        database_url = postgres.get_connection_url(driver="asyncpg")
        os.environ["DATABASE_URL"] = database_url
        await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as source:
            app = build_app(
                Settings(database_url, kafka.get_bootstrap_server(), "secret"),
                providers={"cineart": CineartProvider(source)},
            )
            async with app.router.lifespan_context(app):
                poller = app.state.system.system_map["poller"]
                publisher = app.state.system.system_map["publisher"]
                await poller.shutdown()
                await publisher.shutdown()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    headers = {"X-Admin-Token": "secret"}
                    created = await client.post(
                        "/watches",
                        json={
                            "movie_title": "Duna - Parte Três",
                            "cinema": "Cineart Boulevard",
                            "room": "Sala 06 IMAX",
                        },
                        headers=headers,
                    )
                    assert created.status_code == 201
                    assert await poller.poll_once(datetime.now(UTC)) == 6
                    assert await poller.poll_once(datetime.now(UTC)) == 0
                    assert await publisher.publish_once() == 6
                    unseen = await client.get(
                        "/sessions/preview/new",
                        params={
                            "movie_title": "Duna - Parte Três",
                            "cinema": "Cineart Boulevard",
                            "room": "Sala 06 IMAX",
                        },
                        headers=headers,
                    )
                    assert unseen.status_code == 200
                    assert unseen.json()["count"] == 0
                consumer = AIOKafkaConsumer(
                    "cinema.sessions.discovered.v1",
                    bootstrap_servers=kafka.get_bootstrap_server(),
                    group_id="tracker-e2e",
                    auto_offset_reset="earliest",
                )
                await consumer.start()
                try:
                    events = [
                        json.loads((await asyncio.wait_for(consumer.getone(), timeout=10)).value)
                        for _ in range(6)
                    ]
                    assert len({event["event_id"] for event in events}) == 6
                    assert all(event["event_type"] == "session.discovered" for event in events)
                    assert all(event["purchase_url"].startswith("https://") for event in events)
                finally:
                    await consumer.stop()
