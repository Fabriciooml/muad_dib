import json
from datetime import date, time
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from jsonschema import Draft202012Validator

from cinema_tracker.app import Settings, build_app
from cinema_tracker.domain import AmbiguousMovie, MovieMatch, MovieNotFound, Session


class FakeProvider:
    async def resolve_movie(self, title):
        return MovieMatch(title, "23469", "https://www.cineart.com.br/filme/23469")

    async def fetch_sessions(self, movie_id):
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
    def __init__(self):
        self.watches = {}
        self.known_keys = set()

    async def create_watch(self, watch):
        self.watches[watch.id] = watch

    async def get_watch(self, watch_id):
        return self.watches.get(watch_id)

    async def list_watches(self):
        return list(self.watches.values())

    async def replace_watch(self, watch):
        self.watches[watch.id] = watch
        return True

    async def delete_watch(self, watch_id):
        return self.watches.pop(watch_id, None) is not None

    async def existing_session_keys(self, keys):
        return self.known_keys.intersection(keys)


@pytest.mark.asyncio
async def test_admin_creates_pending_capable_watch_without_movie_id():
    store = FakeStore()
    app = build_app(
        Settings("postgresql+asyncpg://test:test@localhost/test", "localhost:9092", "secret"),
        providers={"cineart": FakeProvider()},
        store=store,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        body = {
            "movie_title": "Duna - Parte Três",
            "cinema": "Cineart Boulevard",
            "room": "Sala 06 IMAX",
        }
        assert (await client.post("/watches", json=body)).status_code == 401
        response = await client.post("/watches", json=body, headers={"X-Admin-Token": "secret"})

    assert response.status_code == 201
    assert response.json()["poll_interval_minutes"] == 15
    assert response.json()["providers"] is None
    assert response.json()["targets"][0]["movie_id"] == "23469"
    assert len(store.watches) == 1


@pytest.mark.asyncio
async def test_preview_new_reads_known_keys_without_creating_watch():
    store = FakeStore()
    app = build_app(
        Settings("postgresql+asyncpg://test:test@localhost/test", "localhost:9092", "secret"),
        providers={"cineart": FakeProvider()},
        store=store,
    )
    query = "movie_title=Duna%20-%20Parte%20Tr%C3%AAs&cinema=Cineart%20Boulevard"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        all_result = await client.get(
            f"/sessions/preview?{query}", headers={"X-Admin-Token": "secret"}
        )
        assert all_result.status_code == 200
        assert all_result.json()["count"] == 1
        assert all_result.json()["sessions"][0]["purchase_url"] == "https://example.com/buy"
        output_schema = json.loads(Path("schemas/output.v1.schema.json").read_text())
        Draft202012Validator(output_schema).validate(all_result.json())
        store.known_keys = {(await FakeProvider().fetch_sessions("23469"))[0].key}
        new_result = await client.get(
            f"/sessions/preview/new?{query}", headers={"X-Admin-Token": "secret"}
        )

    assert new_result.status_code == 200
    assert new_result.json()["count"] == 0
    assert new_result.headers["cache-control"] == "no-store"
    assert store.watches == {}


@pytest.mark.asyncio
async def test_admin_lists_replaces_and_deletes_watch():
    store = FakeStore()
    app = build_app(
        Settings("postgresql+asyncpg://test:test@localhost/test", "localhost:9092", "secret"),
        providers={"cineart": FakeProvider()},
        store=store,
    )
    headers = {"X-Admin-Token": "secret"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        created = await client.post("/watches", json={"movie_title": "Duna"}, headers=headers)
        watch_id = created.json()["id"]
        listed = await client.get("/watches", headers=headers)
        changed = await client.put(
            f"/watches/{watch_id}",
            json={
                "movie_title": "Duna",
                "providers": ["cineart"],
                "enabled": False,
                "poll_interval_minutes": 5,
            },
            headers=headers,
        )
        deleted = await client.delete(f"/watches/{watch_id}", headers=headers)

    assert [item["id"] for item in listed.json()] == [watch_id]
    assert changed.status_code == 200
    assert changed.json()["revision"] == 2
    assert changed.json()["poll_interval_minutes"] == 5
    assert changed.json()["enabled"] is False
    assert deleted.status_code == 204
    assert UUID(watch_id) not in store.watches


@pytest.mark.asyncio
async def test_live_and_metrics_are_public_but_ready_requires_dependencies():
    app = build_app(
        Settings("postgresql+asyncpg://test:test@localhost/test", "localhost:9092", "secret"),
        providers={"cineart": FakeProvider()},
        store=FakeStore(),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        live = await client.get("/health/live")
        ready = await client.get("/health/ready")
        metrics = await client.get("/metrics")

    assert live.status_code == 200
    assert ready.status_code == 503
    assert metrics.status_code == 200


@pytest.mark.asyncio
async def test_pending_watch_is_created_and_ambiguous_title_rejected():
    class MissingProvider:
        async def resolve_movie(self, title):
            raise MovieNotFound(title)

        async def fetch_sessions(self, movie_id):
            raise AssertionError("pending movie should not fetch")

    class AmbiguousProvider(MissingProvider):
        async def resolve_movie(self, title):
            raise AmbiguousMovie([MovieMatch(title, "1", "https://example.com/1")])

    store = FakeStore()
    app = build_app(
        Settings("postgresql+asyncpg://test:test@localhost/test", "localhost:9092", "secret"),
        providers={"missing": MissingProvider(), "ambiguous": AmbiguousProvider()},
        store=store,
    )
    headers = {"X-Admin-Token": "secret"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        pending = await client.post(
            "/watches", json={"movie_title": "Duna", "providers": ["missing"]}, headers=headers
        )
        conflict = await client.post(
            "/watches", json={"movie_title": "Duna", "providers": ["ambiguous"]}, headers=headers
        )
        invalid = await client.post(
            "/watches", json={"movie_title": "Duna", "providers": []}, headers=headers
        )

    assert pending.status_code == 201
    assert pending.json()["targets"][0]["resolution_status"] == "pending_not_found"
    assert conflict.status_code == 409
    assert invalid.status_code == 422
    assert len(store.watches) == 1


def test_openapi_contains_watch_response_contract():
    app = build_app(
        Settings("postgresql+asyncpg://test:test@localhost/test", "localhost:9092", "secret"),
        providers={"cineart": FakeProvider()},
        store=FakeStore(),
    )
    post_response = app.openapi()["paths"]["/watches"]["post"]["responses"]["201"]
    assert post_response["content"]["application/json"]["schema"]["$ref"].endswith("/WatchResponse")
